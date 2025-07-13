
import os
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time
import re
from src.utils.logger import get_logger

class FeatureEngineering:
    def __init__(self, silver_path: str, gold_path : str, data_quality : pd.DataFrame):
        self.silver_path = silver_path
        self.gold_path = gold_path
        self.data_quality = data_quality
        self.logger = get_logger(self.__class__.__name__)

    def get_missing_files(self):
        # Check zip files
        silver_files = os.listdir(self.silver_path)
        self.logger.info(f"Searching for files in {self.silver_path}")
        self.logger.info(f"Found {len(silver_files)} .ftr files.")

        # Check ftr files
        gold_files = os.listdir(self.gold_path)
        self.logger.info(f"Searching for files in {self.gold_path}")
        self.logger.info(f"Found {len(gold_files)} .ftr files.")
        
        # Check unprocessed files
        missing_files = [file for file in silver_files if file not in gold_files]
        return missing_files
    
    def _add_vehicle_type_scope(self, df, data_quality, file_name):

        # Process each type of vehicle
        cars = data_quality['cars']
        motorbikes = data_quality['motorbikes']
        trucks = data_quality['trucks']
        tractor = data_quality['tractor']
        unknown = data_quality['unknown']

        # Full mapping data
        mapping = cars | motorbikes | trucks | tractor | unknown
        df['clean_marca'] = df['marca'].map(mapping)
        dq_warning = df[df['clean_marca'].isnull()].marca.value_counts().to_frame().query('marca > 100')
        if dq_warning.shape[0] > 0:
            for brand in dq_warning.index:
                self.logger.warning(f"File {file_name} - Brand {brand} has a lot of observations that has been ignored: {dq_warning.loc[brand, 'marca']}")


        # Updated keys to filter the data
        filter_list_cars = list(set(data_quality['cars'].values()))
        filter_list_motorbikes = list(set(data_quality['motorbikes'].values()))
        filter_list_trucks = list(set(data_quality['trucks'].values()))
        filter_list_tractor = list(set(data_quality['tractor'].values()))
        filter_list_unknown = list(set(data_quality['unknown'].values()))

        # Create column of type of vehicle and also will serve us as filter
        condition = [df['clean_marca'].isin(filter_list_cars),
                    df['clean_marca'].isin(filter_list_motorbikes),
                    df['clean_marca'].isin(filter_list_trucks),
                    df['clean_marca'].isin(filter_list_tractor),
                    df['clean_marca'].isin(filter_list_unknown)]
        choice = ['Car', 'Motorbike', 'Truck', 'Tractor', 'Delete']
        df['type_vehicle'] = np.select(condition, choice, 'Delete')

        # We filter out of scope vehicles
        df = df.query('type_vehicle != "Delete"').drop(columns = 'marca').rename(columns = {'clean_marca' : 'marca'})

        # Add electric vehicles
        df['cilindrada'] = df['cilindrada'].astype(int)
        df['electric'] = np.where(df['cilindrada'] == 0, 1, 0)
        return df
    
    def _add_new_latitude_longitude(self, data_quality):
        # Config the geocode of Nominatim
        geolocator = Nominatim(user_agent="geo_es_localidades")
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)

        new_cities = data_quality['add_cities']
        if len(new_cities) > 0:
            # Code to get unknown cities location
            latitudes = []
            longitudes = []
            for localidad in new_cities:
                location = geocode(f"{localidad}, España")
                if location:
                    lat = location.latitude
                    lon = location.longitude
                else:
                    lat = None
                    lon = None
                latitudes.append(lat)
                longitudes.append(lon)

    def _add_latitude_longitude(self, df, data_quality, file_name):

        # Database with Latitude and Longitude for each city
        lat_lon = pd.read_csv('localidades_mas_lat_lon.csv', index_col=0)
        df = df.merge(lat_lon, on = 'localidad', how = 'left')

        # Read cities out of scope
        cities_out_scope = data_quality['cities']

        # Get cities that are new in the current data
        df_null_lat_lon = df[df['latitud'].isnull()].groupby('localidad').count()[['marca']].query('marca > 100').reset_index()

        # Filter out the cities already identified as out of scope
        df_null_lat_lon[~ df_null_lat_lon.localidad.isin(cities_out_scope)]

        if df_null_lat_lon.shape[0] > 0:
            for i in df_null_lat_lon.index:
                self.logger.warning(f"File {file_name} - City {df_null_lat_lon.loc[i, 'localidad']} has a lot of observations that has been ignored: {df_null_lat_lon.loc[i, 'marca']}")

        return df[df['latitud'].notnull()].reset_index(drop = True)
        
    
    def process_silver_file(self, file):
        # Get the name of the file processed
        file_name = re.search(r'export_mensual_mat_\d{6}', file).group()

        # 0. Read silver file
        df_silver = pd.read_feather(file)

        # 1. Apply data quality for brands and type of vehicles
        df_gold_raw = self._add_vehicle_type_scope(df_silver, self.data_quality, file_name)

        # 2. Apply process to add new lat and lon to cities
        # self._add_new_latitude_longitude(df_silver, self.data_quality)
        df_gold = self._add_latitude_longitude(df_gold_raw, self.data_quality, file_name)

        df_gold.columns = [
                        "registration_date",
                        "model",
                        "engine_displacement_cc",
                        "city",
                        "brand",
                        "vehicle_type",
                        "is_electric",
                        "latitude",
                        "longitude"
                    ]
        
        df_gold['year'] = df_gold['registration_date'].dt.year
        df_gold['month'] = df_gold['registration_date'].dt.month
        

        return df_gold[['registration_date', 'year', 'month', 'city', 'latitude', 'longitude',
                        'brand', 'model', 'engine_displacement_cc', 'vehicle_type', 'is_electric']]
    

    def process_files_to_gold(self):
        
        # Check missing files to process in silver
        missing_files = self.get_missing_files()

        # Check if we need to process new files
        if len(missing_files) > 0:
            self.logger.info(f"Found {len(missing_files)} .ftr silver files unprocessed.")
            self.logger.info(f"Processing new files to gold:")

            for missing_file in missing_files:
                silver_file = os.path.join(self.silver_path, missing_file)
                df_gold = self.process_silver_file(silver_file)
                df_gold.to_feather(os.path.join(self.gold_path, missing_file))
                self.logger.info(f"Saved gold data to {os.path.join(self.gold_path, missing_file)}")

        else:
            self.logger.info(f"All .ftr silver files are processed. No need for updates in gold")