
import os
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time
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
    
    def _add_vehicle_type_scope(self, df, data_quality):

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
                self.logger.warning(f"Warning, the brand {brand} has a lot of observations that has been ignored: {dq_warning.loc[brand, 'marca']}")


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
        return df
    
    def _add_latitude_longitude(self, ):
        # Config the geocode of Nominatim
        geolocator = Nominatim(user_agent="geo_es_localidades")
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)

        df = pd.read_csv('localidades_mas_lat_lon.csv', index_col = 0)
    
    def process_silver_file(self, file):

        # 0. Read silver file
        df_silver = pd.read_feather(file)

        # 1. Apply data quality for brands and type of vehicles
        df_gold_raw = self._add_vehicle_type_scope(df_silver, self.data_quality)
        
        # 2. Add lat and lon. Warning for new localities
        # df_gold = self._add_latitude_longitude(df_gold_raw)

        # 3. Add electric vehicles

        return df_gold_raw
    

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
        #         df_gold.to_feather(os.path.join(self.gold_path, missing_file))
        #         self.logger.info(f"Saved gold data to {os.path.join(self.gold_path, missing_file)}")

        # else:
        #     self.logger.info(f"All .ftr silver files are processed. No need for updates in gold")