from src.data_gathering import DataGatherer
from src.data_cleaning import DataCleaner
from src.feature_engineering import FeatureEngineering
from src.utils.paths import BRONZE, SILVER, GOLD
from src.utils.config import data_quality, colspecs, colnames


def main():

    # URL in which we are going to check all the files
    dgt_url = "https://www.dgt.es/menusecundario/dgt-en-cifras/matraba-listados/matriculaciones-automoviles-mensual.html"

    # Instance of DataGatherer
    gatherer = DataGatherer(base_page_url= dgt_url,
                            bronze_path= BRONZE)

    # Download new data to bronze
    gatherer.download_to_bronze()

    # Instance of Data Cleaner. 
    cleaner = DataCleaner(bronze_path = BRONZE,
                          silver_path = SILVER,
                          colspecs = colspecs,
                          colnames = colnames)
    
    # Process .zip file and create .ftr and move them to silver
    cleaner.process_files_to_silver()

    # Instance Feature Engineering
    feature_engineer = FeatureEngineering(silver_path = SILVER,
                                          gold_path = GOLD,
                                          data_quality = data_quality)
    
    # Process silver files to gold. Add useful information
    feature_engineer.process_files_to_gold()

if __name__ == "__main__":
    main()