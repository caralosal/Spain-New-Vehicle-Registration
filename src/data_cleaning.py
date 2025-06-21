from pathlib import Path
import pandas as pd
import os
from zipfile import ZipFile
from io import TextIOWrapper
from src.utils.logger import get_logger
from src.utils.paths import BRONZE, SILVER



class DataCleaner:
    def __init__(self, bronze_path: Path, silver_path: Path, colspecs: list, colnames: list):
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.colspecs = colspecs
        self.colnames = colnames
        self.logger = get_logger(self.__class__.__name__)

    def get_missing_files(self):
        # Check zip files
        bronze_files = os.listdir(self.bronze_path)
        self.logger.info(f"Searching for files in {self.bronze_path}")
        self.logger.info(f"Found {len(bronze_files)} .zip files.")

        # Check ftr files
        silver_files = os.listdir(self.silver_path)
        self.logger.info(f"Searching for files in {self.silver_path}")
        self.logger.info(f"Found {len(silver_files)} .ftr files.")

        # Change extension of .ftr to compare with zip files
        silver_files_zip = [file.replace("ftr", "zip") for file in silver_files]
        
        # Check unprocessed files
        missing_files = [file for file in bronze_files if file not in silver_files_zip]
        return missing_files

    def process_files_to_silver(self):
        
        # Check missing files to process
        missing_files = self.get_missing_files()

        # Check if we need to process new files
        if len(missing_files) > 0:
            self.logger.info(f"Found {len(missing_files)} .zip files unprocessed.")
            self.logger.info(f"Processing new files:")

            for missing_file in missing_files:
                bronze_zip_file = os.path.join(self.bronze_path, missing_file)
                df_silver = self.process_zip_file(bronze_zip_file)
                df_silver.to_feather(os.path.join(self.silver_path, missing_file.replace("zip", "ftr")))
                self.logger.info(f"Saved cleaned data to {os.path.join(self.silver_path, missing_file.replace('zip', 'ftr'))}")

        else:
            self.logger.info(f"All .zip files are processed. No need for updates")


    def process_zip_file(self, zip_path: Path):
        self.logger.info(f"Processing {zip_path}")

        # 1. Read the CSV inside the ZIP (assuming only one CSV per zip)
        df = self.read_fwf_from_zip(zip_path)

        # 2. Perform cleaning steps (can be modularized later)
        df_clean = self._clean_dataframe(df)

        # 3. Clean date column
        df_clean_date = self._clean_date_column(df_clean)
        return df_clean_date
        

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        # Minimal example cleaning logic
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        return df
    
    def _clean_date_column(self, df: pd.DataFrame) -> pd.DataFrame:
        # It is read by int
        df['fecha_matriculacion'] = df['fecha_matriculacion'].astype('str')

        # Days less than 10 doesn't include the 0. We need to introduce it
        df['fecha_matriculacion'] = df['fecha_matriculacion'].str.zfill(8)

        # Convert to datetime column
        df['fecha_matriculacion'] = pd.to_datetime(df['fecha_matriculacion'], format='%d%m%Y')

        return df
    
    def read_fwf_from_zip(self, zip_path):
        with ZipFile(zip_path) as z:
            txt_name = z.namelist()[0]
            with z.open(
                txt_name) as f:
                df = pd.read_fwf(
                    TextIOWrapper(f, encoding="latin-1"),
                    colspecs=self.colspecs,
                    names=self.colnames,
                    skiprows=1
                )
        return df
