import requests
from pathlib import PurePosixPath
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin
from src.utils.logger import get_logger
from src.utils.paths import BRONZE
import os
import time

class DataGatherer:
    def __init__(self, base_page_url: str, bronze_path: str, retries: int = 3, delay: int = 2):
        self.base_page_url = base_page_url
        self.bronze_path = bronze_path
        self.session = requests.Session()
        self.logger = get_logger(self.__class__.__name__)
        self.retries = retries
        self.delay = delay

    def get_zip_links(self) -> list:
        self.logger.info(f"Scraping zip links from {self.base_page_url}")
        try:
            response = self.session.get(self.base_page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            links = []
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if href.lower().endswith(".zip"):
                    full_url = urljoin(self.base_page_url, href)
                    links.append(full_url)

            self.logger.info(f"Found {len(links)} .zip files")
            
            return links

        except requests.RequestException as e:
            self.logger.error(f"Failed to scrape the page: {e}")
            raise

    def get_bronze_files(self) -> list:
        self.logger.info(f"Searching for files in {self.bronze_path}")
        try:
            bronze_files = os.listdir(self.bronze_path)
            bronze_files = [file for file in bronze_files if file.endswith('.zip')]
            self.logger.info(f"Found {len(bronze_files)} .zip files")
            return bronze_files

        except requests.RequestException as e:
            self.logger.error(f"Failed to scrape the page: {e}")
            raise

    def download_zip(self, url: str) -> Path:
        filename = url.split("/")[-1]
        destination = self.bronze_path / filename

        if destination.exists():
            self.logger.info(f"File already exists, skipping: {filename}")
            return destination

        self.logger.info(f"Downloading: {url}")
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(url, stream=True, timeout=10)
                response.raise_for_status()

                with open(destination, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                self.logger.info(f"Saved to {destination}")
                return destination

            except requests.RequestException as e:
                self.logger.warning(f"Attempt {attempt} failed for {filename}: {e}")
                time.sleep(self.delay)

        self.logger.error(f"Failed to download after {self.retries} attempts: {filename}")
        raise Exception(f"Download failed: {filename}")


    def get_missing_files(self, zipped_list, list_files) -> list:
        """
        Returns items in zipped_list that are not present in list_files.

        Args:
            zipped_list (List[str]): List of all available files (e.g., from web).
            list_files (List[str]): List of already existing/downloaded files.

        Returns:
            List[str]: Files that still need to be downloaded.
        """
        existing_files_set = set(list_files)
        missing_files = [file for file in zipped_list if PurePosixPath(file).name not in existing_files_set]
        
        return missing_files

    def download_to_bronze(self):
        # Get data from the DGT links and for bronze folder
        zip_links = self.get_zip_links()
        bronze_files = self.get_bronze_files()

        # Search for not downloaded files
        missing_files = self.get_missing_files(zip_links, bronze_files)

        if len(missing_files) > 0:
            self.logger.info(f"Found {len(missing_files)} new files. Updating the folder")
        
            # For those missing files, download them into the bronze layer
            for url in missing_files:
                zip_path = self.download_zip(url)
                # self.unzip_file(zip_path)
        else:
            self.logger.info(f"All files downloaded, no need for updates.")