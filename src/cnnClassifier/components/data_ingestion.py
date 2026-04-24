import os
import urllib.request as request
import zipfile
from cnnClassifier import logger
from cnnClassifier.utils.common import get_size
from pathlib import Path
from cnnClassifier.entity.config_entity import DataIngestionConfig

class DataIngestion:
    def __init__(self, config):
        self.config = config

    def download_file(self):
        os.makedirs(os.path.dirname(self.config.local_data_file), exist_ok=True)

        file_path = self.config.local_data_file

        # 🔁 If file exists → validate it
        if os.path.exists(file_path):
            if zipfile.is_zipfile(file_path):
                logger.info("Valid zip file already exists. Skipping download.")
                return
            else:
                logger.warning("Corrupted file detected. Deleting...")
                os.remove(file_path)

        # ⬇️ Download fresh file
        filename, headers = request.urlretrieve(
            url=self.config.source_URL,
            filename=file_path
        )

        # ✅ Validate after download
        if not zipfile.is_zipfile(filename):
            os.remove(filename)
            raise ValueError("Downloaded file is not a valid zip file.")

        logger.info(f"{filename} downloaded successfully")

    def extract_zip_file(self):
        unzip_path = self.config.unzip_dir
        os.makedirs(unzip_path, exist_ok=True)

        file_path = self.config.local_data_file

        # ✅ Double safety check
        if not zipfile.is_zipfile(file_path):
            raise ValueError("File is not a valid zip file")

        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(unzip_path)

        logger.info("Extraction completed successfully")