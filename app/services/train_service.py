import logging
import os

from pydantic import ValidationError
from repository.kafka_repository import KafkaRepository
from models.train_data import TrainData
from typing import List
import requests
import pandas as pd
from config import EXPORT_FILE_PATH_CSV, EXPORT_FILE_PATH_JSON
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

class TrainService:
    def __init__(self, kafka_repo: KafkaRepository, api_url: str):
        self.kafka_repo = kafka_repo
        self.api_url = api_url

        self.kafka_repo.start_topic() # TODO not sure if this should be initialized here

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(3),
    )
    def _fetch_from_api(self, api_url):
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()

    def fetch_live_train_data(self) -> List[dict]:
        """Fetch live train data from the external API."""
        try:
            return self._fetch_from_api(self.api_url)
        except requests.RequestException as err:
            logger.error(f"Error fetching data from API: {err}")
            return []

    def produce_train_data(self, train_data: List[dict]):
        """Send raw train data to Kafka."""
        for record in train_data:
            self.kafka_repo.produce(value=record)

    def clean_data_and_export(self, export_format: str) -> None:
        """Validate raw train data and produce validated data to Kafka."""
        raw_data = self.kafka_repo.consume()

        if not raw_data:
            logger.warning("No data available for cleaning and export.")
            return

        preprocessed_data = self._preprocess_raw_data(raw_data)
        cleaned_data = self._process_preprocessed_data(preprocessed_data)

        file_path = ""
        if export_format.lower() == "csv":
            file_path = self._export_to_csv(cleaned_data)
        elif export_format.lower() == "json":
            file_path = self._export_to_json(cleaned_data)
        else:
            logger.error(f"Invalid export format: {export_format}. Use 'csv' or 'json'.")

        return file_path

    def _preprocess_raw_data(self, raw_data: list) -> list:
        df = pd.DataFrame(raw_data)

        time_table_rows = df["timeTableRows"] if "timeTableRows" in df else None

        str_columns = df.select_dtypes("object").columns
        df[str_columns] = df[str_columns].apply(lambda x: x.str.strip())

        # Normalize dates
        df["departureDate"] = pd.to_datetime(df["departureDate"])
        df["timetableAcceptanceDate"] = pd.to_datetime(df["timetableAcceptanceDate"])

        if time_table_rows is not None:
            df["timeTableRows"] = time_table_rows

        # df = df.drop_duplicates(subset=["trainNumber", "departureDate"]) TODO not sure if this should be used
        return df.to_dict(orient="records")

    def _process_preprocessed_data(self, preprocessed_data: list) -> list:
        """Validate, standardize, and deduplicate the raw data."""
        unique_records = set()
        cleaned_data = []

        for record in preprocessed_data:
            try:
                valid_record = TrainData(**record)
                record_key = (valid_record.trainNumber, valid_record.departureDate)

                if record_key not in unique_records:
                    unique_records.add(record_key)
                    cleaned_data.append(valid_record.model_dump())
            except Exception as err:
                logger.error(f"Failed for record: {record} | Errors: {err}")

        return cleaned_data

    def _export_to_csv(self, data):
        """Export the validated data to a CSV file using pandas."""
        if not data:
            logger.warning("No validated data available for export.")
            return ""
        
        file_path = EXPORT_FILE_PATH_CSV
        df = pd.DataFrame(data)
        try:
            df.to_csv(file_path, index=False)
            logger.info(f"Validated data exported to CSV at {file_path}")
            return file_path
        except Exception as err:
            logger.error(f"Error exporting data to CSV: {err}")
            return ""
        
    def _export_to_json(self, data):
        """Export the validated data to a JSON file using pandas."""
        if not data:
            logger.warning("No validated data available for export.")
            return ""
        
        file_path = EXPORT_FILE_PATH_JSON
        df = pd.DataFrame(data)
        try:
            df.to_json(file_path, orient="records", indent=4, force_ascii=False, date_format="iso")
            logger.info(f"Validated data exported to JSON at {file_path}")
            return file_path
        except Exception as err:
            logger.error(f"Error exporting data to JSON: {err}")
            return ""