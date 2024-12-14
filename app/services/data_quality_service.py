import pandas as pd
from repository.kafka_repository import KafkaRepository
import logging

logger = logging.getLogger(__name__)

class DataQualityService:
    def __init__(self, kafka_repo: KafkaRepository):
        self.kafka_repo = kafka_repo

    def generate_data_quality_report(self) -> dict:
        """Generate a data quality report from validated data."""
        data = self.kafka_repo.consume()
        if not data:
            logger.warning("No data found. Returning empty report.")
            return {
                "total_records": 0,
                "missing_values": {},
                "duplicates": 0,
                "anomalies": {}
            }
        
        df = pd.DataFrame(data)

        return {
            "total_records": len(df),
            "missing_values": self._check_missing_values(df),
            "duplicates": self._count_duplicates(df),
            "anomalies": self._find_anomalies(df)
        }

    def _check_missing_values(self, df: pd.DataFrame) -> dict:
        """Check for missing values in critical fields."""
        critical_fields = ["trainNumber", "departureDate", "timeTableRows"]
        return {field: int(df[field].isnull().sum()) for field in critical_fields if field in df}

    def _count_duplicates(self, df: pd.DataFrame) -> int:
        """Count duplicate records based on unique identifiers."""
        return int(df.duplicated(subset=["trainNumber", "departureDate"]).sum())

    def _find_anomalies(self, df: pd.DataFrame) -> dict:
        """Identify anomalies in the data."""
        anomalies = {}

        # Check for invalid or non-standard date formats
        try:
            invalid_dates = df.loc[pd.to_datetime(df['departureDate'], errors='coerce').isna()]
            anomalies['departureDate'] = len(invalid_dates)
        except KeyError:
            anomalies['departureDate'] = "Field missing"

        # Check for negative or unexpected train numbers
        if "trainNumber" in df:
            anomalies['trainNumber'] = int((df['trainNumber'] < 0).sum())

        return anomalies
