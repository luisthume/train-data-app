import os

DIGITRAFFIC_API_URL = "https://rata.digitraffic.fi/api/v1/live-trains/"
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC_NAME = os.environ["TOPIC_NAME"]
RAW_DATA_TOPIC = "raw_train_data"
EXPORT_FILE_PATH_CSV = "output/validated_data.csv"
EXPORT_FILE_PATH_JSON = "output/validated_data.json"