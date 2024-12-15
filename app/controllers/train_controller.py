import logging
import os
from fastapi.responses import FileResponse
from fastapi import APIRouter, HTTPException
from repository.kafka_repository import KafkaRepository
from services.train_service import TrainService
from services.data_quality_service import DataQualityService
from config import DIGITRAFFIC_API_URL, KAFKA_BROKER, TOPIC_NAME

router = APIRouter(prefix="/train", tags=["Train Data"])
logger = logging.getLogger(__name__)

train_service = TrainService(
    KafkaRepository(broker=KAFKA_BROKER, topic=TOPIC_NAME, group_id="train_service_group"),
    DIGITRAFFIC_API_URL
)
data_quality_service = DataQualityService(
    KafkaRepository(broker=KAFKA_BROKER, topic=TOPIC_NAME, group_id="data_quality_service_group")
)

@router.get("/ingest", summary="Fetch and produce train data.")
async def ingest_train_data() -> dict:
    """
    Endpoint to fetch train data and produce it to the Kafka topic.
    :return: Status message.
    """
    train_data = train_service.fetch_live_train_data()
    if not train_data:
        return {"status": "error", "message": "Failed to fetch train data"}

    train_service.produce_train_data(train_data)
    return {"status": "success", "message": f"Train data ingested into Kafka ({train_service.kafka_repo.topic})"}


@router.get("/quality-report", summary="Generate data quality report.")
async def data_quality_report() -> dict:
    """
    Endpoint to consume raw train data and generate a data quality report.
    :return: Data quality report.
    """
    report = data_quality_service.generate_data_quality_report()
    return {"status": "success", "data_quality_report": report}


@router.post("/clean-data/{format}", summary="Clean and export data.", status_code=200)
async def clean_data(format: str) -> FileResponse:
    """
    Clean train data and export it to the specified format (CSV or JSON).

    :param format: The export format ('csv' or 'json').
    :return: FileResponse with the exported file.
    """
    valid_formats = ["csv", "json"]

    if format.lower() not in valid_formats:
        raise HTTPException(status_code=400, detail=f"Invalid format. Choose one of: {', '.join(valid_formats)}.")

    try:
        logger.info(f"Initiating data cleaning and export to {format.upper()} format.")
        file_path = train_service.clean_data_and_export(export_format=format.lower())

        if not file_path or not os.path.exists(file_path):
            detail_message = (
                f"No data was processed or the exported {format.upper()} file could not be found. "
                f"Ensure there is valid train data to process."
            )
            logger.warning(detail_message)
            raise HTTPException(status_code=404, detail=detail_message)

        return FileResponse(
            file_path,
            media_type="text/csv" if format.lower() == "csv" else "application/json",
            headers={"Content-Disposition": f"attachment; filename={os.path.basename(file_path)}"}
        )
    
    except HTTPException as http_err:
        logger.error(f"HTTP error during data cleaning/export: {http_err.detail}")
        raise http_err

    except Exception as err:
        logger.exception("Unexpected error during data cleaning and export.")
        raise HTTPException(
            status_code=500, 
            detail=f"Data cleaning and export failed due to an internal error: {str(err)}"
        )