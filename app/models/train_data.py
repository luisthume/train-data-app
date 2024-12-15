from pydantic import BaseModel, model_validator
from typing import List, Optional
from datetime import date, datetime


class TimeTableRow(BaseModel):
    """
    Represents a row in the train's timetable.
    """
    stationShortCode: str
    stationUICCode: int
    countryCode: str
    type: str  # e.g.: "DEPARTURE" or "ARRIVAL"
    trainStopping: bool
    commercialStop: Optional[bool] = None
    commercialTrack: Optional[str] = None
    cancelled: bool
    scheduledTime: datetime
    causes: List[dict]

class TrainData(BaseModel):
    """
    Represents train schedule data.
    """
    trainNumber: int
    departureDate: date
    operatorUICCode: int
    operatorShortCode: str
    trainType: str
    trainCategory: str
    commuterLineID: Optional[str] = None
    runningCurrently: bool
    cancelled: bool
    version: int
    timetableType: str
    timetableAcceptanceDate: datetime
    timeTableRows: List[TimeTableRow] 

    # # Logic level validators
    # @model_validator(mode='after')
    # def validate_dates(cls, values):
    #     if values.departureDate > values.timetableAcceptanceDate.date():
    #         raise ValueError("Departure date cannot be after timetable acceptance date.")
    #     return values