from pydantic import BaseModel
from typing import List, Optional
from datetime import date, datetime


class TimeTableRow(BaseModel):
    stationShortCode: str
    stationUICCode: int
    countryCode: str
    type: str  # Example: "DEPARTURE" or "ARRIVAL"
    trainStopping: bool
    commercialStop: Optional[bool] = None  # appears only when true
    commercialTrack: Optional[str] = None
    cancelled: bool
    scheduledTime: datetime
    causes: List[dict]  # Assuming causes is a list of dictionaries

class TrainData(BaseModel):
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