from typing import Dict
from pydantic import BaseModel, Field


class DailyData(BaseModel):
    open: float = Field(alias="1. open")
    high: float = Field(alias="2. high")
    low: float = Field(alias="3. low")
    close: float = Field(alias="4. close")
    volume: float = Field(alias="5. volume")


class AlphaVantageResponse(BaseModel):
    meta_data: dict = Field(alias="Meta Data")
    time_series_daily: Dict[str, DailyData] = Field(alias="Time Series (Daily)")

