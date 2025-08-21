from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date
from uuid import UUID

class RestCountryModel(BaseModel):
    country_name: str
    capital: Optional[str]
    country_code: str  # CHAR(2)
    latitude: Optional[float]
    longitude: Optional[float]
    region: Optional[str]
    subregion: Optional[str]
    population: Optional[int]

class POIModel(BaseModel):
    xid: str
    name: Optional[str]
    description: Optional[str]
    image: Optional[str]
    kinds: Optional[str]
    otm_url: Optional[str]
    country_code: str
    latitude: float
    longitude: float
    poi_timestamp: Optional[datetime]

class TouristModel(BaseModel):
    user_id: UUID
    name: str
    country: str
    registration_date: date


class BookingModel(BaseModel):
    booking_id: UUID
    user_id: UUID
    destination: str
    type: str
    category: str
    status: str
    price: float
    start_date: date
    end_date: date

class VisitModel(BaseModel):
    visit_id: UUID
    user_id: UUID
    place_id: str
    timestamp: datetime

class SearchEventModel(BaseModel):
    search_id: UUID
    user_id: UUID
    search_country: str
    timestamp: datetime
    converted_to_booking: bool