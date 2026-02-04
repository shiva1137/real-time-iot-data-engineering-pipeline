"""
Pydantic models for API request/response (Topic 7).
"""

from datetime import date, datetime
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator


class SensorSummary(BaseModel):
    """Single sensor with latest reading."""
    sensor_id: str
    location: Optional[str] = None
    state: Optional[str] = None
    device_type: Optional[str] = None
    latest_temperature: Optional[float] = None
    latest_humidity: Optional[float] = None
    latest_timestamp: Optional[datetime] = None


class SensorListResponse(BaseModel):
    """List of sensors with optional pagination."""
    sensors: List[SensorSummary]
    total: int
    limit: int
    offset: int


class AnalyticsDataPoint(BaseModel):
    """Single time-series point."""
    timestamp: datetime
    avg_temperature: Optional[float] = None
    max_temperature: Optional[float] = None
    min_temperature: Optional[float] = None
    avg_humidity: Optional[float] = None
    reading_count: Optional[int] = None


class AnalyticsResponse(BaseModel):
    """Analytics time series for a sensor."""
    sensor_id: str
    start_date: date
    end_date: date
    granularity: str = "daily"
    data: List[AnalyticsDataPoint]


class HealthComponent(BaseModel):
    """Status of one dependency."""
    status: str  # "up" | "down"
    message: Optional[str] = None
    details: Optional[dict] = None


class HealthResponse(BaseModel):
    """Health check response."""
    status: str  # "healthy" | "degraded" | "unhealthy"
    timestamp: datetime
    database: HealthComponent
    latest_data: Optional[HealthComponent] = None
    recent_failures: Optional[int] = None


class ErrorResponse(BaseModel):
    """Consistent error response."""
    error_code: str
    message: str
    request_id: Optional[str] = None
    details: Optional[dict] = None
