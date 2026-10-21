"""Request/response schemas (pydantic)."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class PredictRequest(BaseModel):
    """Feature record for the severity classifier. Units: see GET /api/model/status.
    Optional measurements may be null (imputed with training medians)."""
    model_config = ConfigDict(extra="forbid", json_schema_extra={"example": {
        "rain_3h_mm": 55.0, "rain_max_1h_mm_hr": 32.0, "level_ratio_max_3h": 0.92, "humidity_avg_3h": 94.0,
        "disrupted_ratio_1h": 0.45, "speed_ratio_avg_1h": 0.42, "reports_6h": 4,
        "water_obs_available": 1, "traffic_obs_available": 1, "zone_id": "Z-R04-C02"}})

    rain_3h_mm: Optional[float] = Field(None, ge=0, le=900, description="mm over the previous 3 h")
    rain_max_1h_mm_hr: Optional[float] = Field(None, ge=0, le=300, description="mm/hr peak in the previous hour")
    level_ratio_max_3h: Optional[float] = Field(None, ge=0, le=10, description="water level / danger level")
    humidity_avg_3h: Optional[float] = Field(None, ge=0, le=100, description="% relative humidity")
    disrupted_ratio_1h: Optional[float] = Field(None, ge=0, le=1, description="share of disrupted traffic obs")
    speed_ratio_avg_1h: Optional[float] = Field(None, ge=0, le=2, description="speed / free-flow speed")
    reports_6h: float = Field(..., ge=0, le=10000, description="public reports in the previous 6 h")
    water_obs_available: int = Field(..., ge=0, le=1)
    traffic_obs_available: int = Field(..., ge=0, le=1)
    zone_id: Optional[str] = Field(None, max_length=32)
    input_source: Optional[str] = Field(None, max_length=120)


class ErrorResponse(BaseModel):
    detail: str
