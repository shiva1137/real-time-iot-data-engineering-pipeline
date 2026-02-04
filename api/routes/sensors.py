"""
Sensors and analytics routes (Topic 7).
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Query, HTTPException, Depends

from api.models.schemas import (
    SensorSummary,
    SensorListResponse,
    AnalyticsDataPoint,
    AnalyticsResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sensors", tags=["sensors"])


def get_db():
    """Dependency that yields a DB connection from pool."""
    from api.database import get_connection
    return get_connection()


@router.get("", response_model=SensorListResponse)
def list_sensors(
    location: Optional[str] = Query(None, description="Filter by city"),
    status: Optional[str] = Query(None, description="active/inactive/faulty"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db=Depends(get_db),
):
    """List all sensors with latest readings. Cache TTL 1 min (handled by client/cache layer)."""
    # Build query from real_time_aggregates or processed_daily - use latest window per sensor
    try:
        with db.cursor() as cur:
            cur.execute("""
                SELECT sensor_id, location, state, device_type,
                       avg_temperature AS latest_temperature,
                       avg_humidity AS latest_humidity,
                       window_start AS latest_timestamp
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY sensor_id ORDER BY window_start DESC) AS rn
                    FROM real_time_aggregates
                    WHERE (%s::text IS NULL OR location = %s)
                ) t
                WHERE rn = 1
                ORDER BY sensor_id
                LIMIT %s OFFSET %s
            """, (location, location, limit, offset))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        sensors = [
            SensorSummary(
                sensor_id=r[cols.index("sensor_id")],
                location=r[cols.index("location")] if "location" in cols else None,
                state=r[cols.index("state")] if "state" in cols else None,
                device_type=r[cols.index("device_type")] if "device_type" in cols else None,
                latest_temperature=r[cols.index("latest_temperature")] if "latest_temperature" in cols else None,
                latest_humidity=r[cols.index("latest_humidity")] if "latest_humidity" in cols else None,
                latest_timestamp=r[cols.index("latest_timestamp")] if "latest_timestamp" in cols else None,
            )
            for r in rows
        ]
        with db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM (SELECT 1 FROM real_time_aggregates WHERE (%s::text IS NULL OR location = %s) GROUP BY sensor_id) t",
                (location, location),
            )
            total = cur.fetchone()[0]
        return SensorListResponse(sensors=sensors, total=total, limit=limit, offset=offset)
    except Exception as e:
        logger.exception("list_sensors failed")
        raise HTTPException(status_code=500, detail="Database error")


@router.get("/{sensor_id}/analytics", response_model=AnalyticsResponse)
def get_analytics(
    sensor_id: str,
    start_date: date = Query(..., description="YYYY-MM-DD"),
    end_date: date = Query(..., description="YYYY-MM-DD"),
    granularity: str = Query("daily", description="hourly or daily"),
    db=Depends(get_db),
):
    """Get analytics time series for a sensor. Cache TTL 5 min."""
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    if (end_date - start_date).days > 90:
        raise HTTPException(status_code=400, detail="Max 90 days range")
    if granularity not in ("hourly", "daily"):
        raise HTTPException(status_code=400, detail="granularity must be hourly or daily")

    try:
        with db.cursor() as cur:
            if granularity == "daily":
                cur.execute("""
                    SELECT date AS ts,
                           AVG(avg_temperature) AS avg_temperature,
                           MAX(max_temperature) AS max_temperature,
                           MIN(min_temperature) AS min_temperature,
                           AVG(avg_humidity) AS avg_humidity,
                           SUM(reading_count) AS reading_count
                    FROM processed_daily
                    WHERE sensor_id = %s AND date >= %s AND date <= %s
                    GROUP BY date ORDER BY date
                """, (sensor_id, start_date, end_date))
            else:
                cur.execute("""
                    SELECT (date + (hour || ' hours')::interval) AS ts,
                           avg_temperature, max_temperature, min_temperature,
                           avg_humidity, reading_count
                    FROM processed_daily
                    WHERE sensor_id = %s AND date >= %s AND date <= %s
                    ORDER BY date, hour
                """, (sensor_id, start_date, end_date))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        data = []
        for r in rows:
            ts = r[cols.index("ts")]
            if hasattr(ts, "isoformat"):
                ts = ts
            else:
                ts = datetime.combine(ts, datetime.min.time()) if hasattr(ts, "year") else datetime.utcnow()
            data.append(AnalyticsDataPoint(
                timestamp=ts,
                avg_temperature=r[cols.index("avg_temperature")] if "avg_temperature" in cols else None,
                max_temperature=r[cols.index("max_temperature")] if "max_temperature" in cols else None,
                min_temperature=r[cols.index("min_temperature")] if "min_temperature" in cols else None,
                avg_humidity=r[cols.index("avg_humidity")] if "avg_humidity" in cols else None,
                reading_count=r[cols.index("reading_count")] if "reading_count" in cols else None,
            ))
        return AnalyticsResponse(
            sensor_id=sensor_id,
            start_date=start_date,
            end_date=end_date,
            granularity=granularity,
            data=data,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("get_analytics failed")
        raise HTTPException(status_code=500, detail="Database error")
