"""
Health check endpoint (Topic 7 & 9).

Checks database connection, latest data timestamp, optional recent failures.
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter
from api.models.schemas import HealthResponse, HealthComponent
from api.database import check_connection, get_latest_data_timestamp

logger = logging.getLogger(__name__)
router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health():
    """
    Health check: DB connection, latest data timestamp.
    Alert if data freshness > 5 min (via monitoring.alerts).
    """
    try:
        from monitoring.alerts import check_data_freshness, Severity, emit_alert
    except ImportError:
        check_data_freshness = lambda ts, now: None
        Severity = type("Severity", (), {"HIGH": "HIGH"})
        emit_alert = lambda name, msg, sev, ctx: None

    now = datetime.now(timezone.utc)
    db_ok, db_msg = check_connection()
    latest_ok, latest_ts = get_latest_data_timestamp()

    db_component = HealthComponent(
        status="up" if db_ok else "down",
        message=None if db_ok else db_msg,
    )
    latest_component = None
    if latest_ok and latest_ts is not None:
        # Ensure timezone-aware for comparison
        if latest_ts.tzinfo is None:
            from datetime import timezone as tz
            latest_ts = latest_ts.replace(tzinfo=tz.utc)
        check_data_freshness(latest_ts, now)
        latest_component = HealthComponent(
            status="up",
            message=f"Latest data: {latest_ts.isoformat()}",
            details={"latest_timestamp": latest_ts.isoformat()},
        )
    elif latest_ok and latest_ts is None:
        latest_component = HealthComponent(
            status="degraded",
            message="No data yet",
        )
    else:
        latest_component = HealthComponent(status="down", message="Could not get latest data")

    if not db_ok:
        emit_alert("health", "Database connection failed", Severity.HIGH, {"message": db_msg})

    status = "healthy"
    if not db_ok:
        status = "unhealthy"
    elif latest_component and latest_component.status == "degraded":
        status = "degraded"

    return HealthResponse(
        status=status,
        timestamp=now,
        database=db_component,
        latest_data=latest_component,
        recent_failures=None,
    )
