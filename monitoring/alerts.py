"""
Alert Logic (Topic 9)

Threshold-based alerts: data freshness, quality %, DAG failures, throughput,
DLQ size, API error rate. Severity HIGH/MEDIUM. Output to console/log (Slack optional).
"""

import logging
from enum import Enum
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class Severity(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"


# Thresholds (from Learning Guide Topic 9)
THRESHOLDS = {
    "data_freshness_minutes": (5, Severity.HIGH),
    "quality_pct_min": (95, Severity.HIGH),
    "throughput_msgs_per_min_min": (500, Severity.MEDIUM),
    "dlq_size_gb": (1.0, Severity.MEDIUM),
    "api_error_rate_pct_max": (5.0, Severity.MEDIUM),
}


def emit_alert(
    name: str,
    message: str,
    severity: Severity = Severity.HIGH,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """Emit an alert to console/log. Optional: send to Slack/webhook."""
    payload = {
        "alert": name,
        "message": message,
        "severity": severity.value,
        "context": context or {},
    }
    if severity == Severity.HIGH:
        logger.error("ALERT [%s] %s | %s", severity.value, name, message, extra={"context": payload})
    else:
        logger.warning("ALERT [%s] %s | %s", severity.value, name, message, extra={"context": payload})


def check_data_freshness(latest_timestamp_utc, now_utc) -> None:
    """Alert if latest data is older than 5 minutes."""
    from datetime import datetime, timezone
    if latest_timestamp_utc is None:
        emit_alert("data_freshness", "No data timestamp available", Severity.HIGH)
        return
    if isinstance(latest_timestamp_utc, str):
        latest_timestamp_utc = datetime.fromisoformat(latest_timestamp_utc.replace("Z", "+00:00"))
    delta_minutes = (now_utc - latest_timestamp_utc).total_seconds() / 60
    threshold, sev = THRESHOLDS["data_freshness_minutes"]
    if delta_minutes > threshold:
        emit_alert(
            "data_freshness",
            f"Data freshness {delta_minutes:.1f} min > {threshold} min",
            sev,
            {"latest_timestamp": str(latest_timestamp_utc), "minutes_old": delta_minutes},
        )


def check_quality_pct(valid_pct: float) -> None:
    """Alert if valid record % is below 95%."""
    threshold, sev = THRESHOLDS["quality_pct_min"]
    if valid_pct < threshold:
        emit_alert(
            "data_quality",
            f"Quality {valid_pct:.1f}% < {threshold}%",
            sev,
            {"valid_pct": valid_pct},
        )


def check_throughput(msgs_per_min: float) -> None:
    """Alert if throughput below 500 msgs/min."""
    threshold, sev = THRESHOLDS["throughput_msgs_per_min_min"]
    if msgs_per_min < threshold:
        emit_alert(
            "throughput",
            f"Throughput {msgs_per_min:.0f} msgs/min < {threshold}",
            sev,
            {"msgs_per_min": msgs_per_min},
        )


def check_dlq_size_gb(size_gb: float) -> None:
    """Alert if DLQ size > 1 GB."""
    threshold, sev = THRESHOLDS["dlq_size_gb"]
    if size_gb > threshold:
        emit_alert(
            "dlq_size",
            f"DLQ size {size_gb:.2f} GB > {threshold} GB",
            sev,
            {"size_gb": size_gb},
        )


def check_api_error_rate(error_rate_pct: float) -> None:
    """Alert if API error rate > 5%."""
    threshold, sev = THRESHOLDS["api_error_rate_pct_max"]
    if error_rate_pct > threshold:
        emit_alert(
            "api_errors",
            f"API error rate {error_rate_pct:.1f}% > {threshold}%",
            sev,
            {"error_rate_pct": error_rate_pct},
        )


def alert_dag_failed(dag_id: str, run_id: str, task_id: Optional[str] = None) -> None:
    """Alert on DAG/task failure."""
    msg = f"DAG failed: {dag_id} run {run_id}"
    if task_id:
        msg += f" task {task_id}"
    emit_alert("dag_failure", msg, Severity.HIGH, {"dag_id": dag_id, "run_id": run_id, "task_id": task_id})
