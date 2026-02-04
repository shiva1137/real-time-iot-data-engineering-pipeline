"""
FastAPI Main Application (Topic 7)

REST API for IoT sensor data. Endpoints: GET /health, GET /sensors, GET /sensors/{id}/analytics.
"""

import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.database import init_pool
from api.routes import sensors, health
from api.models.schemas import ErrorResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: init DB pool. Shutdown: close pool."""
    init_pool()
    yield
    # Optional: close pool
    # from api.database import _pool
    # if _pool: _pool.closeall()


app = FastAPI(
    title="IoT Data API",
    description="REST API for IoT sensor data and analytics",
    version="1.0.0",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(sensors.router)
app.include_router(health.router)


@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """Add request_id to state for error responses."""
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    response = await call_next(request)
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Return consistent error response with request_id."""
    request_id = getattr(request.state, "request_id", None)
    logger.exception("Unhandled error: %s", exc)
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error_code="INTERNAL_ERROR",
            message="An unexpected error occurred",
            request_id=request_id,
            details={"type": type(exc).__name__},
        ).model_dump(),
    )


@app.get("/")
def root():
    """Root redirect to docs."""
    return {"message": "IoT Data API", "docs": "/docs", "health": "/health"}
