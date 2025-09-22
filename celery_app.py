# celery_app.py

from celery import Celery
import config
from logging_config import setup_logging

# Apply logging configuration
setup_logging()

app = Celery(
    "gym_pipeline",
    broker=config.CELERY_BROKER_URL,
    backend=config.CELERY_RESULT_BACKEND,
    include=["pipeline.tasks"]
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_routes={
        "pipeline.tasks.get_llm_description": {"queue": "llm"},
        "pipeline.tasks.get_llm_amenities": {"queue": "llm"},
        "pipeline.tasks.get_llm_misc_details": {"queue": "llm"},
    },
)
