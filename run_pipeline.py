# run_pipeline.py

import click
import logging
import os
from dotenv import load_dotenv

from pipeline.models import setup_database
from pipeline.tasks import start_full_pipeline, export_data, export_ui_json # Import export tasks
from logging_config import setup_logging
import config

# Load environment variables from .env file
load_dotenv()

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

@click.group()
def cli():
    pass

@cli.command()
def initdb():
    """Initializes the SQLite database with the necessary tables."""
    logger.info("Initializing database...")
    setup_database()
    logger.info("Database initialized successfully.")

@cli.command()
def resetdb():
    """Deletes the existing database and re-initializes it."""
    db_path = config.DB_PATH
    if os.path.exists(db_path):
        logger.info(f"Deleting existing database: {db_path}")
        os.remove(db_path)
        logger.info("Database deleted.")
    else:
        logger.info("No existing database found to delete.")

    logger.info("Initializing new database...")
    setup_database()
    logger.info("Database reset and re-initialized successfully.")

@cli.command()
@click.option('--test', is_flag=True, help="Run the pipeline in test mode (1 city, 1 page, 3 places).")
@click.option('--skip-llm', is_flag=True, help="Skip LLM enrichment tasks.") # New option
def start(test, skip_llm):
    """Starts the data pipeline."""
    logger.info("Starting pipeline...")
    start_full_pipeline.delay(test_mode=test, skip_llm=skip_llm) # Pass skip_llm
    logger.info("Pipeline task dispatched to Celery.")

@cli.command()
def export():
    """Triggers the data export tasks (CSV, JSONL, UI JSON)."""
    logger.info("Dispatching data export tasks...")
    export_data.delay()
    export_ui_json.delay()
    logger.info("Export tasks dispatched. Check Celery worker logs and Flower UI.")

if __name__ == '__main__':
    cli()
