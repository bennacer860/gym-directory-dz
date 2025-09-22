#!/usr/bin/env python3
"""
Run the optimized pipeline with improved LLM enrichment.

Usage:
    python run_pipeline_optimized.py [--test] [--skip-llm] [--batch-size N]
"""

import click
import logging
from pipeline.models import setup_database
from pipeline.tasks_optimized import (
    start_full_pipeline, 
    batch_enrich_places,
    get_enrichment_stats
)
from celery import group
import config

logger = logging.getLogger(__name__)

@click.command()
@click.option('--test', is_flag=True, help="Run in test mode (1 city, 3 places).")
@click.option('--skip-llm', is_flag=True, help="Skip LLM enrichment tasks.")
@click.option('--batch-size', default=10, help="Batch size for LLM processing.")
@click.option('--stats', is_flag=True, help="Show enrichment statistics.")
@click.option('--use-batch', is_flag=True, help="Use batch processing for places.")
def start(test, skip_llm, batch_size, stats, use_batch):
    """Start the optimized data pipeline."""
    
    # Show stats if requested
    if stats:
        result = get_enrichment_stats.delay()
        stats_data = result.get(timeout=30)
        click.echo("\n=== Enrichment Statistics ===")
        click.echo(f"Completion by Status: {stats_data['completion_by_status']}")
        click.echo(f"Field Completion: {stats_data['field_completion']}")
        click.echo(f"Cache Info: {stats_data['cache_info']}")
        return
    
    click.echo("🚀 Initializing optimized pipeline...")
    setup_database()
    
    if use_batch:
        # Example of batch processing specific places
        click.echo(f"Using batch processing with size {batch_size}")
        
        # Get places that need enrichment
        from pipeline.models import get_db_connection
        conn = get_db_connection()
        places = conn.execute("""
            SELECT place_id 
            FROM places 
            WHERE status IN ('ENRICHMENT_PENDING', 'FAILED_ENRICH')
            LIMIT 50
        """).fetchall()
        conn.close()
        
        if places:
            place_ids = [p['place_id'] for p in places]
            click.echo(f"Found {len(place_ids)} places to enrich in batches")
            batch_enrich_places.delay(place_ids, batch_size=batch_size)
        else:
            click.echo("No places found that need enrichment")
    else:
        # Regular pipeline start
        mode = "TEST" if test else "FULL"
        llm_status = "DISABLED" if skip_llm else "ENABLED"
        click.echo(f"Starting {mode} pipeline with LLM enrichment {llm_status}")
        
        # Note: You would need to modify start_full_pipeline to use optimized tasks
        # For now, this demonstrates the concept
        start_full_pipeline.delay(test_mode=test, skip_llm=skip_llm)
    
    click.echo("✅ Pipeline tasks queued. Monitor with: celery -A celery_app flower")

if __name__ == "__main__":
    start()