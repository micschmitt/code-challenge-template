import click
import os
from flask.cli import with_appcontext
from app.ingestion import run_ingestion
from app.analysis import run_stats_calculation


@click.command()
@click.option('--data-dir', default='wx_data', help='Directory containing weather data files')
@with_appcontext
def ingest_data(data_dir):
    """Ingest weather data from text files into the database."""
    if not os.path.exists(data_dir):
        click.echo(f"Error: Data directory '{data_dir}' not found")
        return
    
    click.echo(f"Starting data ingestion from {data_dir}...")
    
    try:
        stats = run_ingestion(data_dir)
        
        click.echo("Data ingestion completed!")
        click.echo(f"Files processed: {stats['files_processed']}")
        click.echo(f"Records ingested: {stats['records_ingested']}")
        click.echo(f"Records skipped (duplicates): {stats['records_skipped']}")
        click.echo(f"Errors: {stats['errors']}")
        
    except Exception as e:
        click.echo(f"Error during ingestion: {str(e)}")


@click.command()
@with_appcontext
def calculate_stats():
    """Calculate weather statistics for all stations and years."""
    click.echo("Starting statistics calculation...")
    
    try:
        stats = run_stats_calculation()
        
        click.echo("Statistics calculation completed!")
        click.echo(f"Stations processed: {stats['stations_processed']}")
        click.echo(f"Years processed: {stats['years_processed']}")
        click.echo(f"Statistics calculated: {stats['stats_calculated']}")
        click.echo(f"Errors: {stats['errors']}")
        
    except Exception as e:
        click.echo(f"Error during calculation: {str(e)}")


def register_commands(app):
    app.cli.add_command(ingest_data)
    app.cli.add_command(calculate_stats)
