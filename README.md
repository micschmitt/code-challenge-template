# Weather Data API

REST API for weather station data ingestion, analysis, and retrieval with SQLite backend.

## Quick Start

```bash
# Setup
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Ingest data
python run.py ingest-data --data-dir wx_data

# Calculate statistics  
python run.py calculate-stats

# Start server
python run.py
```

Server runs at `http://localhost:5000` with docs at `/docs/`

## API Endpoints

### `GET /weather`
Query weather records with filtering and pagination.

**Parameters:** `station_id`, `date`, `start_date`, `end_date`, `page`, `per_page`

```bash
curl "http://localhost:5000/weather?station_id=USC00110072&start_date=2014-01-01&per_page=10"
```

### `GET /weather/stats` 
Query calculated annual statistics.

**Parameters:** `station_id`, `year`, `start_year`, `end_year`, `page`, `per_page`

```bash
curl "http://localhost:5000/weather/stats?station_id=USC00110072&year=2014"
```

## Data Models

**WeatherData**: Raw daily measurements (station_id, date, max_temp, min_temp, precipitation)  
**WeatherStats**: Annual aggregates (station_id, year, avg_max_temp, avg_min_temp, total_precipitation)

## Testing

```bash
pytest tests/ -v
```

## Architecture

- **Database**: SQLite with SQLAlchemy ORM
- **API**: Flask-RESTX with auto-generated OpenAPI docs
- **Data Processing**: Batch ingestion with duplicate detection
- **Units**: Temperatures in Celsius, precipitation in centimeters

## AWS Deployment

**Recommended Stack:**
- ECS Fargate + RDS PostgreSQL
- S3 for data files + Lambda for processing
- EventBridge for scheduled analytics
- CloudWatch for monitoring

## Project Structure

```
app/
├── __init__.py       # Flask app factory
├── models.py         # SQLAlchemy models
├── ingestion.py      # Data processing
├── analysis.py       # Statistics calculation
├── api.py            # REST endpoints
└── commands.py       # CLI commands
tests/test_app.py     # Unit tests
run.py               # Application entry point
```
