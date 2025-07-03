import os
import logging
from datetime import datetime
from pathlib import Path
from app import db
from app.models import WeatherData
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)


class WeatherDataIngester:
    def __init__(self, batch_size=1000):
        self.batch_size = batch_size
        self.stats = {
            'files_processed': 0,
            'records_ingested': 0,
            'records_skipped': 0,
            'errors': 0
        }
    
    def ingest_from_directory(self, data_directory):
        start_time = datetime.now()
        logger.info(f"Starting ingestion from {data_directory}")
        
        data_path = Path(data_directory)
        if not data_path.exists():
            raise FileNotFoundError(f"Data directory not found: {data_directory}")
        
        weather_files = list(data_path.glob("*.txt"))
        logger.info(f"Found {len(weather_files)} files")
        
        for file_path in weather_files:
            try:
                station_id = file_path.stem
                self._ingest_file(file_path, station_id)
                self.stats['files_processed'] += 1
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                self.stats['errors'] += 1
        
        duration = datetime.now() - start_time
        logger.info(f"Completed in {duration}")
        logger.info(f"Files: {self.stats['files_processed']}, "
                   f"Records: {self.stats['records_ingested']}, "
                   f"Skipped: {self.stats['records_skipped']}, "
                   f"Errors: {self.stats['errors']}")
        
        return self.stats
    
    def _ingest_file(self, file_path, station_id):
        logger.info(f"Processing {file_path} (Station: {station_id})")
        
        batch = []
        line_count = 0
        
        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    record = self._parse_line(line, station_id)
                    if record:
                        batch.append(record)
                        line_count += 1
                        
                        if len(batch) >= self.batch_size:
                            self._process_batch(batch)
                            batch = []
                            
                except Exception as e:
                    logger.warning(f"Error parsing line: {line} - {e}")
                    self.stats['errors'] += 1
        
        if batch:
            self._process_batch(batch)
        
        logger.info(f"Processed {line_count} lines from {file_path}")
    
    def _parse_line(self, line, station_id):
        parts = line.split('\t')
        if len(parts) != 4:
            raise ValueError(f"Expected 4 fields, got {len(parts)}")
        
        try:
            date_str = parts[0].strip()
            max_temp = int(parts[1].strip())
            min_temp = int(parts[2].strip())
            precipitation = int(parts[3].strip())
            
            date_obj = datetime.strptime(date_str, '%Y%m%d').date()
            
            return WeatherData(
                station_id=station_id,
                date=date_obj,
                max_temp=max_temp,
                min_temp=min_temp,
                precipitation=precipitation
            )
            
        except ValueError as e:
            raise ValueError(f"Error parsing fields: {e}")
    
    def _process_batch(self, batch):
        try:
            db.session.bulk_save_objects(batch)
            db.session.commit()
            self.stats['records_ingested'] += len(batch)
        except IntegrityError:
            db.session.rollback()
            self._process_individually(batch)
    
    def _process_individually(self, batch):
        for record in batch:
            try:
                db.session.add(record)
                db.session.commit()
                self.stats['records_ingested'] += 1
            except IntegrityError:
                db.session.rollback()
                self.stats['records_skipped'] += 1


def run_ingestion(data_directory="wx_data"):
    ingester = WeatherDataIngester()
    return ingester.ingest_from_directory(data_directory)
