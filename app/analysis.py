import logging
from datetime import datetime
from app import db
from app.models import WeatherData, WeatherStats
from sqlalchemy import func, and_

logger = logging.getLogger(__name__)


class WeatherStatsCalculator:
    def __init__(self):
        self.stats = {
            'stations_processed': 0,
            'years_processed': 0,
            'stats_calculated': 0,
            'errors': 0
        }
    
    def calculate_all_stats(self):
        start_time = datetime.now()
        logger.info("Starting statistics calculation")
        
        station_years = db.session.query(
            WeatherData.station_id,
            func.extract('year', WeatherData.date).label('year')
        ).distinct().all()
        
        logger.info(f"Found {len(station_years)} station-year combinations")
        
        for station_id, year in station_years:
            try:
                self._calculate_station_year_stats(station_id, int(year))
                self.stats['stats_calculated'] += 1
            except Exception as e:
                logger.error(f"Error calculating stats for {station_id} {year}: {e}")
                self.stats['errors'] += 1
        
        stations = set(station_id for station_id, _ in station_years)
        years = set(int(year) for _, year in station_years)
        self.stats['stations_processed'] = len(stations)
        self.stats['years_processed'] = len(years)
        
        duration = datetime.now() - start_time
        logger.info(f"Completed in {duration}")
        logger.info(f"Stations: {self.stats['stations_processed']}, "
                   f"Years: {self.stats['years_processed']}, "
                   f"Stats: {self.stats['stats_calculated']}, "
                   f"Errors: {self.stats['errors']}")
        
        return self.stats
    
    def calculate_station_stats(self, station_id):
        logger.info(f"Calculating stats for station {station_id}")
        
        years = db.session.query(
            func.extract('year', WeatherData.date).label('year')
        ).filter_by(station_id=station_id).distinct().all()
        
        for year_tuple in years:
            year = int(year_tuple[0])
            try:
                self._calculate_station_year_stats(station_id, year)
                self.stats['stats_calculated'] += 1
            except Exception as e:
                logger.error(f"Error calculating stats for {station_id} {year}: {e}")
                self.stats['errors'] += 1
        
        return self.stats
    
    def _calculate_station_year_stats(self, station_id, year):
        query = db.session.query(WeatherData).filter(
            and_(
                WeatherData.station_id == station_id,
                func.extract('year', WeatherData.date) == year
            )
        )
        
        avg_max_temp = None
        avg_min_temp = None
        total_precipitation = None
        
        # Calculate averages excluding missing data (-9999)
        max_temps = query.filter(WeatherData.max_temp != -9999).with_entities(WeatherData.max_temp).all()
        if max_temps:
            avg_max_temp = sum(temp[0] for temp in max_temps) / len(max_temps) / 10.0
        
        min_temps = query.filter(WeatherData.min_temp != -9999).with_entities(WeatherData.min_temp).all()
        if min_temps:
            avg_min_temp = sum(temp[0] for temp in min_temps) / len(min_temps) / 10.0
        
        precipitations = query.filter(WeatherData.precipitation != -9999).with_entities(WeatherData.precipitation).all()
        if precipitations:
            total_precipitation = sum(precip[0] for precip in precipitations) / 100.0
        
        # Upsert statistics record
        existing_stat = WeatherStats.query.filter_by(
            station_id=station_id,
            year=year
        ).first()
        
        if existing_stat:
            existing_stat.avg_max_temp = avg_max_temp
            existing_stat.avg_min_temp = avg_min_temp
            existing_stat.total_precipitation = total_precipitation
            existing_stat.updated_at = datetime.utcnow()
        else:
            new_stat = WeatherStats(
                station_id=station_id,
                year=year,
                avg_max_temp=avg_max_temp,
                avg_min_temp=avg_min_temp,
                total_precipitation=total_precipitation
            )
            db.session.add(new_stat)
        
        db.session.commit()


def run_stats_calculation():
    calculator = WeatherStatsCalculator()
    return calculator.calculate_all_stats()
