from app import db
from datetime import datetime
from sqlalchemy import Index


class WeatherData(db.Model):
    """Daily weather measurements from weather stations."""
    __tablename__ = 'weather_data'
    
    id = db.Column(db.Integer, primary_key=True)
    station_id = db.Column(db.String(20), nullable=False, index=True)
    date = db.Column(db.Date, nullable=False, index=True)
    max_temp = db.Column(db.Integer)
    min_temp = db.Column(db.Integer)
    precipitation = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_station_date', 'station_id', 'date'),
        db.UniqueConstraint('station_id', 'date', name='uq_station_date')
    )
    
    def __repr__(self):
        return f'<WeatherData {self.station_id} {self.date}>'
    
    @property
    def max_temp_celsius(self):
        return self.max_temp / 10.0 if self.max_temp != -9999 else None
    
    @property
    def min_temp_celsius(self):
        return self.min_temp / 10.0 if self.min_temp != -9999 else None
    
    @property
    def precipitation_cm(self):
        return self.precipitation / 100.0 if self.precipitation != -9999 else None


class WeatherStats(db.Model):
    """Annual weather statistics by station."""
    __tablename__ = 'weather_stats'
    
    id = db.Column(db.Integer, primary_key=True)
    station_id = db.Column(db.String(20), nullable=False, index=True)
    year = db.Column(db.Integer, nullable=False, index=True)
    avg_max_temp = db.Column(db.Float)
    avg_min_temp = db.Column(db.Float)
    total_precipitation = db.Column(db.Float)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_station_year', 'station_id', 'year'),
        db.UniqueConstraint('station_id', 'year', name='uq_station_year')
    )
    
    def __repr__(self):
        return f'<WeatherStats {self.station_id} {self.year}>'
