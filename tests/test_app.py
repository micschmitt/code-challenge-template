"""Unit tests for the weather data application."""

import pytest
import os
import tempfile
from datetime import date, datetime
from app import create_app, db
from app.models import WeatherData, WeatherStats
from app.ingestion import WeatherDataIngester
from app.analysis import WeatherStatsCalculator


@pytest.fixture
def app():
    db_fd, db_path = tempfile.mkstemp()
    
    app = create_app()
    app.config['TESTING'] = True
    app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{db_path}'
    app.config['WTF_CSRF_ENABLED'] = False
    
    with app.app_context():
        db.create_all()
        yield app
        db.drop_all()
    
    os.close(db_fd)
    os.unlink(db_path)


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def sample_weather_data():
    return [
        WeatherData(
            station_id='USC00110072',
            date=date(2023, 1, 1),
            max_temp=100,
            min_temp=-50,
            precipitation=25
        ),
        WeatherData(
            station_id='USC00110072',
            date=date(2023, 1, 2),
            max_temp=150,
            min_temp=0,
            precipitation=0
        ),
        WeatherData(
            station_id='USC00110072',
            date=date(2023, 1, 3),
            max_temp=-9999,
            min_temp=-100,
            precipitation=50
        ),
    ]


class TestWeatherDataModel:
    
    def test_weather_data_creation(self, app):
        with app.app_context():
            weather = WeatherData(
                station_id='TEST001',
                date=date(2023, 1, 1),
                max_temp=200,
                min_temp=100,
                precipitation=50
            )
            db.session.add(weather)
            db.session.commit()
            
            assert weather.id is not None
            assert weather.station_id == 'TEST001'
            assert weather.date == date(2023, 1, 1)
    
    def test_temperature_conversion(self, app):
        with app.app_context():
            weather = WeatherData(
                station_id='TEST001',
                date=date(2023, 1, 1),
                max_temp=200,
                min_temp=-100,
                precipitation=150
            )
            
            assert weather.max_temp_celsius == 20.0
            assert weather.min_temp_celsius == -10.0
            assert weather.precipitation_cm == 1.5
    
    def test_missing_data_handling(self, app):
        with app.app_context():
            weather = WeatherData(
                station_id='TEST001',
                date=date(2023, 1, 1),
                max_temp=-9999,
                min_temp=-9999,
                precipitation=-9999
            )
            
            assert weather.max_temp_celsius is None
            assert weather.min_temp_celsius is None
            assert weather.precipitation_cm is None


class TestWeatherStatsModel:
    
    def test_weather_stats_creation(self, app):
        with app.app_context():
            stats = WeatherStats(
                station_id='TEST001',
                year=2023,
                avg_max_temp=15.5,
                avg_min_temp=5.2,
                total_precipitation=125.7
            )
            db.session.add(stats)
            db.session.commit()
            
            assert stats.id is not None
            assert stats.station_id == 'TEST001'
            assert stats.year == 2023
            assert stats.avg_max_temp == 15.5


class TestWeatherDataIngester:
    
    def test_parse_line(self, app):
        with app.app_context():
            ingester = WeatherDataIngester()
            line = "19850101\t-22\t-128\t94"
            
            record = ingester._parse_line(line, 'USC00110072')
            
            assert record.station_id == 'USC00110072'
            assert record.date == date(1985, 1, 1)
            assert record.max_temp == -22
            assert record.min_temp == -128
            assert record.precipitation == 94
    
    def test_parse_line_invalid_format(self, app):
        with app.app_context():
            ingester = WeatherDataIngester()
            line = "19850101\t-22\t-128"
            
            with pytest.raises(ValueError):
                ingester._parse_line(line, 'USC00110072')
    
    def test_duplicate_handling(self, app, sample_weather_data):
        with app.app_context():
            for record in sample_weather_data:
                db.session.add(record)
            db.session.commit()
            
            initial_count = WeatherData.query.count()
            
            ingester = WeatherDataIngester()
            ingester._process_batch_individually(sample_weather_data)
            
            final_count = WeatherData.query.count()
            assert final_count == initial_count


class TestWeatherStatsCalculator:
    
    def test_calculate_station_year_stats(self, app, sample_weather_data):
        with app.app_context():
            for record in sample_weather_data:
                db.session.add(record)
            db.session.commit()
            
            calculator = WeatherStatsCalculator()
            calculator._calculate_station_year_stats('USC00110072', 2023)
            
            stats = WeatherStats.query.filter_by(
                station_id='USC00110072',
                year=2023
            ).first()
            
            assert stats is not None
            assert stats.avg_max_temp == 12.5
            assert stats.avg_min_temp == -5.0
            assert stats.total_precipitation == 0.75


class TestAPI:
    
    def test_weather_data_endpoint(self, client, app, sample_weather_data):
        with app.app_context():
            for record in sample_weather_data:
                db.session.add(record)
            db.session.commit()
            
            response = client.get('/weather')
            assert response.status_code == 200
            
            data = response.get_json()
            assert 'data' in data
            assert 'pagination' in data
            assert len(data['data']) == 3
    
    def test_weather_data_filtering(self, client, app, sample_weather_data):
        with app.app_context():
            for record in sample_weather_data:
                db.session.add(record)
            db.session.commit()
            
            response = client.get('/weather?station_id=USC00110072')
            assert response.status_code == 200
            
            data = response.get_json()
            assert len(data['data']) == 3
            
            for record in data['data']:
                assert record['station_id'] == 'USC00110072'
    
    def test_weather_data_date_filtering(self, client, app, sample_weather_data):
        with app.app_context():
            for record in sample_weather_data:
                db.session.add(record)
            db.session.commit()
            
            response = client.get('/weather?date=2023-01-01')
            assert response.status_code == 200
            
            data = response.get_json()
            assert len(data['data']) == 1
            assert data['data'][0]['date'] == '2023-01-01'
    
    def test_weather_stats_endpoint(self, client, app):
        with app.app_context():
            stats = WeatherStats(
                station_id='USC00110072',
                year=2023,
                avg_max_temp=15.5,
                avg_min_temp=5.2,
                total_precipitation=125.7
            )
            db.session.add(stats)
            db.session.commit()
            
            response = client.get('/weather/stats')
            assert response.status_code == 200
            
            data = response.get_json()
            assert 'data' in data
            assert 'pagination' in data
            assert len(data['data']) == 1
    
    def test_weather_stats_filtering(self, client, app):
        with app.app_context():
            stats1 = WeatherStats(
                station_id='USC00110072',
                year=2022,
                avg_max_temp=15.5,
                avg_min_temp=5.2,
                total_precipitation=125.7
            )
            stats2 = WeatherStats(
                station_id='USC00110072',
                year=2023,
                avg_max_temp=16.5,
                avg_min_temp=6.2,
                total_precipitation=135.7
            )
            db.session.add(stats1)
            db.session.add(stats2)
            db.session.commit()
            
            response = client.get('/weather/stats?year=2023')
            assert response.status_code == 200
            
            data = response.get_json()
            assert len(data['data']) == 1
            assert data['data'][0]['year'] == 2023
    
    def test_pagination(self, client, app):
        with app.app_context():
            for i in range(150):
                weather = WeatherData(
                    station_id=f'TEST{i:03d}',
                    date=date(2023, 1, 1),
                    max_temp=200,
                    min_temp=100,
                    precipitation=50
                )
                db.session.add(weather)
            db.session.commit()
            
            response = client.get('/weather?per_page=50&page=1')
            assert response.status_code == 200
            
            data = response.get_json()
            assert len(data['data']) == 50
            assert data['pagination']['page'] == 1
            assert data['pagination']['total'] == 150
            assert data['pagination']['pages'] == 3
            
            response = client.get('/weather?per_page=50&page=2')
            assert response.status_code == 200
            
            data = response.get_json()
            assert len(data['data']) == 50
            assert data['pagination']['page'] == 2
    
    def test_swagger_documentation(self, client):
        response = client.get('/docs/')
        assert response.status_code == 200
        assert b'swagger' in response.data.lower() or b'openapi' in response.data.lower()


if __name__ == '__main__':
    pytest.main([__file__])
