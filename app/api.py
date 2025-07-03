from flask import request
from flask_restx import Namespace, Resource, fields
from app import db
from app.models import WeatherData, WeatherStats
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

weather_ns = Namespace('weather', description='Weather data operations', path='/weather')
stats_ns = Namespace('weather/stats', description='Weather statistics operations', path='/weather/stats')

weather_model = weather_ns.model('WeatherData', {
    'id': fields.Integer(description='Record ID'),
    'station_id': fields.String(description='Weather station identifier'),
    'date': fields.Date(description='Date of the weather record'),
    'max_temp_celsius': fields.Float(description='Maximum temperature in Celsius'),
    'min_temp_celsius': fields.Float(description='Minimum temperature in Celsius'),
    'precipitation_cm': fields.Float(description='Precipitation in centimeters'),
})

stats_model = stats_ns.model('WeatherStats', {
    'id': fields.Integer(description='Record ID'),
    'station_id': fields.String(description='Weather station identifier'),
    'year': fields.Integer(description='Year'),
    'avg_max_temp': fields.Float(description='Average maximum temperature in Celsius'),
    'avg_min_temp': fields.Float(description='Average minimum temperature in Celsius'),
    'total_precipitation': fields.Float(description='Total precipitation in centimeters'),
})

pagination_model = weather_ns.model('Pagination', {
    'page': fields.Integer(description='Current page number'),
    'per_page': fields.Integer(description='Number of items per page'),
    'total': fields.Integer(description='Total number of items'),
    'pages': fields.Integer(description='Total number of pages'),
})

weather_response_model = weather_ns.model('WeatherResponse', {
    'data': fields.List(fields.Nested(weather_model)),
    'pagination': fields.Nested(pagination_model),
})

stats_response_model = stats_ns.model('StatsResponse', {
    'data': fields.List(fields.Nested(stats_model)),
    'pagination': fields.Nested(pagination_model),
})


def parse_date(date_string):
    if not date_string:
        return None
    
    formats = ['%Y-%m-%d', '%Y%m%d', '%Y/%m/%d']
    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt).date()
        except ValueError:
            continue
    
    raise ValueError(f"Invalid date format: {date_string}")


@weather_ns.route('')
class WeatherDataList(Resource):
    @weather_ns.doc('list_weather_data')
    @weather_ns.expect(weather_ns.parser()
                      .add_argument('station_id', type=str, help='Filter by station ID')
                      .add_argument('date', type=str, help='Filter by date (YYYY-MM-DD)')
                      .add_argument('start_date', type=str, help='Filter by start date (YYYY-MM-DD)')
                      .add_argument('end_date', type=str, help='Filter by end date (YYYY-MM-DD)')
                      .add_argument('page', type=int, default=1, help='Page number')
                      .add_argument('per_page', type=int, default=100, help='Items per page'))
    @weather_ns.marshal_with(weather_response_model)
    def get(self):
        """Fetch weather data with optional filtering and pagination."""
        station_id = request.args.get('station_id')
        date_str = request.args.get('date')
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        page = int(request.args.get('page', 1))
        per_page = min(int(request.args.get('per_page', 100)), 1000)
        
        query = WeatherData.query
        
        if station_id:
            query = query.filter(WeatherData.station_id == station_id)
        
        if date_str:
            try:
                date_obj = parse_date(date_str)
                query = query.filter(WeatherData.date == date_obj)
            except ValueError as e:
                weather_ns.abort(400, str(e))
        
        if start_date_str:
            try:
                start_date = parse_date(start_date_str)
                query = query.filter(WeatherData.date >= start_date)
            except ValueError as e:
                weather_ns.abort(400, f"Invalid start_date: {str(e)}")
        
        if end_date_str:
            try:
                end_date = parse_date(end_date_str)
                query = query.filter(WeatherData.date <= end_date)
            except ValueError as e:
                weather_ns.abort(400, f"Invalid end_date: {str(e)}")
        
        query = query.order_by(WeatherData.date.desc(), WeatherData.station_id)
        
        try:
            pagination = query.paginate(
                page=page,
                per_page=per_page,
                error_out=False
            )
        except Exception as e:
            logger.error(f"Pagination error: {str(e)}")
            weather_ns.abort(500, "Error retrieving data")
        
        data = []
        for record in pagination.items:
            data.append({
                'id': record.id,
                'station_id': record.station_id,
                'date': record.date,
                'max_temp_celsius': record.max_temp_celsius,
                'min_temp_celsius': record.min_temp_celsius,
                'precipitation_cm': record.precipitation_cm,
            })
        
        return {
            'data': data,
            'pagination': {
                'page': pagination.page,
                'per_page': pagination.per_page,
                'total': pagination.total,
                'pages': pagination.pages,
            }
        }


@stats_ns.route('')
class WeatherStatsList(Resource):
    @stats_ns.doc('list_weather_stats')
    @stats_ns.expect(stats_ns.parser()
                    .add_argument('station_id', type=str, help='Filter by station ID')
                    .add_argument('year', type=int, help='Filter by year')
                    .add_argument('start_year', type=int, help='Filter by start year')
                    .add_argument('end_year', type=int, help='Filter by end year')
                    .add_argument('page', type=int, default=1, help='Page number')
                    .add_argument('per_page', type=int, default=100, help='Items per page'))
    @stats_ns.marshal_with(stats_response_model)
    def get(self):
        """Fetch weather statistics with optional filtering and pagination."""
        station_id = request.args.get('station_id')
        year = request.args.get('year', type=int)
        start_year = request.args.get('start_year', type=int)
        end_year = request.args.get('end_year', type=int)
        page = int(request.args.get('page', 1))
        per_page = min(int(request.args.get('per_page', 100)), 1000)
        
        query = WeatherStats.query
        
        if station_id:
            query = query.filter(WeatherStats.station_id == station_id)
        
        if year:
            query = query.filter(WeatherStats.year == year)
        
        if start_year:
            query = query.filter(WeatherStats.year >= start_year)
        
        if end_year:
            query = query.filter(WeatherStats.year <= end_year)
        
        query = query.order_by(WeatherStats.year.desc(), WeatherStats.station_id)
        
        try:
            pagination = query.paginate(
                page=page,
                per_page=per_page,
                error_out=False
            )
        except Exception as e:
            logger.error(f"Pagination error: {str(e)}")
            stats_ns.abort(500, "Error retrieving data")
        
        data = []
        for record in pagination.items:
            data.append({
                'id': record.id,
                'station_id': record.station_id,
                'year': record.year,
                'avg_max_temp': record.avg_max_temp,
                'avg_min_temp': record.avg_min_temp,
                'total_precipitation': record.total_precipitation,
            })
        
        return {
            'data': data,
            'pagination': {
                'page': pagination.page,
                'per_page': pagination.per_page,
                'total': pagination.total,
                'pages': pagination.pages,
            }
        }
