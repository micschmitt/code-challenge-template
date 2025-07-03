from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_restx import Api
import os
import logging

db = SQLAlchemy()

def create_app(config_name='default'):
    app = Flask(__name__)
    
    app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///weather_data.db')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key')
    
    db.init_app(app)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    api = Api(
        app,
        version='1.0',
        title='Weather Data API',
        description='REST API for weather data management',
        doc='/docs/'
    )
    
    from app.api import weather_ns, stats_ns
    api.add_namespace(weather_ns)
    api.add_namespace(stats_ns)
    
    with app.app_context():
        db.create_all()
    
    return app
