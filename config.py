import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    DATABASE_URL = os.environ.get('DATABASE_URL')
    DATABASE = os.path.join(os.path.dirname(__file__), 'database.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False