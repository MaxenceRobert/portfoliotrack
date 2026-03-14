import os

class Config:
    # Clé secrète pour sécuriser les sessions et les cookies
    # os.urandom génère une chaîne aléatoire — change-la si tu mets en prod
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    
    # Chemin vers la base de données SQLite
    DATABASE = os.path.join(os.path.dirname(__file__), 'database.db')
    
    # Désactive le tracking des modifications SQLAlchemy (on n'en a pas besoin)
    SQLALCHEMY_TRACK_MODIFICATIONS = False