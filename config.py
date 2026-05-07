import os
import secrets
from datetime import timedelta

_secret = os.environ.get('SECRET_KEY')
if not _secret:
    _secret = secrets.token_hex(32)
    print(f"[SECURITY] WARNING: SECRET_KEY non définie dans l'environnement — clé aléatoire générée (les sessions seront invalidées au redémarrage). Définissez SECRET_KEY dans .env pour la production.")

class Config:
    SECRET_KEY = _secret
    DATABASE_URL = os.environ.get('DATABASE_URL')
    DATABASE = os.path.join(os.path.dirname(__file__), 'database.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # ── Session cookies ──────────────────────────────────────────────────────
    PERMANENT_SESSION_LIFETIME = timedelta(days=7)
    SESSION_COOKIE_HTTPONLY    = True
    SESSION_COOKIE_SAMESITE    = 'Lax'
    # SESSION_COOKIE_SECURE = True  # activer en production HTTPS uniquement

    # ── CSRF ─────────────────────────────────────────────────────────────────
    WTF_CSRF_ENABLED     = True
    WTF_CSRF_TIME_LIMIT  = 3600
