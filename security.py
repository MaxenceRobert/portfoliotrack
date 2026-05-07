"""
Extension instances créées hors contexte d'application pour éviter les imports circulaires.
Initialisées dans app.py via init_app().
"""
import os
import secrets
import logging

from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_caching import Cache

log = logging.getLogger("security")

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[],
    storage_uri="memory://",
)

cache = Cache()
