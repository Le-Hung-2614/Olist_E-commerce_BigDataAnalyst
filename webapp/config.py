"""
MongoDB connection configuration for Olist E-Commerce Dashboard.
"""
import os

class Config:
    MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
    DATABASE_NAME = os.environ.get("MONGO_DB", "olist_dw")
    SECRET_KEY = os.environ.get("SECRET_KEY", "olist-dashboard-secret-key-2026")
    DEBUG = os.environ.get("FLASK_DEBUG", "True").lower() in ("true", "1", "yes")
