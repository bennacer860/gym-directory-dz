# config.py

import os
from dotenv import load_dotenv

load_dotenv()

# --- Google API ---
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

# --- Search Parameters ---
LANGUAGE = "fr"
REGION_CODE = "DZ"
RADIUS_M = 30000  # 30km radius
MAX_PAGES = 5
MAX_REVIEWS_PER_PLACE = 100

# --- Caching ---
REFRESH_DAYS = 30

# --- Project Paths ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "places_pipeline.db")
CSV_OUTPUT_PATH = os.path.join(DATA_DIR, "gyms_dz_pipeline.csv")
JSONL_OUTPUT_PATH = os.path.join(DATA_DIR, "gyms_dz_pipeline.jsonl")

# --- Celery & Broker ---
CELERY_BROKER_URL = "redis://localhost:6379/0"
CELERY_RESULT_BACKEND = "redis://localhost:6379/0"

# --- Ollama ---
OLLAMA_API_URL = "http://localhost:11434/api/generate"

# --- Cities ---
CITIES = [
    {"name": "Alger", "lat": 36.7753, "lng": 3.0602},
    {"name": "Oran", "lat": 35.6911, "lng": -0.6417},
    {"name": "Constantine", "lat": 36.365, "lng": 6.6147},
    {"name": "Annaba", "lat": 36.9, "lng": 7.7667},
    {"name": "Blida", "lat": 36.4703, "lng": 2.8289},
    {"name": "Sétif", "lat": 36.19, "lng": 5.41},
    {"name": "Batna", "lat": 35.5558, "lng": 6.1769},
    {"name": "Djelfa", "lat": 34.6728, "lng": 3.2639},
    {"name": "Biskra", "lat": 34.85, "lng": 5.7333},
    {"name": "Tébessa", "lat": 35.4042, "lng": 8.1222},
    {"name": "Tlemcen", "lat": 34.8828, "lng": -1.3111},
    {"name": "Béjaïa", "lat": 36.75, "lng": 5.0667},
    {"name": "Mostaganem", "lat": 35.9333, "lng": 0.0833},
    {"name": "Sidi Bel Abbès", "lat": 35.1897, "lng": -0.6308},
    {"name": "Skikda", "lat": 36.8667, "lng": 6.9},
]
