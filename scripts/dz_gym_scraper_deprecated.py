"""
This descript is depracted in favor of 2 scripts: 
- discover 
- details
"""

import os
import json
import sqlite3
import time
import logging
from datetime import datetime, timedelta, timezone
import requests
from dateutil.parser import isoparse
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
CITIES = [
    {"name": "Alger", "lat": 36.7753, "lng": 3.0603},
    {"name": "Oran", "lat": 35.6911, "lng": -0.6417},
    {"name": "Constantine", "lat": 36.365, "lng": 6.6147},
    {"name": "Annaba", "lat": 36.9, "lng": 7.7667},
    {"name": "Blida", "lat": 36.4703, "lng": 2.8289},
    {"name": "Sétif", "lat": 36.19, "lng": 5.41},
    {"name": "Batna", "lat": 35.555, "lng": 6.1742},
    {"name": "Djelfa", "lat": 34.6667, "lng": 3.25},
    {"name": "Biskra", "lat": 34.85, "lng": 5.7333},
    {"name": "Tébessa", "lat": 35.4042, "lng": 8.1222},
    {"name": "Tlemcen", "lat": 34.8828, "lng": -1.3111},
    {"name": "Béjaïa", "lat": 36.75, "lng": 5.0667},
    {"name": "Mostaganem", "lat": 35.9333, "lng": 0.0833},
    {"name": "Sidi Bel Abbès", "lat": 35.1897, "lng": -0.6308},
    {"name": "Skikda", "lat": 36.8667, "lng": 6.9},
]
RADIUS_M = 30000  # Justification: 30km is a reasonable radius for a large city to cover most of its suburbs.
MAX_PAGES = 5
LANGUAGE = "fr"
REGION_CODE = "DZ"
REFRESH_DAYS = 30
MAX_REVIEWS_PER_PLACE = 100
DB_PATH = "data/places_cache.db"
CSV_PATH = "data/gyms_dz.csv"
JSONL_PATH = "data/gyms_dz.jsonl"
LOG_FILE = "logs/scraper.log"

# --- LOGGING SETUP ---
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Console handler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

# File handler
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

# --- ADDRESS/CITY HELPERS ---
def get_city_from_address(address):
    if not address:
        return "Unknown"
    for city in CITIES:
        if city['name'].lower() in address.lower():
            return city['name']
    parts = address.split(',')
    if len(parts) > 1:
        return parts[-2].strip()
    return parts[0].strip()

# --- SQLITE CACHE HELPERS ---

def ensure_db():
    """Creates the SQLite database and table if they don't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS place_cache (
                place_id TEXT PRIMARY KEY,
                payload_json TEXT,
                fetched_at TIMESTAMP
            )
        ''')
        conn.commit()

def cache_get(place_id):
    """Gets a place's data from the cache."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT payload_json, fetched_at FROM place_cache WHERE place_id = ?", (place_id,))
        row = cursor.fetchone()
        if row:
            return json.loads(row[0]), isoparse(row[1])
        return None, None

def cache_put(place_id, payload):
    """Puts a place's data into the cache."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO place_cache (place_id, payload_json, fetched_at) VALUES (?, ?, ?)",
            (place_id, json.dumps(payload), datetime.now(timezone.utc).isoformat())
        )
        conn.commit()

def is_stale(fetched_at):
    """Checks if a cached item is stale."""
    if not fetched_at:
        return True
    # Ensure timezone-aware comparison
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - fetched_at) > timedelta(days=REFRESH_DAYS)

# --- GOOGLE PLACES API WRAPPERS ---

def nearby_search(center, radius, page_token=None):
    """Performs a Nearby Search request."""
    url = "https://places.googleapis.com/v1/places:searchNearby"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": "places.id",
    }
    data = {
        "includedTypes": ["gym"],
        "maxResultCount": 20,
        "locationRestriction": {
            "circle": {
                "center": {"latitude": center["lat"], "longitude": center["lng"]},
                "radius": radius,
            }
        },
        "languageCode": LANGUAGE,
        "regionCode": REGION_CODE,
    }
    if page_token:
        data["pageToken"] = page_token

    for attempt in range(5):
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in (429, 500, 503, 504):
                logging.warning(f"    Retrying due to {e.response.status_code} error...")
                time.sleep(2 ** attempt)
                continue
            raise e
    return None

def get_details(place_id):
    """Fetches Place Details for a given place_id."""
    url = f"https://places.googleapis.com/v1/places/{place_id}"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": "id,displayName,formattedAddress,location,internationalPhoneNumber,websiteUri,regularOpeningHours,rating,userRatingCount,reviews,photos",
    }
    params = {
        "languageCode": LANGUAGE,
        "regionCode": REGION_CODE,
    }

    for attempt in range(5):
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in (429, 500, 503, 504):
                logging.warning(f"    Retrying due to {e.response.status_code} error...")
                time.sleep(2 ** attempt)
                continue
            raise e
    return None

# --- DATA NORMALIZATION AND EXPORT ---

def flatten_record(p):
    """Flattens a place's data for CSV export."""
    hours = ""
    if p.get("regularOpeningHours", {}).get("weekdayDescriptions"):
        hours = "; ".join(p["regularOpeningHours"]["weekdayDescriptions"])

    return [
        p.get("id"),
        p.get("displayName", {}).get("text"),
        p.get("formattedAddress"),
        p.get("city"),
        p.get("location", {}).get("latitude"),
        p.get("location", {}).get("longitude"),
        p.get("internationalPhoneNumber"),
        p.get("websiteUri"),
        p.get("rating"),
        p.get("userRatingCount"),
        hours,
        p.get("photo_reference"),
        p.get("map_url"),
    ]

def normalize_place(p):
    """Normalizes the place data for JSONL export.

    Idempotent: safe to call on raw Google payloads or already-normalized records.
    """
    # City: keep existing if present; otherwise derive from address
    p["city"] = p.get("city") or get_city_from_address(p.get("formattedAddress"))

    # Reviews: handle both raw Google shape and already-normalized shape
    normalized_reviews = []
    for review in (p.get("reviews") or [])[:MAX_REVIEWS_PER_PLACE]:
        if not isinstance(review, dict):
            continue
        # Raw Google Places review
        if (
            "authorAttribution" in review
            or "relativePublishTimeDescription" in review
            or isinstance(review.get("text"), dict)
        ):
            author_name = (review.get("authorAttribution") or {}).get("displayName")
            rating = review.get("rating")
            relative = review.get("relativePublishTimeDescription")
            lang = review.get("originalLanguageCode")
            text_field = review.get("text")
            text = (text_field or {}).get("text") if isinstance(text_field, dict) else text_field
        else:
            # Already-normalized review
            author_name = review.get("author_name")
            rating = review.get("rating")
            relative = review.get("relative_time_description")
            lang = review.get("original_language")
            text = review.get("text")
        normalized_reviews.append({
            "author_name": author_name,
            "rating": rating,
            "relative_time_description": relative,
            "original_language": lang,
            "text": text,
        })
    p["reviews"] = normalized_reviews

    # Photo reference: preserve if already set; otherwise take from first photo
    if not p.get("photo_reference"):
        if p.get("photos") and len(p["photos"]) > 0:
            p["photo_reference"] = p["photos"][0].get("name")
        else:
            p["photo_reference"] = None

    # Map URL: preserve if already set; otherwise compute
    if not p.get("map_url"):
        if p.get("location"):
            lat = p["location"].get("latitude")
            lng = p["location"].get("longitude")
            if lat is not None and lng is not None:
                p["map_url"] = f"https://www.google.com/maps/search/?api=1&query={lat},{lng}&query_place_id={p['id']}"
        else:
            p["map_url"] = None

    return p

def export_csv(rows):
    """Exports the data to a CSV file."""
    import csv
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        # Quote all non-numeric fields so text columns (like city) are always quoted
        writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow([
            "place_id", "name", "address", "city", "lat", "lng", "phone", "website",
            "rating", "reviews_count", "hours", "photo_reference", "map_url"
        ])
        writer.writerows(rows)

def export_jsonl(payloads):
    """Exports the data to a JSONL file."""
    os.makedirs(os.path.dirname(JSONL_PATH), exist_ok=True)
    with open(JSONL_PATH, "w", encoding="utf-8") as f:
        for payload in payloads:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

import argparse

# --- MAIN WORKFLOW ---

def main(test_mode=False):
    """Main function to run the data collection workflow."""
    if test_mode:
        logging.info("--- RUNNING IN TEST MODE ---")
        CITIES_TO_SEARCH = CITIES[:3]
        RADIUS_TO_SEARCH = 5000
        MAX_PAGES_TO_SEARCH = 1
    else:
        CITIES_TO_SEARCH = CITIES
        RADIUS_TO_SEARCH = RADIUS_M
        MAX_PAGES_TO_SEARCH = MAX_PAGES

    if not API_KEY:
        logging.error("Error: GOOGLE_PLACES_API_KEY environment variable not set.")
        return

    ensure_db()

    all_place_ids = set()
    page_count = 0
    # Map each place_id to the search city it was found under
    place_city_map = {}

    for city in CITIES_TO_SEARCH:
        logging.info(f"--- Searching in {city['name']} ---")
        next_page_token = None
        for page_num in range(MAX_PAGES_TO_SEARCH):
            logging.info(f"  Page {page_num + 1}...")
            results = nearby_search(city, RADIUS_TO_SEARCH, next_page_token)
            page_count += 1

            if results and "places" in results:
                for place in results["places"]:
                    pid = place["id"]
                    all_place_ids.add(pid)
                    # Keep the first associated city for this place_id
                    place_city_map.setdefault(pid, city["name"])

            next_page_token = results.get("nextPageToken")
            if not next_page_token:
                break
            time.sleep(2)

    logging.info(f"\nFound {len(all_place_ids)} unique places across {page_count} pages.\n")

    csv_rows = []
    jsonl_payloads = []
    cache_hits = 0
    details_count = 0

    for i, place_id in enumerate(all_place_ids):
        source_city = place_city_map.get(place_id, "Unknown")
        cached_data, fetched_at = cache_get(place_id)

        normalized_place = None

        if cached_data and not is_stale(fetched_at):
            place_details = cached_data
            cache_hits += 1
            log_name = place_details.get("displayName", {}).get("text", "N/A")
            logging.info(f"CACHE HIT: Using cached data for {place_id} ({log_name}, {source_city})")
            # Normalize for export; do not trust cache format blindly
            normalized_place = normalize_place(place_details.copy())
        else:
            if cached_data:
                log_name = cached_data.get("displayName", {}).get("text", "N/A")
                logging.info(f"CACHE STALE: Fetching new data for {place_id} ({log_name}, {source_city})")
            else:
                logging.info(f"CACHE MISS: Fetching new data for {place_id}")
            place_details = get_details(place_id)
            if place_details:
                normalized_place = normalize_place(place_details.copy())
                # Keep cache consistent with outputs
                normalized_place["city"] = source_city
                cache_put(place_id, normalized_place)
                log_name = normalized_place.get("displayName", {}).get("text", "N/A")
                logging.info(f"SUCCESS: Fetched and cached data for {place_id} ({log_name}, {source_city})")
            else:
                logging.error(f"FAILURE: Could not fetch data for {place_id}")
            time.sleep(0.1)  # Small delay to be nice to the API

        if normalized_place:
            details_count += 1
            # Ensure city in exports matches the search context
            normalized_place["city"] = source_city
            csv_rows.append(flatten_record(normalized_place))
            jsonl_payloads.append(normalized_place)

    logging.info(f"\n--- Exporting Data ---")
    logging.info(f"  Total details fetched: {details_count}")
    logging.info(f"  Cache hits: {cache_hits}")

    export_csv(csv_rows)
    export_jsonl(jsonl_payloads)

    logging.info(f"\nSuccessfully exported data to {CSV_PATH} and {JSONL_PATH}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Scrape gym data from Google Places API.')
    parser.add_argument('--test-mode', action='store_true', help='Run in test mode with limited data.')
    args = parser.parse_args()
    main(test_mode=args.test_mode)
