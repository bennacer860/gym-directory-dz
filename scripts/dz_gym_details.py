import os
import json
import sqlite3
import time
import logging
from datetime import datetime, timedelta, timezone
import requests
from dateutil.parser import isoparse
from dotenv import load_dotenv
import csv # Added for CSV export

load_dotenv()

# --- CONFIGURATION ---
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
if not API_KEY:
    raise ValueError("GOOGLE_PLACES_API_KEY environment variable not set.")

# Justification for RADIUS_M: 30km is a reasonable radius for a large city to cover most of its suburbs.
# Note: RADIUS_M and MAX_PAGES are primarily for discovery, but kept here for consistency if needed.
RADIUS_M = 30000
MAX_PAGES = 5
LANGUAGE = "fr"
REGION_CODE = "DZ"
REFRESH_DAYS = 30
MAX_REVIEWS_PER_PLACE = 100
DB_PATH = "data/places_cache.db"
CSV_PATH = "data/gyms_dz.csv"
JSONL_PATH = "data/gyms_dz.jsonl"
DISCOVERED_GYMS_JSON = "data/discovered_gyms.json" # Path to discovered place_ids
LOG_FILE = "logs/details_scraper.log" # Changed log file name

# Algerian cities with approximate central coordinates (used for get_city_from_address)
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
        # Attempt to get the city from the second to last part, common for addresses
        return parts[-2].strip()
    return parts[0].strip() # Fallback to the first part

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
                logging.warning(f"    Retrying due to {e.response.status_code} error for {place_id}. Attempt {attempt + 1}...")
                time.sleep(2 ** attempt)
                continue
            logging.error(f"    HTTP Error {e.response.status_code} for {place_id}: {e.response.text}")
            break
        except requests.exceptions.RequestException as e:
            logging.error(f"    Request Exception for {place_id}: {e}")
            break
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
        p.get("city"), # 'city' field is added during normalization
        p.get("location", {}).get("latitude"),
        p.get("location", {}).get("longitude"),
        p.get("internationalPhoneNumber"),
        p.get("websiteUri"),
        p.get("rating"),
        p.get("userRatingCount"),
        hours,
        # p.get("photo_reference"), # Removed as photos are not fetched by this script
        # p.get("map_url"), # Removed as map_url is not generated by this script
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

    # Extract photo reference
    if p.get("photos") and isinstance(p["photos"], list) and len(p["photos"]) > 0:
        p["photo_reference"] = p["photos"][0].get("name")

    # Remove photos field as it's large and not needed after extracting the reference
    p.pop("photos", None)

    return p

def export_csv(rows):
    """Exports the data to a CSV file."""
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        # Quote all non-numeric fields so text columns (like city) are always quoted
        writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow([
            "place_id", "name", "address", "city", "lat", "lng", "phone", "website",
            "rating", "reviews_count", "hours"
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
    if not API_KEY:
        logging.error("Error: GOOGLE_PLACES_API_KEY environment variable not set.")
        return

    ensure_db()

    all_discovered_gyms = []
    try:
        with open(DISCOVERED_GYMS_JSON, "r", encoding="utf-8") as f:
            all_discovered_gyms = json.load(f)
    except FileNotFoundError:
        logging.error(f"Error: {DISCOVERED_GYMS_JSON} not found. Please run dz_gym_discovery.py first.")
        return
    except json.JSONDecodeError:
        logging.error(f"Error: Could not decode JSON from {DISCOVERED_GYMS_JSON}. File might be empty or corrupted.")
        return

    # Extract unique place_ids and their associated city from the discovery output
    unique_place_ids = {} # {place_id: city_name}
    for gym in all_discovered_gyms:
        if gym.get("place_id"):
            unique_place_ids[gym["place_id"]] = gym.get("city", "Unknown") # Assuming city might be in discovery output

    place_ids_to_process = list(unique_place_ids.keys())

    if test_mode:
        logging.info("--- RUNNING IN TEST MODE (Details) ---")
        place_ids_to_process = place_ids_to_process[:5] # Process only first 5 for test mode

    logging.info(f"\nProcessing {len(place_ids_to_process)} unique place IDs for details.\n")

    csv_rows = []
    jsonl_payloads = []
    cache_hits = 0
    details_fetched_count = 0

    for i, place_id in enumerate(place_ids_to_process):
        source_city = unique_place_ids.get(place_id, "Unknown") # Get city from discovery output
        cached_data, fetched_at = cache_get(place_id)

        normalized_place = None

        if cached_data and not is_stale(fetched_at):
            place_details = cached_data
            cache_hits += 1
            log_name = place_details.get("displayName", {}).get("text", "N/A")
            logging.info(f"CACHE HIT ({i+1}/{len(place_ids_to_process)}): Using cached data for {place_id} ({log_name}, {source_city})")
            normalized_place = normalize_place(place_details.copy())
        else:
            if cached_data:
                log_name = cached_data.get("displayName", {}).get("text", "N/A")
                logging.info(f"CACHE STALE ({i+1}/{len(place_ids_to_process)}): Fetching new data for {place_id} ({log_name}, {source_city})")
            else:
                logging.info(f"CACHE MISS ({i+1}/{len(place_ids_to_process)}): Fetching new data for {place_id}")

            place_details = get_details(place_id)
            if place_details:
                normalized_place = normalize_place(place_details.copy())
                # Ensure city in outputs matches the search context from discovery
                normalized_place["city"] = source_city
                cache_put(place_id, normalized_place)
                details_fetched_count += 1
                log_name = normalized_place.get("displayName", {}).get("text", "N/A")
                logging.info(f"SUCCESS ({i+1}/{len(place_ids_to_process)}): Fetched and cached data for {place_id} ({log_name}, {source_city})")
            else:
                logging.error(f"FAILURE ({i+1}/{len(place_ids_to_process)}): Could not fetch data for {place_id}")
            time.sleep(0.1)  # Small delay to be nice to the API

        if normalized_place:
            csv_rows.append(flatten_record(normalized_place))
            jsonl_payloads.append(normalized_place)

        if (i + 1) % 100 == 0:
            logging.info(f"Progress: Processed {i + 1} places. Cache hits: {cache_hits}, New fetches: {details_fetched_count}")


    logging.info(f"\n--- Exporting Data ---")
    logging.info(f"  Total unique place IDs processed: {len(place_ids_to_process)}")
    logging.info(f"  Cache hits: {cache_hits}")
    logging.info(f"  New details fetched: {details_fetched_count}")

    export_csv(csv_rows)
    export_jsonl(jsonl_payloads)

    logging.info(f"\nSuccessfully exported data to {CSV_PATH} and {JSONL_PATH}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Fetch and process gym details from Google Places API.')
    parser.add_argument('--test-mode', action='store_true', help='Run in test mode with limited data.')
    args = parser.parse_args()
    main(test_mode=args.test_mode)