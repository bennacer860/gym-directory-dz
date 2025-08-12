import os
import json
import sqlite3
import time
from datetime import datetime, timedelta
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
DB_PATH = "places_cache.db"
CSV_PATH = "gyms_dz.csv"
JSONL_PATH = "gyms_dz.jsonl"

# --- SQLITE CACHE HELPERS ---

def ensure_db():
    """Creates the SQLite database and table if they don't exist."""
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
            (place_id, json.dumps(payload), datetime.utcnow().isoformat())
        )
        conn.commit()

def is_stale(fetched_at):
    """Checks if a cached item is stale."""
    if not fetched_at:
        return True
    return (datetime.utcnow() - fetched_at) > timedelta(days=REFRESH_DAYS)

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
                print(f"    Retrying due to {e.response.status_code} error...")
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
        "X-Goog-FieldMask": "id,displayName,formattedAddress,location,internationalPhoneNumber,websiteUri,regularOpeningHours,rating,userRatingCount,reviews",
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
                print(f"    Retrying due to {e.response.status_code} error...")
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
        p.get("location", {}).get("latitude"),
        p.get("location", {}).get("longitude"),
        p.get("internationalPhoneNumber"),
        p.get("websiteUri"),
        p.get("rating"),
        p.get("userRatingCount"),
        hours,
    ]

def normalize_reviews(p):
    """Normalizes the reviews for JSONL export."""
    reviews = []
    if p.get("reviews"):
        for review in p["reviews"][:MAX_REVIEWS_PER_PLACE]:
            reviews.append({
                "author_name": review.get("authorAttribution", {}).get("displayName"),
                "rating": review.get("rating"),
                "relative_time_description": review.get("relativePublishTimeDescription"),
                "original_language": review.get("originalLanguageCode"),
                "text": review.get("text", {}).get("text"),
            })
    p["reviews"] = reviews
    return p

def export_csv(rows):
    """Exports the data to a CSV file."""
    import csv
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "place_id", "name", "address", "lat", "lng", "phone", "website",
            "rating", "reviews_count", "hours"
        ])
        writer.writerows(rows)

def export_jsonl(payloads):
    """Exports the data to a JSONL file."""
    with open(JSONL_PATH, "w", encoding="utf-8") as f:
        for payload in payloads:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

import argparse

# --- MAIN WORKFLOW ---

def main(test_mode=False):
    """Main function to run the data collection workflow."""
    if test_mode:
        print("--- RUNNING IN TEST MODE ---")
        CITIES_TO_SEARCH = CITIES[:1]
        RADIUS_TO_SEARCH = 5000
        MAX_PAGES_TO_SEARCH = 2
    else:
        CITIES_TO_SEARCH = CITIES
        RADIUS_TO_SEARCH = RADIUS_M
        MAX_PAGES_TO_SEARCH = MAX_PAGES

    if not API_KEY:
        print("Error: GOOGLE_PLACES_API_KEY environment variable not set.")
        return

    ensure_db()

    all_place_ids = set()
    page_count = 0

    for city in CITIES_TO_SEARCH:
        print(f"--- Searching in {city['name']} ---")
        next_page_token = None
        for page_num in range(MAX_PAGES_TO_SEARCH):
            print(f"  Page {page_num + 1}...")
            results = nearby_search(city, RADIUS_TO_SEARCH, next_page_token)
            page_count += 1

            if results and "places" in results:
                for place in results["places"]:
                    all_place_ids.add(place["id"])

            next_page_token = results.get("nextPageToken")
            if not next_page_token:
                break
            time.sleep(2)

    print(f"\nFound {len(all_place_ids)} unique places across {page_count} pages.\n")

    csv_rows = []
    jsonl_payloads = []
    cache_hits = 0
    details_count = 0

    for i, place_id in enumerate(all_place_ids):
        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{len(all_place_ids)} places...")

        cached_data, fetched_at = cache_get(place_id)

        if cached_data and not is_stale(fetched_at):
            place_details = cached_data
            cache_hits += 1
        else:
            place_details = get_details(place_id)
            if place_details:
                cache_put(place_id, place_details)
            time.sleep(0.1) # Small delay to be nice to the API

        if place_details:
            details_count += 1
            csv_rows.append(flatten_record(place_details))
            jsonl_payloads.append(normalize_reviews(place_details.copy()))

    print(f"\n--- Exporting Data ---")
    print(f"  Total details fetched: {details_count}")
    print(f"  Cache hits: {cache_hits}")

    export_csv(csv_rows)
    export_jsonl(jsonl_payloads)

    print(f"\nSuccessfully exported data to {CSV_PATH} and {JSONL_PATH}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Scrape gym data from Google Places API.')
    parser.add_argument('--test-mode', action='store_true', help='Run in test mode with limited data.')
    args = parser.parse_args()
    main(test_mode=args.test_mode)