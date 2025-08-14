import os
import json
import time
import requests
import argparse # Added
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file

# --- Configuration Block ---
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY") # Changed to os.getenv
if not API_KEY:
    raise ValueError("GOOGLE_PLACES_API_KEY environment variable not set.")

# Justification for RADIUS_M: 25-35 km is a good balance to cover most of a city's
# metropolitan area without excessive overlap or too many results per search.
# 30 km is chosen as a mid-point for broad coverage.
RADIUS_M = 30000  # 30 km
MAX_PAGES = 5
LANGUAGE = "fr"
REGION_CODE = "DZ"

# Intermediate JSON artifact path
DISCOVERED_GYMS_JSON = "/Users/rafikben/coding/affiliate/gym-directory-dz/data/discovered_gyms.json"

# Algerian cities with approximate central coordinates
# Coordinates obtained from a quick web search for city centers.
CITIES = [
    {"name": "Alger", "lat": 36.7538, "lng": 3.0588},
    {"name": "Oran", "lat": 35.6911, "lng": -0.6371},
    {"name": "Constantine", "lat": 36.3650, "lng": 6.6147},
    {"name": "Annaba", "lat": 36.9000, "lng": 7.7667},
    {"name": "Blida", "lat": 36.4700, "lng": 2.8267},
    {"name": "Sétif", "lat": 36.1900, "lng": 5.3800},
    {"name": "Batna", "lat": 35.5500, "lng": 6.1800},
    {"name": "Djelfa", "lat": 34.6700, "lng": 3.2600},
    {"name": "Biskra", "lat": 34.8500, "lng": 5.7200},
    {"name": "Tébessa", "lat": 35.4000, "lng": 8.1200},
    {"name": "Tlemcen", "lat": 34.8800, "lng": -1.3100},
    {"name": "Béjaïa", "lat": 36.7500, "lng": 5.0800},
    {"name": "Mostaganem", "lat": 35.9300, "lng": 0.0800},
    {"name": "Sidi Bel Abbès", "lat": 35.2000, "lng": -0.6300},
    {"name": "Skikda", "lat": 36.8800, "lng": 6.9000},
]

def _handle_api_error(response, retries_left):
    if response.status_code == 429 or response.status_code >= 500:
        if retries_left > 0:
            sleep_time = 2 ** (3 - retries_left) # Exponential backoff
            print(f"    API error {response.status_code}. Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
            return True
        else:
            print(f"    API error {response.status_code}. No retries left.")
    else:
        print(f"    API error {response.status_code}: {response.text}")
    return False

def nearby_search(center, radius, page_token=None):
    url = "https://places.googleapis.com/v1/places:searchNearby"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.location"
    }
    data = {
        "includedTypes": ["gym"],
        "locationRestriction": {
            "circle": {
                "center": {
                    "latitude": center["lat"],
                    "longitude": center["lng"]
                },
                "radius": radius
            }
        },
        "languageCode": LANGUAGE,
        "regionCode": REGION_CODE
    }
    if page_token:
        data["pageToken"] = page_token

    retries = 3
    while retries > 0:
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status() # Raise an exception for HTTP errors
            return response.json()
        except requests.exceptions.HTTPError as e:
            if not _handle_api_error(e.response, retries - 1):
                break
        except requests.exceptions.RequestException as e:
            print(f"    Request failed: {e}")
            break
        retries -= 1
    return None

def parse_arguments():
    parser = argparse.ArgumentParser(description="Discover gyms in Algeria using Google Places API (New).")
    parser.add_argument("--test-mode", action="store_true",
                        help="Enable test mode: process the first 2 cities and 2 pages.")
    return parser.parse_args()

def main():
    args = parse_arguments()

    # Make a mutable copy of CITIES for modification
    current_cities = list(CITIES)
    current_max_pages = MAX_PAGES

    if args.test_mode:
        current_cities = current_cities[:2] # First 2 cities
        current_max_pages = 2 # 2 pages
        print("Running in TEST MODE (first 2 cities, 2 pages).")

    all_discovered_gyms = []
    unique_place_ids = set()

    for city in current_cities: # Use current_cities
        print(f"Processing city: {city['name']}")
        page_count = 0
        next_page_token = None

        while page_count < current_max_pages: # Use current_max_pages
            response = nearby_search(city, RADIUS_M, next_page_token)

            if response and "places" in response:
                for place in response["places"]:
                    place_id = place["id"]
                    if place_id not in unique_place_ids:
                        unique_place_ids.add(place_id)
                        all_discovered_gyms.append({
                            "place_id": place_id,
                            "name": place.get("displayName", {}).get("text"),
                            "lat": place.get("location", {}).get("latitude"),
                            "lng": place.get("location", {}).get("longitude"),
                            "city": city["name"]
                        })
                print(f"    Found {len(response['places'])} places on page {page_count + 1}. Total unique gyms: {len(unique_place_ids)}")

            next_page_token = response.get("nextPageToken") if response else None
            page_count += 1

            if not next_page_token:
                print("    No more pages for this city.")
                break
            else:
                print("    Next page token found. Waiting 1-2 seconds before next request...")
                time.sleep(1.5) # Sleep between paginated requests

    # Ensure the data directory exists
    os.makedirs(os.path.dirname(DISCOVERED_GYMS_JSON), exist_ok=True)

    with open(DISCOVERED_GYMS_JSON, "w", encoding="utf-8") as f:
        json.dump(all_discovered_gyms, f, ensure_ascii=False, indent=4)
    print(f"\nDiscovery complete. Saved {len(all_discovered_gyms)} unique gyms to {DISCOVERED_GYMS_JSON}")

    print("\n--- How to run ---")
    print("1. Create a virtual environment: `python3 -m venv venv`")
    print("2. Activate the virtual environment: `source venv/bin/activate` (Linux/macOS) or `.\venv\Scripts\activate` (Windows)")
    print("3. Install dependencies: `pip install requests python-dotenv`")
    print("4. Set your Google Places API Key: `export GOOGLE_PLACES_API_KEY='YOUR_API_KEY'` or create a .env file.")
    print("5. Run the script: `python3 scripts/dz_gym_discovery.py`")
    print("\n--- Cost & Quotas ---")
    print("This script primarily uses Nearby Search (Basic Data).")
    print("Cost is per request. Each page is a new request.")
    print("Total requests = sum(pages_per_city) for all cities.")
    print("Knobs to control spend:")
    print(" - RADIUS_M: Smaller radius means fewer results per search, potentially more searches to cover an area.")
    print(" - MAX_PAGES: Limiting pages directly limits requests.")
    print(" - CITIES: Reduce the number of cities to search.")
    print("\n--- Compliance Checklist ---")
    print(" - No HTML scraping: All data fetched via official Google Places API.")
    print(" - Attribution: If this data is displayed with a map, it must be on a Google map with proper Google attribution.")

if __name__ == "__main__":
    main()
