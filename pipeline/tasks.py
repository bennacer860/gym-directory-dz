# pipeline/tasks.py

import logging
import re
from celery import group
from celery_app import app
from pipeline.models import get_db_connection
import config
import requests
import time
import json
from datetime import datetime, timedelta, timezone
import csv
import os
import sqlite3 # Import sqlite3 for Row factory
import traceback # Import traceback for detailed error reporting

logger = logging.getLogger(__name__)

# --- Helper Functions ---

def get_city_config(city_name):
    for city in config.CITIES:
        if city['name'] == city_name:
            return city
    return None

def update_place_status(place_id, status):
    conn = get_db_connection()
    try:
        with conn:
            conn.execute(
                "UPDATE places SET status = ?, updated_at = ? WHERE place_id = ?",
                (status, datetime.now(timezone.utc).isoformat(), place_id)
            )
        logger.info(f"Updated status for {place_id} to {status}")
    except Exception:
        logger.exception(f"Failed to update status for {place_id}")
    finally:
        conn.close()

def make_api_request(url, method='GET', headers=None, json_payload=None):
    try:
        if method.upper() == 'POST':
            response = requests.post(url, headers=headers, json=json_payload)
        else:
            response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise # Re-raise the exception

def call_ollama_api(prompt, is_json_response=False):
    try:
        payload = {"model": "gpt-oss", "prompt": prompt, "stream": False}
        response = requests.post(config.OLLAMA_API_URL, json=payload)
        response.raise_for_status()
        response_json = response.json()
        response_text = response_json.get("response", "")
        if is_json_response:
            match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
            if match:
                return json.loads(match.group(1))
            else:
                start = response_text.find('{') if '{' in response_text else response_text.find('[')
                if start != -1: return json.loads(response_text[start:])
                return None
        else:
            return re.sub(r'<think>.*?</think>', '', response_text, flags=re.DOTALL).strip()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error calling Ollama API: {e}")
        raise # Re-raise the exception
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from Ollama response: {e}")
        raise # Re-raise the exception
    return None

# --- Celery Tasks ---

@app.task
def start_full_pipeline(test_mode=False, skip_llm=False):
    cities_to_run = config.CITIES
    if test_mode:
        logger.info("--- RUNNING IN TEST MODE ---")
        cities_to_run = [config.CITIES[0]]
    for city_conf in cities_to_run:
        try:
            conn = get_db_connection()
            with conn:
                conn.execute("INSERT OR IGNORE INTO cities (name) VALUES (?)", (city_conf['name'],))
            conn.close()
            discover_places.delay(city_name=city_conf['name'], test_mode=test_mode, skip_llm=skip_llm)
        except Exception as e:
            logger.exception(f"Failed to initialize city {city_conf['name']} in database.")
            raise # Re-raise the exception
    result_str = f"Pipeline started for {len(cities_to_run)} cities."
    logger.info(result_str)
    # Removed automatic export calls
    return result_str

@app.task
def discover_places(city_name, test_mode=False, skip_llm=False):
    city_conf = get_city_config(city_name)
    if not city_conf: raise ValueError(f"Error: City config not found for {city_name}.")
    headers = {
        "Content-Type": "application/json", "X-Goog-Api-Key": config.API_KEY,
        "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.location",
    }
    payload = {
        "includedTypes": ["gym"], "maxResultCount": 20,
        "locationRestriction": {
            "circle": {"center": {"latitude": city_conf['lat'], "longitude": city_conf['lng']}, "radius": config.RADIUS_M,}
        },
        "languageCode": config.LANGUAGE, "regionCode": config.REGION_CODE,
    }
    place_ids, page_count, next_page_token = set(), 0, None
    max_pages = config.MAX_PAGES
    if test_mode:
        logger.info(f"--- {city_name}: TEST MODE, fetching only 1 page --- ")
        max_pages = 1
    try:
        conn = get_db_connection()
        with conn:
            conn.execute("UPDATE cities SET status = 'DISCOVERING', discovered_at = ? WHERE name = ?", (datetime.now(timezone.utc), city_name))
        conn.close()
        while page_count < max_pages:
            page_count += 1
            if next_page_token: payload['pageToken'] = next_page_token
            logger.info(f"City: {city_name} - Requesting page {page_count}...")
            data = make_api_request("https://places.googleapis.com/v1/places:searchNearby", method='POST', headers=headers, json_payload=payload)
            if data and 'places' in data:
                for p in data['places']:
                    place_ids.add(p['id'])
            next_page_token = data.get('nextPageToken')
            if not next_page_token: break
            time.sleep(2)
    except Exception as e:
        logger.exception(f"Error during discovery for {city_name}. Aborting city.")
        conn = get_db_connection()
        with conn:
            conn.execute("UPDATE cities SET status = 'FAILED' WHERE name = ?", (city_name,))
        conn.close()
        raise # Re-raise the exception
    if not place_ids:
        logger.warning(f"No places discovered for {city_name}. City processing complete.")
        conn = get_db_connection()
        with conn:
            conn.execute("UPDATE cities SET status = 'COMPLETED' WHERE name = ?", (city_name,))
        conn.close()
        return f"No places discovered for {city_name}."
    logger.info(f"Discovered {len(place_ids)} unique places in {city_name}. Writing to DB and queueing for processing.")
    try:
        conn = get_db_connection()
        with conn:
            for place_id in place_ids:
                conn.execute("INSERT OR IGNORE INTO places (place_id, source_city) VALUES (?, ?)", (place_id, city_name))
            conn.execute("UPDATE cities SET status = 'COMPLETED' WHERE name = ?", (city_name,))
        conn.close()

        places_to_process = list(place_ids)
        if test_mode:
            places_to_process = places_to_process[:3] # Take only the first 3 places
            logger.info(f"--- TEST MODE: Limiting processing to {len(places_to_process)} places. --- ")

        for place_id in places_to_process:
            process_place.delay(place_id, skip_llm=skip_llm)
    except Exception as e:
        logger.exception(f"Database error while saving discovered places for {city_name}")
        raise # Re-raise the exception
    return f"Discovered and queued {len(places_to_process)} places for {city_name} (total discovered: {len(place_ids)})."

@app.task
def process_place(place_id, skip_llm=False):
    update_place_status(place_id, 'DETAILS_PENDING')
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        result = cursor.execute("SELECT fetched_at FROM place_details_cache WHERE place_id = ?", (place_id,)).fetchone()
    finally:
        conn.close()
    if result and result['fetched_at'] and datetime.now(timezone.utc) - datetime.fromisoformat(result['fetched_at']) < timedelta(days=config.REFRESH_DAYS):
        logger.info(f"Cache hit (fresh) for {place_id}. Enriching from cache.")
        update_place_status(place_id, 'ENRICHMENT_PENDING')
        enrich_data.delay(place_id, skip_llm=skip_llm)
        return "Cache hit. Triggered enrichment."
    else:
        logger.info(f"Cache miss or stale for {place_id}. Fetching details.")
        update_place_status(place_id, 'FETCHING_DETAILS')
        fetch_place_details.delay(place_id, skip_llm=skip_llm)
        return "Cache miss. Triggered detail fetch."

@app.task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_place_details(self, place_id, skip_llm=False):
    url = f"https://places.googleapis.com/v1/places/{place_id}"
    headers = {
        "Content-Type": "application/json", "X-Goog-Api-Key": config.API_KEY,
        "X-Goog-FieldMask": "id,displayName,formattedAddress,location,internationalPhoneNumber,websiteUri,regularOpeningHours,rating,userRatingCount,reviews,photos",
    }
    try:
        data = make_api_request(url, headers=headers)
        conn = get_db_connection()
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO place_details_cache (place_id, payload_json, fetched_at, status) VALUES (?, ?, ?, ?)",
                (place_id, json.dumps(data), datetime.now(timezone.utc).isoformat(), "SUCCESS")
            )
            if 'reviews' in data and data['reviews']:
                for review in data['reviews']:
                    review_id = review.get('name')
                    if not review_id: continue
                    conn.execute(
                        "INSERT OR REPLACE INTO reviews (review_id, place_id, author_name, rating, text, published_at_str) VALUES (?, ?, ?, ?, ?, ?)",
                        (review_id, place_id, review.get('authorAttribution', {}).get('displayName'), review.get('rating'), review.get('text', {}).get('text'), review.get('publishTime'))
                    )
        conn.close()
        logger.info(f"Successfully fetched and cached details for {place_id}.")
        update_place_status(place_id, 'ENRICHMENT_PENDING')
        enrich_data.delay(place_id, skip_llm=skip_llm)
        return "Successfully fetched and cached details."
    except Exception as exc:
        logger.warning(f"API call failed for {place_id}. Attempt {self.request.retries + 1} of {self.max_retries}.")
        if self.request.retries >= self.max_retries - 1:
            logger.error(f"Final attempt failed for {place_id}. Marking as FAILED_FETCH.")
            update_place_status(place_id, 'FAILED_FETCH')
            conn = get_db_connection()
            with conn:
                conn.execute(
                    "INSERT OR REPLACE INTO place_details_cache (place_id, payload_json, fetched_at, status) VALUES (?, ?, ?, ?)",
                    (place_id, json.dumps({"error": str(exc)}), datetime.now(timezone.utc).isoformat(), "FAILED_FETCH")
                )
            conn.close()
        raise self.retry(exc=exc)

@app.task
def enrich_data(place_id, skip_llm=False):
    update_place_status(place_id, 'ENRICHING')
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        result = cursor.execute("SELECT payload_json FROM place_details_cache WHERE place_id = ?", (place_id,)).fetchone()
        if not result or not result['payload_json']: raise ValueError("No payload found in cache.")
        data = json.loads(result['payload_json'])
        if "error" in data: raise ValueError("Payload contains fetch error")

        photo_urls = [f"https://places.googleapis.com/v1/{p['name']}/media?key={config.API_KEY}&maxHeightPx=1024" for p in data.get('photos', [])]
        gym_record = {
            "place_id": data.get('id'), "name": data.get('displayName', {}).get('text'),
            "address": data.get('formattedAddress'), "lat": data.get('location', {}).get('latitude'),
            "lng": data.get('location', {}).get('longitude'), "phone": data.get('internationalPhoneNumber'),
            "website": data.get('websiteUri'), "rating": data.get('rating'),
            "reviews_count": data.get('userRatingCount'), "hours": json.dumps(data.get('regularOpeningHours', {}).get('weekdayDescriptions')),
            "photo_urls": json.dumps(photo_urls)
        }
        with conn:
            conn.execute('''INSERT OR REPLACE INTO gyms (place_id, name, address, lat, lng, phone, website, rating, reviews_count, hours, photo_urls, processed_at) VALUES (:place_id, :name, :address, :lat, :lng, :phone, :website, :rating, :reviews_count, :hours, :photo_urls, :processed_at)''', {**gym_record, "processed_at": datetime.now(timezone.utc)})
        
        if not skip_llm: # Conditionally dispatch LLM tasks
            logger.info(f"Successfully saved base record for {place_id}. Triggering parallel LLM enrichment tasks.")
            llm_tasks = group(get_llm_description.s(place_id), get_llm_amenities.s(place_id), get_llm_misc_details.s(place_id))
            llm_tasks.apply_async()
        else:
            logger.info(f"[{place_id}] Skipping LLM enrichment as --skip-llm flag is set.")

        update_place_status(place_id, 'COMPLETED')
        return "Base record saved. Dispatched LLM tasks." if not skip_llm else "Base record saved. LLM tasks skipped."
    except Exception as e:
        update_place_status(place_id, 'FAILED_ENRICH')
        logger.exception(f"Failed to parse or enrich data for {place_id}: {e}")
        raise # Re-raise the exception
    finally:
        conn.close()

# --- LLM Enrichment Tasks ---

@app.task(bind=True, max_retries=3, default_retry_delay=60)
def get_llm_description(self, place_id):
    logger.info(f"[{place_id}] Starting LLM task: get_description (Attempt {self.request.retries + 1})")
    conn = get_db_connection()
    try:
        reviews = conn.execute("SELECT text FROM reviews WHERE place_id = ? AND text IS NOT NULL", (place_id,)).fetchall()
        if not reviews:
            return {"status": "skipped", "reason": "No reviews found."}
        reviews_text = "\n".join([row['text'] for row in reviews])
        
        prompt = f'''
(REMAIN UTF-8 encoded)
## objectif
Rédigez une brève description en un seul paragraphe pour la salle de sport en vous basant sur les avis suivants.
examples
- Five Gym Club est une salle située à Alger Centre, bien située, lumineuse et propre. Elle est très bien équipée et propose des heures d'entraînement spéciales pour les femmes ainsi que pour les hommes.
- Centre All For One situé à Zeralda, est un vaste centre de remise en forme doté de deux salles séparées pour femmes et pour hommes. Il dispose de matériel de matériel de haute qualité et propose des cours collectifs pour femmes, incluant le pilates, la zumba, et d'autres activités similaires.
- Power Fitness Constantine est une salle spacieuse, propre et bien équipée, idéale pour tous types d'entraînements. Avec un bon matériel à disposition, elle offre un cadre parfait pour atteindre vos objectifs de fitness dans une ambiance agréable.


## contraintes
- EVITEZ les jugements subjectifs, exemple (Climatisation Glaciale dans les Vestiaires Femme, Piscine avec Température Équilibrée,Propriété Stricte)
- GARDER la description courte et positive, pas plus de 3-4 phrase.
- NE PAS MENTIONNER les prix ou donner un jugement sur les prix.

# etapes
- rediger une description
- appliqueur les contraintes et changer la description si il le faut

Avis:
{reviews_text}

IMPORTANT: Votre réponse ne doit contenir que le texte de la description, et rien d'autre. N'incluez aucun autre texte, balise ou formatage.
IMPORTANT: Restez neutre dans la description et n'utilisez aucun jugement des avis comme (Les douches ne fonctionnent pas à cause de l'odeur, du warm-out )
'''
        description = call_ollama_api(prompt)

        if description:
            with conn:
                conn.execute("UPDATE gyms SET description = ? WHERE place_id = ?", (description, place_id))
            logger.info(f"[{place_id}] Successfully updated description.")
            return {"status": "success", "description": description}
        else:
            raise ValueError("Ollama call failed or returned no description.") # Raise if no description
    except Exception as e:
        logger.exception(f"[{place_id}] Failed LLM task: get_description (Attempt {self.request.retries + 1} of {self.max_retries}): {e}")
        raise self.retry(exc=e) # Re-raise the exception for retry
    finally:
        conn.close()

@app.task(bind=True, max_retries=3, default_retry_delay=60)
def get_llm_amenities(self, place_id):
    logger.info(f"[{place_id}] Starting LLM task: get_amenities (Attempt {self.request.retries + 1})")
    conn = get_db_connection()
    try:
        reviews = conn.execute("SELECT text FROM reviews WHERE place_id = ? AND text IS NOT NULL", (place_id,)).fetchall()
        if not reviews:
            return {"status": "skipped", "reason": "No reviews found."}
        reviews_text = "\n".join([row['text'] for row in reviews])

        prompt = f'''
    # objectif
    (REMAIN UTF-8 encoded)
    Extrayez une liste d'équipements à partir des avis suivants. 
    
    ## description de output
    Chaque chaîne de caractères doit être un équipement de quelques mots qui peut être utilisé comme filtre (par exemple: Sauna, Wi-Fi gratuit, Parking,
    Bar, Nutrition Conseil, Cours Collectifs, Cours de Yoga, Crossfit, Entraînement Fonctionnel, Entraînement Personnel, Entraînement Virtuel, Garderie,
    Hammam, Musculation, Parking, Pilates, Piscine, Poids Lourds, Powerlifting, Sauna, Services Spa, Thérapie de Massage, Vestiaires, Zone de Récupération,
    Équipements Cardio, Équipements High-Tech, Équipements Modernes, Équipements de Base). Si possible, favoriser ces mot au lieux de long phrase quand 
    il ya des similarite dans le sense.

    ## contraint
    EVITEZ dans la list
    - Des tags qui ne contiennent pas du francais ou des mot qui n'ont aucun rapport avec le sujet du sport.
    - Des tags qui ont de nom de personne9
    - Des tags qui ont des critique our jugement, example (Climatisation Glaciale dans les Vestiaires Femme, Piscine avec Température Équilibrée,Propriété Stricte) 
    GARDEZ la list moins de 10 elements les plu proche de le (description de output)
    RETOURNEZ UNIQUEMENT UN SEUL tableau JSON de chaînes de caractères. NE Retourne pas de description, seulement la structure JSON. example de reponse: ```json
     [
      "Spa",
       "Massage Thalassothérapie",
       "Hammam",
       "Piscine",
       "Sauna",
       "Vestiaires",
       "Zone de Récupération",
      "Services Spa",
       "Thérapie de Massage",
       "Attitude Client Pro",
       "Garderie"
     ]
    ```
    
    Avis:
{reviews_text}
    '''
        amenities = call_ollama_api(prompt, is_json_response=True)

        if amenities:
            with conn:
                conn.execute("UPDATE gyms SET amenities = ? WHERE place_id = ?", (json.dumps(amenities), place_id))
            logger.info(f"[{place_id}] Successfully updated amenities.")
            return {"status": "success", "amenities": amenities}
        else:
            raise ValueError("Ollama call failed or returned no amenities.") # Raise if no amenities
    except Exception as e:
        logger.exception(f"[{place_id}] Failed LLM task: get_amenities (Attempt {self.request.retries + 1} of {self.max_retries}): {e}")
        raise self.retry(exc=e) # Re-raise the exception for retry
    finally:
        conn.close()

@app.task(bind=True, max_retries=3, default_retry_delay=60)
def get_llm_misc_details(self, place_id):
    logger.info(f"[{place_id}] Starting LLM task: get_misc_details (Attempt {self.request.retries + 1})")
    conn = get_db_connection()
    try:
        gym_info = conn.execute("SELECT name, hours, description FROM gyms WHERE place_id = ?", (place_id,)).fetchone()
        reviews = conn.execute("SELECT text FROM reviews WHERE place_id = ? AND text IS NOT NULL", (place_id,)).fetchall()
        
        gym_name = gym_info['name'] if gym_info else ''
        gym_hours = gym_info['hours'] if gym_info else ''
        description = gym_info['description'] if gym_info else ''
        reviews_text = "\n".join([row['text'] for row in reviews])

        women_only_prompt = f"""
    En vous basant sur les informations suivantes, déterminez si cette salle de sport dispose d'un espace réservé aux femmes ou d'horaires spéciaux pour les femmes. Vous trouverez des descriptions telles que "femme, horaire femme, fille".
    Nom: {gym_name}
    Description: {description}
    Avis: {reviews_text}

    Répondez avec UNIQUEMENT un objet JSON au format : {{\"women_only\": boolean}}.
    Exemple : ```json
    {{\"women_only\": true}}
    ```
    """
        women_only_response = call_ollama_api(women_only_prompt, is_json_response=True)
        has_women_hours = women_only_response.get('women_only', False) if isinstance(women_only_response, dict) else False
        logger.info(f"[{place_id}] Got women-only status: {has_women_hours}")

        hours_prompt = f"Traduire ou résumer les horaires suivants en une seule phrase en français: {gym_hours}"
        hours_french = call_ollama_api(hours_prompt)
        logger.info(f"[{place_id}] Got French hours: {hours_french}")

        with conn:
            conn.execute("UPDATE gyms SET has_women_hours = ?, hours_french = ? WHERE place_id = ?", (has_women_hours, hours_french, place_id))
        logger.info(f"[{place_id}] Successfully updated misc details.")
        return {"status": "success", "has_women_hours": has_women_hours, "hours_french": hours_french}
    except Exception as e:
        logger.exception(f"[{place_id}] Failed LLM task: get_misc_details (Attempt {self.request.retries + 1} of {self.max_retries}): {e}")
        raise self.retry(exc=e) # Re-raise the exception for retry
    finally:
        conn.close()

@app.task
def export_data():
    logger.info("Starting data export...")
    conn = get_db_connection()
    try:
        # Fetch all gyms
        conn.row_factory = sqlite3.Row # Ensure dict-like access
        gyms = conn.execute("SELECT * FROM gyms").fetchall()
        logger.info(f"Fetched {len(gyms)} records from 'gyms' table for export.")

        if not gyms: # NEW CHECK
            logger.warning("No data found in 'gyms' table. Skipping export.")
            return "Data export skipped: No gyms found."

        # Ensure data directory exists
        os.makedirs(config.DATA_DIR, exist_ok=True)

        # --- CSV Export ---
        csv_file_path = os.path.join(config.DATA_DIR, "gyms_dz.csv")
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'place_id', 'name', 'address', 'lat', 'lng', 'phone', 'website',
                'rating', 'reviews_count', 'hours', 'description', 'amenities',
                'hours_french', 'has_women_hours', 'photo_urls', 'processed_at' # Added processed_at
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for gym in gyms:
                row = dict(gym)
                # Flatten hours (from JSON list to string)
                row['hours'] = json.loads(row['hours']) if row['hours'] else []
                row['hours'] = '; '.join(row['hours']) if isinstance(row['hours'], list) else row['hours']

                # Flatten amenities (from JSON list to string)
                row['amenities'] = json.loads(row['amenities']) if row['amenities'] else []
                row['amenities'] = ', '.join(row['amenities']) if isinstance(row['amenities'], list) else row['amenities']

                # Flatten photo_urls (from JSON list to string)
                row['photo_urls'] = json.loads(row['photo_urls']) if row['photo_urls'] else []
                row['photo_urls'] = ', '.join(row['photo_urls']) if isinstance(row['photo_urls'], list) else row['photo_urls']

                writer.writerow(row)
        logger.info(f"Exported {len(gyms)} records to {csv_file_path}")

    except Exception as e:
        logger.exception(f"Error during data export: {e}")
        raise # Re-raise the exception
    finally:
        conn.close()
    return "Data export completed successfully."

@app.task
def export_ui_json():
    logger.info("Starting UI JSON export...")
    conn = get_db_connection()
    try:
        conn.row_factory = sqlite3.Row
        # Fetch all gyms with their source city
        query = """
            SELECT
                g.place_id, g.name, p.source_city AS city, g.address, g.phone, g.website,
                g.description, g.amenities, g.hours_french, g.has_women_hours, g.rating,
                g.photo_urls
            FROM gyms g
            JOIN places p ON g.place_id = p.place_id
        """
        gyms_data = conn.execute(query).fetchall()
        logger.info(f"Fetched {len(gyms_data)} records from 'gyms' table for UI export.")

        if not gyms_data: # NEW CHECK
            logger.warning("No data found in 'gyms' table for UI export. Skipping export.")
            return "UI JSON export skipped: No gyms found."

        output_data = []
        for gym in gyms_data:
            # Default image if no photo_urls
            image_url = "https://images.pexels.com/photos/1552106/pexels-photo-1552106.jpeg?auto=compress&cs=tinysrgb&w=800" # Default image
            if gym['photo_urls']:
                photo_urls_list = json.loads(gym['photo_urls'] or '[]') # Robustly handle None/empty string
                if photo_urls_list: image_url = photo_urls_list[0]
            
            # Amenities from JSON string
            amenities_list = json.loads(gym['amenities'] or '[]') # Robustly handle None/empty string

            # Hours transformation (simplified for UI)
            hours_weekdays = gym['hours_french'] if gym['hours_french'] else "Non communiqué"
            hours_weekends = gym['hours_french'] if gym['hours_french'] else "Non communiqué"
            if gym['has_women_hours']:
                hours_weekdays += " (Horaires femmes disponibles)"
                hours_weekends += " (Horaires femmes disponibles)"

            output_data.append({
                "id": gym['place_id'],
                "name": gym['name'],
                "city": gym['city'],
                "address": gym['address'],
                "phone": gym['phone'],
                "email": "", # Not collected
                "website": gym['website'],
                "description": gym['description'],
                "amenities": amenities_list,
                "hours": {
                    "weekdays": hours_weekdays,
                    "weekends": hours_weekends
                },
                "priceRange": "Non communiqué", # Not collected
                "rating": gym['rating'],
                "image": image_url,
                "womenOnlyFacility": bool(gym['has_women_hours'])
            })
        
        ui_json_file_path = os.path.join(config.DATA_DIR, "ui-data.json")
        with open(ui_json_file_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Exported {len(output_data)} records to {ui_json_file_path}")

    except Exception as e:
        logger.exception(f"Error during UI JSON export: {e}")
        raise # Re-raise the exception
    finally:
        conn.close()
    return "UI JSON export completed successfully."