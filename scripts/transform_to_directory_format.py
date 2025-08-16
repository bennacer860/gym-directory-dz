import argparse
import json
import logging
import sys
import requests
import subprocess
import re

def setup_logging(log_file):
    """Set up logging to both file and console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

def call_ollama_api(prompt, ollama_url, is_json_response=False):
    """Make a call to the local Ollama API using curl."""
    try:
        curl_command = [
            "curl",
            "-s",
            ollama_url,
            "-d",
            json.dumps({"model": "wizardlm2:7b", "prompt": prompt, "stream": False}, ensure_ascii=False),
        ]
        result = subprocess.run(
            curl_command, capture_output=True, text=True, check=True, encoding="utf-8"
        )
        response_json = json.loads(result.stdout)
        response_text = response_json.get("response", "")
        
        if is_json_response:
            try:
                # Extract JSON from markdown code block
                match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
                if match:
                    json_str = match.group(1)
                    return json.loads(json_str)
                else:
                    # Fallback for plain JSON
                    return json.loads(response_text)
            except json.JSONDecodeError as e:
                logging.error(f"Error decoding JSON from Ollama response: {e}")
                logging.error(f"Raw response: {response_text}")
                return None
        else:
            return response_text.strip()

    except subprocess.CalledProcessError as e:
        logging.error(f"Error calling Ollama API with curl: {e}")
        logging.error(f"Stderr: {e.stderr}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from Ollama response: {e}")
        logging.error(f"Raw response: {result.stdout}")
        return None

def process_batch(batch, ollama_url):
    """Process a batch of gyms to get amenities and descriptions from Ollama."""
    # Combine reviews for amenities extraction
    all_reviews = ""
    for gym in batch:
        if gym.get("reviews"):
            for review in gym["reviews"]:
                if review.get("text"):
                    all_reviews += review["text"] + "\n"

    # Prompt for amenities
    amenities_prompt = f"""
    (REMAIN UTF-8 encoded)
    Extrayez une liste d'équipements à partir des avis suivants. Retournez UNIQUEMENT UN SEUL tableau JSON de chaînes de caractères. 
    NE Retourne pas de description, seulement la structure JSON example: ```json
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
    
    Chaque chaîne de caractères doit être un équipement de quelques mots qui peut être utilisé comme filtre (par exemple, Sauna, Wi-Fi gratuit, Parking,
    Bar, Nutrition Conseil, Cours Collectifs, Cours de Yoga, Crossfit, Entraînement Fonctionnel, Entraînement Personnel, Entraînement Virtuel, Garderie,
    Hammam, Musculation, Parking, Pilates, Piscine, Poids Lourds, Powerlifting, Sauna, Services Spa, Thérapie de Massage, Vestiaires, Zone de Récupération,
    Équipements Cardio, Équipements High-Tech, Équipements Modernes, Équipements de Base). 
    EVITEZ de dans la list
    - Des tags qui ne contiennent pas du francais ou des mot qui n'ont aucun rapport avec le sujet du sport.
    - Des tags qui ont de nom de personne
    - Des tags qui ont des critique
    GARDEZ la list moins de 10 elements 
    Avis:
{all_reviews}
    """
    amenities = call_ollama_api(amenities_prompt, ollama_url, is_json_response=True)
    if not isinstance(amenities, list):
        amenities = []


    # Get descriptions for each gym in the batch
    for gym in batch:
        gym_reviews = ""
        if gym.get("reviews"):
            for review in gym["reviews"]:
                if review.get("text"):
                    gym_reviews += review["text"] + "\n"

        # Prompt for description
        description_prompt = f"""
(REMAIN UTF-8 encoded)
Rédigez une brève description en un seul paragraphe pour la salle de sport en vous basant sur les avis suivants.
Avis:
{gym_reviews}
"""
        description = call_ollama_api(description_prompt, ollama_url)
        gym["description"] = description if description else "No description available."
        gym["amenities"] = amenities

def determine_women_only_with_ollama(gym, ollama_url):
    """Determine if a gym is women-only using Ollama."""
    name = gym.get("displayName", {}).get("text", "")
    description = gym.get("description", "")
    reviews = " ".join([review.get("text", "") for review in gym.get("reviews", [])])
    
    prompt = f"""
    Based on the following information, is this gym exclusively for women?
    Name: {name}
    Description: {description}
    Reviews: {reviews}

    Respond with ONLY a JSON object in the format: {{"women_only": boolean}}.
    Example: ```json
    {{"women_only": true}}
    ```
    """
    
    response = call_ollama_api(prompt, ollama_url, is_json_response=True)
    
    if response and isinstance(response, dict) and "women_only" in response:
        return response.get("women_only", False)
    
    logging.warning("Could not determine women-only status from Ollama, defaulting to False.")
    return False

def extract_city(address):
    if not address:
        return ""
    parts = address.split(',')
    if len(parts) > 1:
        city_part = parts[-1].strip()
        # Remove postal code if present
        return ''.join([i for i in city_part if not i.isdigit()]).strip()
    return address

def format_hours(hours_list):
    if not hours_list:
        return {"weekdays": "N/A", "weekends": "N/A"}
    
    # Simple assumption: first entry for weekdays, last for weekends
    # A more robust implementation would parse each line.
    return {
        "weekdays": hours_list[0] if hours_list else "N/A",
        "weekends": hours_list[-1] if len(hours_list) > 1 else hours_list[0] if hours_list else "N/A"
    }

def get_placeholder_image(index):
    images = [
        "https://images.pexels.com/photos/1552242/pexels-photo-1552242.jpeg?auto=compress&cs=tinysrgb&w=800",
        "https://images.pexels.com/photos/1229356/pexels-photo-1229356.jpeg?auto=compress&cs=tinysrgb&w=800",
        "https://images.pexels.com/photos/1431282/pexels-photo-1431282.jpeg?auto=compress&cs=tinysrgb&w=800",
        "https://images.pexels.com/photos/1552106/pexels-photo-1552106.jpeg?auto=compress&cs=tinysrgb&w=800",
        "https://images.pexels.com/photos/1552252/pexels-photo-1552252.jpeg?auto=compress&cs=tinysrgb&w=800",
        "https://images.pexels.com/photos/1552103/pexels-photo-1552103.jpeg?auto=compress&cs=tinysrgb&w=800",
        "https://images.pexels.com/photos/1552101/pexels-photo-1552101.jpeg?auto=compress&cs=tinysrgb&w=800",
        "https://images.pexels.com/photos/1552100/pexels-photo-1552100.jpeg?auto=compress&cs=tinysrgb&w=800",
    ]
    return images[index % len(images)]

def main():
    parser = argparse.ArgumentParser(description="Transform gym data for UI consumption.")
    parser.add_argument("--input-file", default="data/gyms_dz.jsonl", help="Path to the input JSONL file.")
    parser.add_argument("--output-file", default="data/ui-data.ts", help="Path to the output TS file.")
    parser.add_argument("--log-file", default="logs/transformer.log", help="Path to the log file.")
    parser.add_argument("--test-mode", action="store_true", help="Process only the first gym entry.")
    parser.add_argument("--batch-size", type=int, default=1, help="Batch size for Ollama calls.")
    parser.add_argument("--ollama-url", default="http://localhost:11434/api/generate", help="URL for the local Ollama API.")
    args = parser.parse_args()

    setup_logging(args.log_file)

    logging.info("Starting gym data transformation.")

    try:
        with open(args.input_file, 'r', encoding='utf-8') as f:
            gyms = [json.loads(line) for line in f]
    except FileNotFoundError:
        logging.error(f"Input file not found: {args.input_file}")
        return
    except json.JSONDecodeError:
        logging.error(f"Could not decode JSON from input file: {args.input_file}")
        return

    if args.test_mode:
        gyms = gyms[:1]
        logging.info("Running in test mode. Processing only the first gym.")

    transformed_gyms = []
    for i, gym in enumerate(gyms):
        logging.info(f"Processing gym {i+1}/{len(gyms)}")
        
        process_batch([gym], args.ollama_url)
        
        women_only = determine_women_only_with_ollama(gym, args.ollama_url)

        transformed_gyms.append({
            "id": gym.get("id"),
            "name": gym.get("displayName", {}).get("text"),
            "city": extract_city(gym.get("formattedAddress")),
            "address": gym.get("formattedAddress"),
            "phone": gym.get("internationalPhoneNumber"),
            "email": "",  # Not available from Places API
            "website": gym.get("websiteUri"),
            "description": gym.get("description", "No description available."),
            "amenities": gym.get("amenities", []),
            "hours": format_hours(gym.get("regularOpeningHours", {}).get("weekdayDescriptions")),
            "priceRange": "Non communiqué",
            "rating": gym.get("rating"),
            "image": get_placeholder_image(i),
            "womenOnlyFacility": women_only
        })

    with open(args.output_file, 'w', encoding='utf-8') as f:
        f.write("export default ")
        json.dump(transformed_gyms, f, indent=2, ensure_ascii=False)
        f.write(";")

    logging.info(f"Transformation complete. Output written to {args.output_file}")

if __name__ == "__main__":
    main()