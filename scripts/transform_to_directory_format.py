
import argparse
import json
import logging
import sys
import requests

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

import subprocess

def call_ollama_api(prompt, ollama_url):
    """Make a call to the local Ollama API using curl."""
    try:
        curl_command = [
            "curl",
            "-s",
            ollama_url,
            "-d",
            json.dumps({"model": "wizardlm2:7b", "prompt": prompt, "stream": False}),
        ]
        result = subprocess.run(
            curl_command, capture_output=True, text=True, check=True
        )
        response_json = json.loads(result.stdout)
        response_text = response_json.get("response", "")
        try:
            # Find the start and end of the JSON array
            start_index = response_text.find('[')
            end_index = response_text.rfind(']')
            if start_index != -1 and end_index != -1:
                json_str = response_text[start_index:end_index+1]
                return json.loads(json_str)
            else:
                return response_text.strip()
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from Ollama response: {e}")
            logging.error(f"Raw response: {response_text}")
            return None
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
    Extrayez une liste d'équipements à partir des avis suivants. Retournez UNIQUEMENT un tableau JSON de chaînes de caractères.
    Chaque chaîne de caractères doit être un équipement de quelques mots qui peut être utilisé comme filtre (par exemple, "Sauna", "Wi-Fi gratuit", "Parking"). 
    Evitez les lsit de mot qui ne contiennt pas du francais ou des mot qui n'ont aucun rapport avec le sujet du sport.
    Avis:
{all_reviews}
    """
    amenities_response = call_ollama_api(amenities_prompt, ollama_url)
    amenities = []
    if amenities_response:
        amenities = amenities_response if amenities_response else []

    # Get descriptions for each gym in the batch
    for gym in batch:
        gym_reviews = ""
        if gym.get("reviews"):
            for review in gym["reviews"]:
                if review.get("text"):
                    gym_reviews += review["text"] + "\n"

        # Prompt for description
        description_prompt = f"""
Rédigez une brève description en un seul paragraphe pour la salle de sport en vous basant sur les avis suivants.
Avis:
{gym_reviews}
"""
        description = call_ollama_api(description_prompt, ollama_url)
        gym["description"] = description if description else "No description available."
        gym["amenities"] = amenities


def main():
    parser = argparse.ArgumentParser(description="Transform gym data for UI consumption.")
    parser.add_argument("--input-file", default="data/gyms_dz.jsonl", help="Path to the input JSONL file.")
    parser.add_argument("--output-file", default="data/ui-data.json", help="Path to the output JSON file.")
    parser.add_argument("--log-file", default="logs/transformer.log", help="Path to the log file.")
    parser.add_argument("--test-mode", action="store_true", help="Process only the first gym entry.")
    parser.add_argument("--batch-size", type=int, default=1, help="Batch size for Ollama calls.")
    parser.add_argument("--ollama-url", default="http://localhost:11434/api/generate", help="URL for the local Ollama API.")
    args = parser.parse_args()

    setup_logging(args.log_file)

    logging.info("Starting gym data transformation.")

    try:
        with open(args.input_file, 'r') as f:
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
    for i in range(0, len(gyms), args.batch_size):
        batch = gyms[i:i + args.batch_size]
        logging.info(f"Processing batch {i // args.batch_size + 1}/{(len(gyms) + args.batch_size - 1) // args.batch_size}")
        process_batch(batch, args.ollama_url)
        for gym in batch:
            transformed_gyms.append({
                "id": gym.get("id"),
                "name": gym.get("displayName", {}).get("text"),
                "address": gym.get("formattedAddress"),
                "latitude": gym.get("location", {}).get("latitude"),
                "longitude": gym.get("location", {}).get("longitude"),
                "phone": gym.get("internationalPhoneNumber"),
                "website": gym.get("websiteUri"),
                "rating": gym.get("rating"),
                "reviews_count": gym.get("userRatingCount"),
                "hours": gym.get("regularOpeningHours", {}).get("weekdayDescriptions"),
                "amenities": gym.get("amenities", []),
                "description": gym.get("description", "No description available.")
            })

    with open(args.output_file, 'w') as f:
        json.dump(transformed_gyms, f, indent=2)

    logging.info(f"Transformation complete. Output written to {args.output_file}")

if __name__ == "__main__":
    main()
