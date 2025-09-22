# pipeline/tasks_optimized.py
# Optimized version of tasks.py with improvements for LLM enrichment

import logging
import re
from celery import group, chord
from celery_app import app
from pipeline.models import get_db_connection
import config
import requests
import time
import json
from datetime import datetime, timedelta, timezone
import hashlib
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import pickle

logger = logging.getLogger(__name__)

# --- Data Classes for Better Organization ---
@dataclass
class ReviewData:
    """Cached review data to avoid repeated DB queries"""
    place_id: str
    reviews_text: str
    review_count: int
    
@dataclass
class LLMResult:
    """Standardized LLM result with metadata"""
    success: bool
    data: Optional[Dict]
    error: Optional[str]
    processing_time: float
    prompt_hash: str

# --- Caching Layer ---
class LLMCache:
    """Simple in-memory cache for LLM results with TTL"""
    def __init__(self, ttl_seconds=3600):
        self._cache = {}
        self._ttl = ttl_seconds
    
    def get(self, key: str) -> Optional[LLMResult]:
        if key in self._cache:
            result, timestamp = self._cache[key]
            if time.time() - timestamp < self._ttl:
                return result
            else:
                del self._cache[key]
        return None
    
    def set(self, key: str, value: LLMResult):
        self._cache[key] = (value, time.time())
    
    def clear_expired(self):
        current_time = time.time()
        expired_keys = [k for k, (_, t) in self._cache.items() if current_time - t >= self._ttl]
        for key in expired_keys:
            del self._cache[key]

# Global cache instance
llm_cache = LLMCache(ttl_seconds=3600)  # 1-hour cache

# --- Optimized Helper Functions ---

def get_prompt_hash(prompt: str) -> str:
    """Generate a hash for prompt caching"""
    return hashlib.md5(prompt.encode()).hexdigest()

def batch_fetch_reviews(place_ids: List[str]) -> Dict[str, ReviewData]:
    """Fetch reviews for multiple places in a single query"""
    conn = get_db_connection()
    try:
        placeholders = ','.join(['?' for _ in place_ids])
        query = f"""
            SELECT place_id, text 
            FROM reviews 
            WHERE place_id IN ({placeholders}) AND text IS NOT NULL
            ORDER BY place_id
        """
        reviews = conn.execute(query, place_ids).fetchall()
        
        # Group reviews by place_id
        reviews_by_place = {}
        for row in reviews:
            place_id = row['place_id']
            if place_id not in reviews_by_place:
                reviews_by_place[place_id] = []
            reviews_by_place[place_id].append(row['text'])
        
        # Create ReviewData objects
        result = {}
        for place_id in place_ids:
            if place_id in reviews_by_place:
                reviews_list = reviews_by_place[place_id]
                result[place_id] = ReviewData(
                    place_id=place_id,
                    reviews_text="\n".join(reviews_list),
                    review_count=len(reviews_list)
                )
            else:
                result[place_id] = ReviewData(
                    place_id=place_id,
                    reviews_text="",
                    review_count=0
                )
        
        return result
    finally:
        conn.close()

def call_ollama_api_with_retry(prompt: str, is_json_response: bool = False, max_retries: int = 3) -> Optional[any]:
    """Enhanced Ollama API call with retry logic and caching"""
    prompt_hash = get_prompt_hash(prompt)
    
    # Check cache first
    cached_result = llm_cache.get(prompt_hash)
    if cached_result and cached_result.success:
        logger.info(f"Cache hit for prompt hash: {prompt_hash}")
        return cached_result.data
    
    start_time = time.time()
    
    for attempt in range(max_retries):
        try:
            payload = {"model": "gpt-oss", "prompt": prompt, "stream": False}
            response = requests.post(config.OLLAMA_API_URL, json=payload, timeout=30)
            response.raise_status()
            response_json = response.json()
            response_text = response_json.get("response", "")
            
            if is_json_response:
                # Improved JSON extraction
                match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
                if match:
                    result = json.loads(match.group(1))
                else:
                    # Try to find JSON object or array
                    json_match = re.search(r'(\{[^{}]*\}|\[[^\[\]]*\])', response_text)
                    if json_match:
                        result = json.loads(json_match.group(1))
                    else:
                        raise ValueError("No valid JSON found in response")
            else:
                # Clean response text
                result = re.sub(r'<think>.*?</think>', '', response_text, flags=re.DOTALL).strip()
            
            # Cache successful result
            processing_time = time.time() - start_time
            llm_result = LLMResult(
                success=True,
                data=result,
                error=None,
                processing_time=processing_time,
                prompt_hash=prompt_hash
            )
            llm_cache.set(prompt_hash, llm_result)
            
            return result
            
        except Exception as e:
            logger.warning(f"Ollama API attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                # Cache failed result to avoid repeated failures
                processing_time = time.time() - start_time
                llm_result = LLMResult(
                    success=False,
                    data=None,
                    error=str(e),
                    processing_time=processing_time,
                    prompt_hash=prompt_hash
                )
                llm_cache.set(prompt_hash, llm_result)
                raise
            time.sleep(2 ** attempt)  # Exponential backoff
    
    return None

# --- Optimized LLM Enrichment Tasks ---

@app.task
def enrich_data_optimized(place_id: str, skip_llm: bool = False):
    """Optimized enrichment with better data flow"""
    update_place_status(place_id, 'ENRICHING')
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor()
        result = cursor.execute("SELECT payload_json FROM place_details_cache WHERE place_id = ?", (place_id,)).fetchone()
        if not result or not result['payload_json']:
            raise ValueError("No payload found in cache.")
        
        data = json.loads(result['payload_json'])
        if "error" in data:
            raise ValueError("Payload contains fetch error")
        
        # Save base gym data
        photo_urls = [f"https://places.googleapis.com/v1/{p['name']}/media?key={config.API_KEY}&maxHeightPx=1024" for p in data.get('photos', [])]
        gym_record = {
            "place_id": data.get('id'),
            "name": data.get('displayName', {}).get('text'),
            "address": data.get('formattedAddress'),
            "lat": data.get('location', {}).get('latitude'),
            "lng": data.get('location', {}).get('longitude'),
            "phone": data.get('internationalPhoneNumber'),
            "website": data.get('websiteUri'),
            "rating": data.get('rating'),
            "reviews_count": data.get('userRatingCount'),
            "hours": json.dumps(data.get('regularOpeningHours', {}).get('weekdayDescriptions')),
            "photo_urls": json.dumps(photo_urls)
        }
        
        with conn:
            conn.execute('''INSERT OR REPLACE INTO gyms (place_id, name, address, lat, lng, phone, website, rating, reviews_count, hours, photo_urls, processed_at) VALUES (:place_id, :name, :address, :lat, :lng, :phone, :website, :rating, :reviews_count, :hours, :photo_urls, :processed_at)''', {**gym_record, "processed_at": datetime.now(timezone.utc)})
        
        if not skip_llm:
            # Optimized: Pre-fetch review data for all LLM tasks
            review_data = batch_fetch_reviews([place_id])[place_id]
            
            if review_data.review_count > 0:
                # Use chord to ensure all LLM tasks complete before final processing
                logger.info(f"Starting optimized LLM enrichment for {place_id}")
                
                # Pass review data as serialized parameter to avoid repeated DB queries
                review_data_dict = {
                    'place_id': review_data.place_id,
                    'reviews_text': review_data.reviews_text,
                    'review_count': review_data.review_count
                }
                
                llm_tasks = group(
                    get_llm_all_in_one.s(place_id, review_data_dict, gym_record['name'], gym_record['hours'])
                )
                llm_tasks.apply_async()
            else:
                logger.info(f"[{place_id}] No reviews found, skipping LLM enrichment")
                update_place_status(place_id, 'COMPLETED')
        else:
            logger.info(f"[{place_id}] Skipping LLM enrichment as --skip-llm flag is set.")
            update_place_status(place_id, 'COMPLETED')
        
        return "Base record saved. LLM enrichment in progress."
        
    except Exception as e:
        update_place_status(place_id, 'FAILED_ENRICH')
        logger.exception(f"Failed to parse or enrich data for {place_id}: {e}")
        raise
    finally:
        conn.close()

@app.task(bind=True, max_retries=3, default_retry_delay=60)
def get_llm_all_in_one(self, place_id: str, review_data_dict: Dict, gym_name: str, gym_hours: str):
    """Combined LLM task that processes all enrichments in a single call"""
    logger.info(f"[{place_id}] Starting combined LLM enrichment (Attempt {self.request.retries + 1})")
    
    reviews_text = review_data_dict['reviews_text']
    
    # Single comprehensive prompt for all enrichments
    combined_prompt = f'''
(REMAIN UTF-8 encoded)
Analysez les avis suivants pour la salle de sport "{gym_name}" et fournissez les informations demandées en format JSON.

## Instructions:
1. Rédigez une description en français (3-4 phrases maximum, positive et neutre)
2. Extrayez une liste d'équipements/services (maximum 10, en français)
3. Déterminez s'il y a des espaces/horaires réservés aux femmes
4. Traduisez les horaires en français

## Contraintes:
- Évitez les jugements subjectifs et les critiques
- Ne mentionnez pas les prix
- Restez factuel et positif
- Les équipements doivent être des mots-clés courts (ex: "Sauna", "Parking", "Cours Collectifs")

## Horaires actuels:
{gym_hours}

## Avis à analyser:
{reviews_text}

## Format de réponse obligatoire:
```json
{{
    "description": "Description de la salle en français",
    "amenities": ["équipement1", "équipement2", ...],
    "has_women_hours": true/false,
    "hours_french": "Traduction des horaires en français"
}}
```

IMPORTANT: Répondez UNIQUEMENT avec le JSON, sans autre texte.
'''

    try:
        start_time = time.time()
        result = call_ollama_api_with_retry(combined_prompt, is_json_response=True)
        processing_time = time.time() - start_time
        
        if not result:
            raise ValueError("LLM returned empty result")
        
        # Validate result structure
        required_fields = ['description', 'amenities', 'has_women_hours', 'hours_french']
        for field in required_fields:
            if field not in result:
                raise ValueError(f"Missing required field: {field}")
        
        # Save all results in a single transaction
        conn = get_db_connection()
        try:
            with conn:
                conn.execute("""
                    UPDATE gyms 
                    SET description = ?, 
                        amenities = ?, 
                        has_women_hours = ?, 
                        hours_french = ?
                    WHERE place_id = ?
                """, (
                    result['description'],
                    json.dumps(result['amenities']),
                    result['has_women_hours'],
                    result['hours_french'],
                    place_id
                ))
            
            logger.info(f"[{place_id}] Successfully updated all LLM fields in {processing_time:.2f}s")
            
            # Update status only after successful save
            update_place_status(place_id, 'COMPLETED')
            
            return {
                "status": "success",
                "place_id": place_id,
                "processing_time": processing_time,
                "fields_updated": required_fields
            }
            
        finally:
            conn.close()
            
    except Exception as e:
        logger.exception(f"[{place_id}] Failed combined LLM enrichment (Attempt {self.request.retries + 1}): {e}")
        if self.request.retries >= self.max_retries - 1:
            update_place_status(place_id, 'FAILED_ENRICH')
        raise self.retry(exc=e)

# --- Batch Processing for Multiple Places ---

@app.task
def batch_enrich_places(place_ids: List[str], batch_size: int = 10):
    """Process multiple places in batches for efficiency"""
    logger.info(f"Starting batch enrichment for {len(place_ids)} places")
    
    # Pre-fetch all review data in one query
    all_review_data = batch_fetch_reviews(place_ids)
    
    # Process in batches
    for i in range(0, len(place_ids), batch_size):
        batch = place_ids[i:i + batch_size]
        
        # Create tasks for this batch
        batch_tasks = []
        for place_id in batch:
            if all_review_data[place_id].review_count > 0:
                batch_tasks.append(
                    enrich_data_optimized.s(place_id, skip_llm=False)
                )
        
        # Execute batch
        if batch_tasks:
            job = group(batch_tasks)
            job.apply_async()
            
        # Small delay between batches to avoid overwhelming the system
        time.sleep(1)
    
    return f"Batch enrichment started for {len(place_ids)} places"

# --- Monitoring and Analytics ---

@app.task
def get_enrichment_stats():
    """Get statistics about LLM enrichment performance"""
    conn = get_db_connection()
    try:
        stats = {}
        
        # Get completion rates
        completion_stats = conn.execute("""
            SELECT 
                status, 
                COUNT(*) as count 
            FROM places 
            GROUP BY status
        """).fetchall()
        
        stats['completion_by_status'] = {row['status']: row['count'] for row in completion_stats}
        
        # Get enrichment field completion
        field_stats = conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN description IS NOT NULL THEN 1 ELSE 0 END) as with_description,
                SUM(CASE WHEN amenities IS NOT NULL THEN 1 ELSE 0 END) as with_amenities,
                SUM(CASE WHEN has_women_hours IS NOT NULL THEN 1 ELSE 0 END) as with_women_hours,
                SUM(CASE WHEN hours_french IS NOT NULL THEN 1 ELSE 0 END) as with_hours_french
            FROM gyms
        """).fetchone()
        
        stats['field_completion'] = dict(field_stats)
        
        # Cache statistics
        stats['cache_info'] = {
            'size': len(llm_cache._cache),
            'expired_cleared': 0
        }
        llm_cache.clear_expired()
        
        return stats
        
    finally:
        conn.close()

# --- Helper function to update place status (unchanged from original) ---
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