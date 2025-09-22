# pipeline/models.py

import sqlite3
import config

def get_db_connection():
    """Creates a database connection."""
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def setup_database():
    """Sets up the database tables."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # --- Cities Table ---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cities (
            name TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING, DISCOVERING, COMPLETED, FAILED
            discovered_at DATETIME
        )
    ''')

    # --- Places Table (Master List) ---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS places (
            place_id TEXT PRIMARY KEY,
            source_city TEXT,
            status TEXT NOT NULL DEFAULT 'DISCOVERED',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME,
            FOREIGN KEY (source_city) REFERENCES cities(name)
        )
    ''')

    # --- Place Details Cache ---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS place_details_cache (
            place_id TEXT PRIMARY KEY,
            payload_json TEXT,
            fetched_at DATETIME,
            status TEXT, -- SUCCESS, FAILED_FETCH
            FOREIGN KEY (place_id) REFERENCES places(place_id)
        )
    ''')

    # --- Gyms Table (Final Data) ---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS gyms (
            place_id TEXT PRIMARY KEY,
            name TEXT,
            address TEXT,
            lat REAL,
            lng REAL,
            phone TEXT,
            website TEXT,
            rating REAL,
            reviews_count INTEGER,
            hours TEXT,
            photo_urls TEXT, -- JSON array of photo URLs
            description TEXT,
            amenities TEXT, -- JSON array of strings
            hours_french TEXT,
            has_women_hours BOOLEAN,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (place_id) REFERENCES places(place_id)
        )
    ''')

    # --- Reviews Table ---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reviews (
            review_id TEXT PRIMARY KEY,
            place_id TEXT,
            author_name TEXT,
            rating INTEGER,
            text TEXT,
            published_at_str TEXT,
            FOREIGN KEY (place_id) REFERENCES places(place_id)
        )
    ''')

    # --- LLM Cache Table ---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS llm_cache (
            cache_key TEXT PRIMARY KEY,
            place_id TEXT,
            task_name TEXT,
            prompt_hash TEXT,
            response_json TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (place_id) REFERENCES places(place_id)
        )
    ''')

    conn.commit()
    conn.close()
