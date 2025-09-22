# Gym Directory DZ - Data Pipeline

This project implements a robust data pipeline to collect and enrich gym ("salle de sport") information across the 15 largest cities in Algeria using the Google Places API (New, v1) and a local Large Language Model (LLM) via Ollama. The pipeline is built with Celery for task orchestration, Redis as a message broker, and SQLite for data persistence.

## Features

*   **Google Places API (New, v1):** Utilizes Nearby Search and Place Details for data collection.
*   **Celery Task Queue:** Asynchronous processing for scalability, retries, and error handling.
*   **Redis Integration:** Used as the message broker and result backend for Celery.
*   **SQLite Database:** Persistent storage for raw API responses, processed data, and pipeline state.
*   **LLM Enrichment (Ollama):** Extracts detailed descriptions, amenities, French opening hours, and women-only facility status from reviews using a local LLM.
*   **Data Caching:** Implements a 30-day cache for Place Details to optimize API usage and costs.
*   **Comprehensive Logging:** Detailed logs for pipeline progress and errors.
*   **Flexible Export Formats:** Exports data into CSV, JSONL, and a UI-ready JSON format.
*   **Flower Monitoring:** Web-based monitoring tool for Celery tasks.

## Architecture

```ascii
+-----------------+     +-----------------+     +---------------------+     +---------------------+     +---------------------+     +-----------------+
|  start_pipeline | --> | discover_places | --> |   process_place     | --> | fetch_place_details | --> |     enrich_data     | --> |  LLM Enrichment |
| (CLI Trigger)   |     | (per city)      |     | (cache check)       |     | (Google API)        |     | (base processing)   |     | (3 parallel tasks) |
+-----------------+     +-----------------+     +---------------------+     +---------------------+     +---------------------+     +-----------------+
                                                                                                                            |
                                                                                                                            +-----------------+
                                                                                                                            |  export_data    |
                                                                                                                            | (CSV, JSONL, UI)|
                                                                                                                            +-----------------+
```

## Prerequisites

Before running the pipeline, ensure you have the following installed and configured:

*   **Python 3.10+**
*   **`pip`** (Python package installer)
*   **`redis-server`**: Running locally or accessible via network.
*   **Ollama**: Running locally.
    *   Download and install Ollama from [ollama.com](https://ollama.com/).
    *   Pull the `gpt-oss` model (or your preferred compatible model): `ollama pull gpt-oss`
*   **Google Places API Key (New, v1)**: Obtain one from the Google Cloud Console.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-repo/gym-directory-dz.git
    cd gym-directory-dz
    ```

2.  **Create and activate a Python virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install Python dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables:**
    Create a `.env` file in the root of the project with your API key and Ollama URL:
    ```
    GOOGLE_PLACES_API_KEY=YOUR_GOOGLE_PLACES_API_KEY
    OLLAMA_API_URL=http://localhost:11434/api/generate
    ```
    *(Ensure Ollama is running and the `gpt-oss` model is pulled.)*

## Running the Pipeline

The pipeline consists of several services that need to be running concurrently.

1.  **Start Redis Server:**
    If you don't have Redis running as a system service, you can start it manually:
    ```bash
    redis-server
    or
    docker run -d --name my-redis -p 6379:6379 redis
    ```
    *(Leave this terminal window open.)*

2.  **Initialize the Database:**
    This creates the necessary SQLite tables. You only need to do this once or if you want to reset your data.
    ```bash
    python run_pipeline.py initdb
    ```

3.  **Start Celery Worker:**
    This process will pick up and execute tasks from the Redis queue. You can adjust `--concurrency` based on your system's resources and API rate limits.
    ```bash
    celery -A celery_app.app worker --loglevel=info --concurrency=4
    ```
    *(Leave this terminal window open.)*

4.  **Start Flower Monitoring (Optional but Recommended):**
    Flower provides a real-time web interface to monitor your Celery tasks.
    ```bash
    celery -A celery_app.app flower
    ```
    Access Flower in your web browser at `http://localhost:5555`.
    *(Leave this terminal window open.)*

5.  **Trigger the Pipeline:**
    Once Redis, the Celery worker, and optionally Flower are running, you can start the data collection process:
    ```bash
    python run_pipeline.py start
    ```
    *   **Test Mode:** To run a quick test (processes only the first city and one page of results), use:
        ```bash
        python run_pipeline.py start --test
        ```

## Output Files

After the pipeline completes, the following files will be generated in the `data/` directory:

*   **`gyms_dz.csv`**: A CSV file containing flattened gym data, including LLM-enriched fields.
*   **`gyms_dz.jsonl`**: A JSONL file where each line is a full JSON object representing the original Google Places API payload, augmented with LLM-enriched data and normalized reviews.
*   **`ui-data.json`**: A single JSON file formatted specifically for a UI, containing a list of gym objects with transformed fields.

## Cost & Quotas (Google Places API)

*   **Cost Model:**
    *   **Nearby Search:** Costs are proportional to `(Number of Pages) * (Number of Cities)`.
    *   **Place Details:** Costs are proportional to the number of unique `place_id`s.
*   **Controlling Spend:**
    *   **`config.RADIUS_M`**: Adjust the search radius.
    *   **`config.MAX_PAGES`**: Limit the number of pages per city.
    *   **City Subset:** Process a smaller list of cities.
    *   **Field Masking:** The pipeline uses field masks to request only necessary data, which helps avoid higher-priced data SKUs.
    *   **Caching (`config.REFRESH_DAYS`):** The 30-day cache significantly reduces costs on subsequent runs by avoiding redundant API calls.

## Compliance

*   **Data Retention:** Non-`place_id` data is treated as cacheable for up to 30 days.
*   **Attribution:** If Places content is displayed with a map, it must be on a Google map and include proper Google attribution.
*   **No Scraping:** All data is collected via the official Google Places API.