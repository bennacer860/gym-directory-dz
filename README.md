# Gym Directory DZ

## A) Architecture

```ascii
+--------------+     +-----------------------+     +-----------------+     +----------------------+     +-----------+
|   City List  | --> |   Nearby Search       | --> |  Deduplication  | --> |  Place Details       | --> |  Exports  |
| (15 cities)  |     | (<=5 pages per city)  |     | (by place_id)   |     | (with 30-day cache)  |     | (CSV/JSONL) |
+--------------+     +-----------------------+     +-----------------+     +----------------------+     +-----------+
```

## B) Data Model

### SQLite Cache

-   **Table:** `place_cache`
    -   `place_id` (TEXT, PRIMARY KEY)
    -   `payload_json` (TEXT)
    -   `fetched_at` (TIMESTAMP)

### CSV Schema

`place_id,name,address,lat,lng,phone,website,rating,reviews_count,hours`

### JSONL Schema

Each line is a JSON object representing the full Place Details payload, with an added normalized `reviews` array.

```json
{
  "id": "...",
  "displayName": { ... },
  "formattedAddress": "...",
  // ... other fields from Place Details
  "reviews": [
    {
      "author_name": "...",
      "rating": 5,
      "relative_time_description": "...",
      "original_language": "...",
      "text": "..."
    }
  ]
}
```

## C) Algorithm

1.  **Initialization:**
    *   Load configuration from environment variables and constants.
    *   Create the SQLite database and table if they don't exist.
2.  **City Iteration:**
    *   For each city in the predefined list:
        *   Initialize `nextPageToken` to `None`.
        *   Start a loop that will run for a maximum of `MAX_PAGES`.
3.  **Pagination Loop:**
    *   Perform a Nearby Search request for the current city with the current `nextPageToken`.
    *   Collect all `place_id`s from the response.
    *   If a `nextPageToken` is present in the response, store it for the next iteration and sleep for a short duration (e.g., 2 seconds) to avoid hitting rate limits.
    *   If no `nextPageToken` is returned, or the page limit is reached, break the loop for the current city.
4.  **Deduplication:**
    *   After iterating through all cities, create a unique set of all collected `place_id`s.
5.  **Details Fetching:**
    *   For each unique `place_id`:
        *   Check the cache:
            *   If the `place_id` exists in the cache and the data is not stale (i.e., fetched within `REFRESH_DAYS`), use the cached data.
            *   Otherwise, fetch the Place Details from the API.
        *   If fetched from the API, store the new data in the cache with the current timestamp.
6.  **Data Normalization and Export:**
    *   For each fetched place (from cache or API):
        *   Normalize the opening hours and reviews as per the specified schema.
        *   Append the flattened data to a list for CSV export.
        *   Append the full JSON payload (with normalized reviews) to a list for JSONL export.
7.  **File Generation:**
    *   Write the collected data to CSV and JSONL files.
8.  **Logging:**
    *   Log progress, counts (pages, IDs, cache hits), and any errors encountered throughout the process.

## D) Production-grade Python

See `dz_gym_scraper.py`.

## E) Pagination Policy

The script will request up to 5 pages of results for each city. The pagination logic is as follows:

1.  The first request for a city is made without a page token.
2.  If the response contains a `nextPageToken`, the script will wait for 2 seconds before making the next request using this token.
3.  This process continues until one of the following conditions is met:
    *   The script has retrieved 5 pages for the current city.
    *   The API response does not contain a `nextPageToken`.

## F) Reviews Policy

The script will capture all reviews returned by the Place Details API call. A hard cap of `MAX_REVIEWS_PER_PLACE=100` is implemented to ensure that the script can handle a large number of reviews in the future, even though the current API typically returns a much smaller number (around 5).

## G) Cost & Quotas

*   **Cost Model:**
    *   **Nearby Search:** The number of searches is approximately `(Number of Pages) * (Number of Cities)`.
    *   **Place Details:** The number of details requests is equal to the number of unique `place_id`s found.
*   **Controlling Spend:**
    *   **Radius:** A smaller radius will result in fewer places found, thus reducing the number of Place Details requests.
    *   **MAX_PAGES:** Limiting the number of pages per city directly reduces the number of Nearby Search requests.
    *   **City Subset:** Running the script on a smaller list of cities will reduce both search and details requests.
    *   **Field Masking:** The script uses field masks to request only the necessary data, which can help reduce costs by avoiding higher-priced data SKUs (e.g., by dropping `reviews`, `internationalPhoneNumber`, or `websiteUri`).
    *   **Rolling Refresh:** The caching mechanism with a 30-day refresh period ensures that we don't repeatedly fetch data for the same place, significantly reducing costs on subsequent runs. A strategy to refresh only a fraction of the cache daily (e.g., `1/30th`) could further smoothen the costs.

## I) How to Run

1.  **Set up a virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2.  **Install dependencies:**
    ```bash
    pip install requests python-dateutil python-dotenv
    ```

3.  **Set the API key:**

    Create a `.env` file in the root of the project and add the following line:
    ```
    GOOGLE_PLACES_API_KEY=YOUR_API_KEY
    ```

5.  **Run in test mode:**
    ```bash
    python dz_gym_scraper.py --test-mode
    ```


*   **Data Retention:** `place_id` is stored indefinitely. All other data fetched from the Places API is treated as cacheable for a maximum of 30 days.
*   **Attribution:** Any application that displays this data on a map must use a Google Map and display the proper Google attributions.
*   **No Scraping:** The script does not scrape HTML content and relies solely on the Google Places API.
*   **Basemap:** Places content will not be mixed with non-Google basemaps in any frontend application.
