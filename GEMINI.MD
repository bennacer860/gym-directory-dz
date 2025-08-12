You are a senior data engineer. Build a Google Places API (New, v1)–only workflow to collect gyms (“salle de sport”) in Algeria.

# Scope & Constraints
- Use ONLY the official Google Places API (New).
- Cities: the 15 biggest in Algeria: Alger, Oran, Constantine, Annaba, Blida, Sétif, Batna, Djelfa, Biskra, Tébessa, Tlemcen, Béjaïa, Mostaganem, Sidi Bel Abbès, Skikda.
- For each city, use **Nearby Search** with includedTypes=["gym"] centered on the city, radius 25–35 km (justify exact value).
- Pagination: request **up to 5 pages** per search (stop earlier if nextPageToken is absent or limit reached).
- Details: for each unique place_id, fetch Place Details.
- Reviews: save **all review texts** returned by Place Details, up to **100 per place** (NOTE: today the API typically returns up to ~5 reviews; still implement a hard cap at 100 to be future-proof).
- Localization: languageCode="fr", regionCode="DZ".
- Retention: store place_id indefinitely; treat ALL other Places fields (including reviews) as cacheable ≤ 30 days—refresh or delete after that.
- Compliance: no HTML scraping; if Places content is displayed with a map, it must be on a Google map; include proper Google attribution.

# Deliverables (return all of this)
A) **Architecture (succinct)**
- ASCII diagram of: city list → Nearby Search (≤5 pages) → de-dupe by place_id → Details (with reviews) + 30-day cache → exports.

B) **Data model**
- SQLite cache table for details (place_id PK, payload_json, fetched_at).
- CSV schema: place_id, name, address, lat, lng, phone, website, rating, reviews_count, hours.
- JSONL schema: full Details payload + normalized `reviews[]` objects {author_name, rating, relative_time_description, original_language, text}.

C) **Algorithm (step-by-step)**
- Build city centers + radius.
- For each city: loop pages (≤5) with nextPageToken; collect place_ids.
- De-duplicate by place_id.
- For each place_id: if cache missing or stale (>30 days), call Details; else use cached.
- Normalize hours & reviews; persist; export CSV + JSONL.

D) **Production-grade Python (single file)**
- Python 3.10+, only stdlib + requests + python-dateutil.
- Config block at top:
  - API key env var, CITY list with coords, RADIUS_M, MAX_PAGES=5,
  - LANGUAGE="fr", REGION_CODE="DZ",
  - REFRESH_DAYS=30,
  - paths for DB/CSV/JSONL.
- Functions:
  - nearby_search(center, radius, page_token=None) using POST to places:searchNearby and X-Goog-FieldMask for minimal fields: `places.id,places.displayName,places.formattedAddress,places.location,nextPageToken`.
  - get_details(place_id) using GET to /v1/places/{place_id} with field mask: `id,displayName,formattedAddress,location,internationalPhoneNumber,websiteUri,regularOpeningHours,rating,userRatingCount,reviews`.
  - retry/backoff on 429/5xx.
  - SQLite cache helpers (ensure_db, cache_get, cache_put, is_stale).
  - flatten_record(p) → CSV row; normalize hours (weekdayDescriptions join) and reviews (map to {author_name, rating, relative_time_description, original_language, text}).
  - export_csv(rows) and export_jsonl(payloads).
- CLI “How to run” section (venv, pip install, export key, run).
- Logging of progress every 100 details; counts for pages, IDs, cache hits.

E) **Pagination policy**
- Implement loop that requests up to 5 pages per city:
  - page 1: initial request (no token),
  - if nextPageToken present → sleep ~1–2s → get next page,
  - stop on missing token or after 5 pages.

F) **Reviews policy**
- From Place Details, capture all returned reviews.
- Implement a collection cap `MAX_REVIEWS_PER_PLACE=100` to future-proof; today you will likely get ≤5—document this limitation.

G) **Cost & quotas**
- Provide a quick cost model: searches ≈ pages × cities; details ≈ unique place_ids.
- Show knobs to control spend: radius, MAX_PAGES (≤5), city subset, dropping reviews/phone/website to change SKU tier, rolling refresh (refresh ~1/30 per day).

H) **Compliance checklist**
- Retention (≤30 days for non-place_id).
- Attribution requirements.
- No mixing Places content with non-Google basemaps on the same page.
- No scraping.

# Output formatting
- Use clear headings, bullets, and fenced code blocks.
- Ensure the code is runnable as-is and respects the field masks and rate limits.
- Be explicit about sleep between paginated requests and backoff.
- Note the practical limit on reviews today (≈5) while still coding for ≤100.


