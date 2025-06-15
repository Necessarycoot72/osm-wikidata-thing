# OSM Feature Finder for Wikidata Matching

## Description

This script identifies OpenStreetMap (OSM) features that meet specific criteria indicating they might need a Wikidata tag added. The criteria are:
1. The OSM feature has a `gnis:feature_id` tag.
2. The OSM feature does *not* currently have a `wikidata` tag.
3. A corresponding Wikidata entry exists that has the same GNIS ID (Wikidata property P590) as the `gnis:feature_id` tag on the OSM feature.

The script queries the Overpass API for OSM data and the Wikidata Query Service for Wikidata information.

## Requirements

*   Python 3.7+
*   The `requests` library (for OSM data fetching).
*   The `aiohttp` library (for asynchronous Wikidata queries).
(All dependencies are listed in `requirements.txt`.)

## Installation

1.  Clone this repository (if applicable).
2.  Install the necessary Python packages:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

To run the script, execute the following command in your terminal:
```bash
python find_osm_features.py [--timeout <seconds>]
```
**Command-line Arguments:**
*   `--timeout <seconds>`: (Optional) Sets the timeout value in seconds. Default: 10000 seconds (when not provided via CLI and no input given at prompt). This value is used directly for two purposes:
    1.  **Server-Side Query Timeout:** It's dynamically inserted into the Overpass query as `[timeout:VALUE]`, instructing the Overpass server to abandon the query if it takes longer than this duration on the server.
    2.  **Client-Side Request Timeout:** The exact same value is used as the timeout for the Python script's HTTP request to the Overpass API.
    This synchronized approach ensures that if the Overpass server stops processing the query due to its timeout, the script will also stop waiting at approximately the same moment, providing clear and timely feedback. It is recommended to use a sufficiently high value (e.g., 10000 seconds) for potentially complex global queries. Setting this too low may cause both the server and the client to terminate prematurely, leading to incomplete or no data. If `--timeout` is not specified, the script will prompt for a value.

Please be patient, as the script makes external API calls to Overpass and Wikidata, which can take some time depending on the amount of data and network conditions.

## Output

*   **Console Logs:** The script prints informational messages and error logs to the console during its execution.
*   **JSON File:** The primary output is a JSON file named `osm_features_to_update.json`. This file contains a list of OSM features that meet all the specified criteria. Each item in the list is a JSON object with the following structure:
    ```json
    {
        "osm_id": 12345, // OSM element ID
        "osm_type": "node", // Type of OSM element (node, way, or relation)
        "tags": {
            "gnis:feature_id": "xxxx",
            "name": "Feature Name",
            // ... other OSM tags
        },
        "matched_wikidata_qid": "Qyyyy" // The QID of the matched Wikidata item
    }
    ```
    If no features are found, the file will contain an empty list.
*   **Purged Duplicates File (Optional):** If features with duplicate `gnis:feature_id` values are found in the initial Overpass API data (either newly fetched or from a cached raw dataset), these features are saved to `purged_duplicate_gnis_features.json` and excluded from further processing. Each feature in this file is stored in its original JSON structure from the Overpass API. This file is only created if such duplicates are detected.

## Resume Capability

The script is designed to be resilient to interruptions.
-   **Automatic Progress Saving**: If the script is interrupted during its run (e.g., by pressing `Ctrl+C`, which sends a SIGINT signal on most platforms), it will save its current progress before exiting. This includes the list of features it was checking, any results found so far, and potentially the raw data from an interrupted Overpass API fetch. The primary method for interruption and saving progress is via `Ctrl+C` (SIGINT).
-   **Progress Storage**: The progress, including the elements mentioned above, is stored in a file named `resume_state.json`, located in the same directory where the script is run.
-   **Automatic Resumption**: When the script is started again, it automatically checks for the `resume_state.json` file.
    -   If saved progress (like partially processed features or previously found results) is found, it loads this and attempts to continue from where it left off, avoiding reprocessing.
    -   Additionally, if the script was interrupted after fetching data from the Overpass API in the previous run, this raw Overpass data is also saved. Upon restarting, if this saved raw data is found, you will be prompted to either use this cached data for the session or fetch fresh data from the API.
-   **Automatic Cleanup**: Upon successful completion of a full run (i.e., all features are processed and final outputs are generated without interruption), the `resume_state.json` file is automatically deleted. This ensures that a subsequent run starts fresh, unless a new interruption occurs.

## How it Works

1.  **Fetch OSM Data:** The script queries the Overpass API to retrieve all OSM features (nodes, ways, and relations - using the `nwr` shorthand) that have a `gnis:feature_id` tag. The query used is:
    ```ql
    [out:json][timeout:VALUE_FROM_USER_OR_DEFAULT];
    (
      nwr["gnis:feature_id"];
    );
    out body;
    >;
    out skel qt;
    ```
    The `VALUE_FROM_USER_OR_DEFAULT` is determined by the `--timeout` CLI argument or the interactive prompt.

    #### Overpass API Interaction & Timeout
    The `find_osm_features.py` script dynamically constructs the Overpass query. The server-side timeout for this query (how long the Overpass server itself will work on the query) is set using the value provided by the `--timeout` command-line argument or the interactive prompt (defaulting to 10000 seconds if no input is given).
    The client-side timeout for the script's HTTP request to the Overpass API is set to the exact same value. This synchronization ensures that the script doesn't wait unnecessarily if the server has already timed out the query. The script logs the timeout value being used and provides feedback if a timeout occurs or if the Overpass API returns 0 elements after a successful query.

2.  **Filter Duplicate GNIS IDs:** After obtaining the raw Overpass data (either freshly fetched or from a cache), the script checks for `gnis:feature_id` values that appear on multiple OSM features. If such duplicates are found, all OSM features sharing any of these non-unique GNIS IDs are removed from the main processing pipeline. These "purged" features are saved to `purged_duplicate_gnis_features.json` for review. This step is taken because a GNIS ID should ideally map to a single Wikidata item and thus be unique among OSM features considered for Wikidata tagging; duplicates often indicate data issues that need manual inspection.

3.  **Filter Existing Wikidata Tags:** It filters out any features from the (now de-duplicated) list that already possess a `wikidata` tag.
4.  **Query Wikidata Concurrently:** For each remaining feature, it extracts the `gnis:feature_id`. The script then uses `asyncio` and `aiohttp` to query the Wikidata Query Service (using SPARQL) concurrently for multiple features at a time (defaulting to a concurrency of up to 25 queries). This asynchronous approach significantly speeds up the process of finding Wikidata items with a matching GNIS ID (property P590).
5.  **Compile Results:** If a match is found on Wikidata, the OSM feature's details (ID, type, tags) along with the matched Wikidata QID are added to the final list.
6.  **Save Output:** This list is then saved to the `osm_features_to_update.json` file.

## Running Tests

Unit tests are provided to verify the functionality of individual components of the script. To run the tests, execute:
```bash
python -m unittest tests.test_osm_processing.py
python -m unittest tests.test_resume_functionality.py
```
Ensure you are in the root directory of the project when running this command.
