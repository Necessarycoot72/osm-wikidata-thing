import asyncio
import aiohttp
import requests
import logging
import json
import os
import argparse
import time
import sys
import collections
import csv
import io

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Custom Exception for Overpass API timeouts.
class OverpassTimeoutError(Exception):
    pass

# --- Configuration Loading ---

def load_config(config_path='config.json'):
    """Loads configuration from a JSON file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_path}. Please create it.")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from the configuration file: {config_path}")
        sys.exit(1)

# Load config at startup
config = load_config()

# API endpoints and query templates from config
OVERPASS_API_URL = config.get("OVERPASS_API_URL", "https://overpass-api.de/api/interpreter")
WIKIDATA_SPARQL_URL = config.get("WIKIDATA_SPARQL_URL", "https://query.wikidata.org/sparql")
BASE_OVERPASS_QUERY = config.get("BASE_OVERPASS_QUERY")
SPARQL_QUERY = config.get("SPARQL_QUERY")

# Validate that essential queries are loaded
if not all([BASE_OVERPASS_QUERY, SPARQL_QUERY]):
    logging.error("BASE_OVERPASS_QUERY or SPARQL_QUERY is missing from the config file.")
    sys.exit(1)

# --- Script Constants ---
PURGED_SHARED_GNIS_FILENAME = "purged_duplicate_gnis_features.json"
PURGED_MULTI_ID_FILENAME = "gnis_ids_on_multiple_features.json"
FINAL_RESULTS_FILENAME = "osm_features_to_update.json"
CONCURRENCY_LIMIT = 5


def save_data_atomically(data, filename, log_context):
    """Saves data to a file atomically by writing to a temp file and then replacing."""
    if not data:
        return

    temp_file = f"{filename}.tmp"
    try:
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        os.replace(temp_file, filename)
        logging.info(f"Saved {len(data)} {log_context} to {filename}.")
    except Exception as e:
        logging.error(f"Error saving {log_context} to {filename}: {e}")
        if os.path.exists(temp_file):
            os.remove(temp_file)


def fetch_osm_features_with_gnis_id(user_timeout):
    """
    Fetches features from Overpass API based on BASE_OVERPASS_QUERY,
    expecting CSV output, and transforms them into a list of dicts.
    """
    try:
        response = requests.get(OVERPASS_API_URL, params={'data': BASE_OVERPASS_QUERY.format(timeout=user_timeout)}, timeout=user_timeout)
        response.raise_for_status()
        csv_text = response.text

        elements = []
        csvfile = io.StringIO(csv_text) # Use io.StringIO to treat the CSV string as a file
        reader = csv.DictReader(csvfile, delimiter='\t') # Overpass CSV uses tab delimiter

        for row in reader:
            try:
                # Ensure essential keys are present
                if '@type' not in row or '@id' not in row or 'gnis:feature_id' not in row:
                    logging.warning(f"Skipping CSV row due to missing required columns: {row}")
                    continue

                elements.append({
                    'type': row['@type'],
                    'id': int(row['@id']), # OSM IDs are integers
                    'tags': {'gnis:feature_id': row['gnis:feature_id']}
                })
            except ValueError as ve: # Catch errors like non-integer ID
                logging.warning(f"Skipping CSV row due to invalid data: {row} - Error: {ve}")
                continue
        return elements
    except requests.exceptions.Timeout:
        raise OverpassTimeoutError("Timeout fetching data from Overpass API.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from Overpass API: {e}")
        return []
    except csv.Error as e:
        logging.error(f"Failed to parse CSV from Overpass API response: {e}")
        return []
    except Exception as e: # Catch any other unexpected errors
        logging.error(f"An unexpected error occurred while processing Overpass data: {e}")
        return []

async def find_wikidata_entry_by_gnis_id(session, gnis_id, max_retries=3):
    for attempt in range(max_retries):
        try:
            async with session.get(WIKIDATA_SPARQL_URL, params={'query': SPARQL_QUERY.format(gnis_id=gnis_id), 'format': 'json'}) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('results', {}).get('bindings'):
                        return data['results']['bindings'][0]['item']['value'].split('/')[-1]
                elif response.status == 429:
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        wait_time = int(retry_after)
                        logging.warning(f"Rate limited for GNIS ID {gnis_id}, retrying after {wait_time} seconds...")
                        await asyncio.sleep(wait_time)
                    else:
                        logging.error(f"Rate limited for GNIS ID {gnis_id}, but no Retry-After header provided.")
                        break
                else:
                    logging.error(f"Error fetching Wikidata entry for GNIS ID {gnis_id}: HTTP {response.status}")
                    break
        except aiohttp.ClientError as e:
            logging.error(f"aiohttp error for GNIS ID {gnis_id}: {e}")
            break
        except asyncio.TimeoutError:
            logging.error(f"Timeout error for GNIS ID {gnis_id}")
            break
        except json.JSONDecodeError:
            logging.error(f"JSON decode error for GNIS ID {gnis_id}")
            break
    return None

# Sentinel value for terminating the logging queue.
LOGGING_SENTINEL = object()

async def log_results_sequentially(results_queue, total_features_to_log):
    """Consumes results from a queue, buffers, and logs them in sequential order."""
    buffered_results = {}
    next_expected_log_index = 0
    logged_items_count = 0

    # Process items until all expected logs are done or termination.
    while logged_items_count < total_features_to_log:
        try:
            result = await results_queue.get() # Wait for an item.
        except asyncio.CancelledError:
            logging.info("Logging task cancelled, attempting to flush buffer.")
            break # Exit to flush buffer.

        if result is LOGGING_SENTINEL:
            results_queue.task_done() # Acknowledge sentinel.
            break # Producers are done; exit to flush buffer.

        if result: # Should be a dict unless a different sentinel is used.
            buffered_results[result['original_index']] = result

        results_queue.task_done() # Mark queue item as processed (buffered).

        # Log contiguous items from buffer.
        while next_expected_log_index in buffered_results:
            item_to_log = buffered_results.pop(next_expected_log_index)

            log_line = f"Item {item_to_log['original_index'] + 1}/{total_features_to_log}: {item_to_log['status']} Wikidata for GNIS ID {item_to_log['gnis_id']}"
            if item_to_log['wikidata_id'] and item_to_log['status'] == "Found":
                log_line += f": {item_to_log['wikidata_id']}"

            logging.info(log_line)
            next_expected_log_index += 1
            logged_items_count +=1

    # Flush any remaining out-of-order items after loop termination.
    if buffered_results:
        logging.debug(f"Flushing {len(buffered_results)} remaining buffered items from logging queue...")
        for index in sorted(buffered_results.keys()): # Process sorted by original index.
            if index >= next_expected_log_index: # Ensure no re-logging.
                item_to_log = buffered_results.pop(index)
                log_line = f"Item {item_to_log['original_index'] + 1}/{total_features_to_log}: {item_to_log['status']} Wikidata for GNIS ID {item_to_log['gnis_id']}"
                if item_to_log['wikidata_id'] and item_to_log['status'] == "Found":
                    log_line += f": {item_to_log['wikidata_id']}"
                logging.info(log_line)
                logged_items_count +=1

    # Final status.
    if logged_items_count < total_features_to_log and total_features_to_log > 0:
         logging.warning(f"Logging task finished, but only {logged_items_count}/{total_features_to_log} items were logged.")
    elif total_features_to_log == 0:
        logging.info("Logging task initiated for zero features; nothing to log.")
    else:
        logging.info(f"Logging task completed. All {logged_items_count}/{total_features_to_log} items logged.")


async def process_features_concurrently(features_to_check, session, master_results_list, results_lock, concurrency_limit=5):
    """Concurrently processes features, logs sequentially, updates master_results_list."""
    if not features_to_check:
        logging.info("No features for Wikidata lookup in this batch.")
        return 0

    results_queue = asyncio.Queue()
    total_features = len(features_to_check)

    logging_task = asyncio.create_task(
        log_results_sequentially(results_queue, total_features)
    )

    semaphore = asyncio.Semaphore(concurrency_limit)
    initial_master_results_len = len(master_results_list)

    async def process_feature(feature, original_index, queue_to_put_on, http_session, current_master_results_list, current_results_lock):
        gnis_id = feature['tags']['gnis:feature_id']
        osm_type = feature['type']
        osm_id = feature['id']
        wikidata_id = None
        status = "Not found"

        try:
            async with semaphore: # Limit concurrent API calls.
                wikidata_id = await find_wikidata_entry_by_gnis_id(http_session, gnis_id)

            if wikidata_id:
                status = "Found"
                feature_data = { # Data for saving.
                    'osm_type': osm_type,
                    'osm_id': osm_id,
                    'gnis_id': gnis_id,
                    'wikidata_id': wikidata_id
                }
                async with results_lock: # Safely append to shared list.
                    master_results_list.append(feature_data)

        except Exception as e:
            logging.error(f"Exception during Wikidata lookup for GNIS ID {gnis_id} (idx: {original_index}): {e}", exc_info=False)
            status = f"Error during lookup: {type(e).__name__}"

        # Data for logging queue.
        result_dict_for_logging = {
            'original_index': original_index,
            'gnis_id': gnis_id,
            'wikidata_id': wikidata_id,
            'osm_type': osm_type,
            'osm_id': osm_id,
            'status': status
        }

        await queue_to_put_on.put(result_dict_for_logging)
        # Returned dict is not strictly needed by caller if master_results_list is primary output.
        return result_dict_for_logging

    # Create and gather processing tasks.
    feature_processing_tasks = [
        process_feature(feature, idx, results_queue, session, master_results_list, results_lock)
        for idx, feature in enumerate(features_to_check)
    ]

    gathered_logging_dicts = await asyncio.gather(*feature_processing_tasks, return_exceptions=True)

    await results_queue.put(LOGGING_SENTINEL) # Signal logging task completion.

    try:
        await logging_task # Ensure logs are flushed.
    except asyncio.CancelledError:
        logging.info("Logging task was cancelled during shutdown of process_features_concurrently.")

    final_master_results_len = len(master_results_list)
    newly_added_count = final_master_results_len - initial_master_results_len

    # Log details for tasks that failed with an unhandled exception.
    for i, item_output in enumerate(gathered_logging_dicts):
        if isinstance(item_output, Exception):
            original_feature = features_to_check[i]
            gnis_id_for_error = original_feature.get('tags', {}).get('gnis:feature_id', f'Unknown_at_idx_{i}')
            logging.error(f"Task for GNIS ID {gnis_id_for_error} (original index {i}) failed: {item_output}", exc_info=False)
            # Note: process_feature itself catches and logs lookup errors.
            # This handles unexpected crashes within process_feature's structure.

    return newly_added_count


def fetch_raw_osm_data(query_timeout):
    """Fetches raw OSM data from the Overpass API."""
    logging.info("Attempting to fetch new data from Overpass API. This may take some time...")
    try:
        return fetch_osm_features_with_gnis_id(query_timeout)
    except OverpassTimeoutError as e:
        logging.error(str(e))
        print(f"\nERROR: The Overpass API query timed out. Timeout: {query_timeout}s.")
        print("Consider increasing the timeout using the --timeout option.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred during Overpass API fetch: {e}")
        sys.exit(1)

def prepare_osm_data(raw_osm_data):
    """Handles deduplication and filtering of raw OSM data."""
    if not raw_osm_data:
        logging.info("No raw OSM data to prepare. Returning empty.")
        return [], None, None

    initial_total_features = len(raw_osm_data)

    # Deduplication Stage 1: Purge OSM features if their GNIS ID is used by multiple OSM features.
    gnis_id_counts = collections.Counter(
        f['tags']['gnis:feature_id'] for f in raw_osm_data if f.get('tags', {}).get('gnis:feature_id')
    )
    gnis_ids_to_purge_shared = {gnis_id for gnis_id, count in gnis_id_counts.items() if count > 1}
    purged_features_shared_gnis_json = []
    temp_features_after_shared_gnis_purge = []

    for feature in raw_osm_data:
        gnis_id = feature.get('tags', {}).get('gnis:feature_id')
        if gnis_id and gnis_id in gnis_ids_to_purge_shared:
            purged_features_shared_gnis_json.append(feature)
        else:
            temp_features_after_shared_gnis_purge.append(feature)

    count_purged_due_to_shared_gnis = len(purged_features_shared_gnis_json)
    if gnis_ids_to_purge_shared:
        logging.info(f"Identified {len(gnis_ids_to_purge_shared)} GNIS IDs used by multiple OSM features. "
                     f"{count_purged_due_to_shared_gnis} OSM features will be purged.")
        save_data_atomically(
            purged_features_shared_gnis_json,
            PURGED_SHARED_GNIS_FILENAME,
            "purged features (shared GNIS)"
        )

    # Deduplication Stage 2: Purge features whose 'gnis:feature_id' tag contains multiple IDs.
    features_with_multiple_ids_in_tag_list = []
    single_gnis_id_features_list = []

    for feature in temp_features_after_shared_gnis_purge:
        gnis_id_value = feature['tags']['gnis:feature_id']
        if ';' in gnis_id_value:
            features_with_multiple_ids_in_tag_list.append(feature)
        else:
            single_gnis_id_features_list.append(feature)

    count_purged_due_to_multiple_ids_in_tag = len(features_with_multiple_ids_in_tag_list)
    if features_with_multiple_ids_in_tag_list:
        logging.info(f"Found {count_purged_due_to_multiple_ids_in_tag} features containing multiple GNIS IDs in their tag value; these will be purged.")
        save_data_atomically(
            features_with_multiple_ids_in_tag_list,
            PURGED_MULTI_ID_FILENAME,
            "features with multiple GNIS IDs in tag"
        )

    features_for_processing_this_run = single_gnis_id_features_list

    logging.info("--- Data Preparation Summary ---")
    logging.info(f"Initial features from Overpass source: {initial_total_features}")
    percent_purged_shared = (count_purged_due_to_shared_gnis / initial_total_features * 100) if initial_total_features > 0 else 0
    logging.info(f"Purged (GNIS ID used by multiple OSM features): {count_purged_due_to_shared_gnis} ({percent_purged_shared:.2f}%)")
    percent_purged_multi_tag = (count_purged_due_to_multiple_ids_in_tag / initial_total_features * 100) if initial_total_features > 0 else 0
    logging.info(f"Purged (feature tag contains multiple GNIS IDs): {count_purged_due_to_multiple_ids_in_tag} ({percent_purged_multi_tag:.2f}%)")
    logging.info(f"Features remaining after all purges: {len(single_gnis_id_features_list)}")
    logging.info(f"Net features for Wikidata lookup this run: {len(features_for_processing_this_run)}")
    logging.info("---------------------------------")

    return (
        features_for_processing_this_run,
        purged_features_shared_gnis_json,
        features_with_multiple_ids_in_tag_list,
    )


async def process_wikidata_lookups(features_to_process_list, master_results_list):
    """Processes features for Wikidata entries; `master_results_list` is updated in-place."""
    if not features_to_process_list:
        logging.info("No features provided for Wikidata lookup in this batch.")
        return master_results_list # Return the list as is.

    results_lock = asyncio.Lock() # Lock for safe concurrent appends to master_results_list.
    headers = {'User-Agent': 'OSM-Wikidata-Updater/1.0 (your.email@example.com; find_osm_features.py)'}

    async with aiohttp.ClientSession(headers=headers) as session:
        # process_features_concurrently updates master_results_list directly
        # and returns the count of newly added items.
        count_newly_added = await process_features_concurrently(
            features_to_process_list,
            session,
            master_results_list, # Passed by reference, updated in place.
            results_lock,
            concurrency_limit=CONCURRENCY_LIMIT # Use the constant
        )

        if count_newly_added > 0:
            logging.info(f"Wikidata lookup phase added {count_newly_added} new results to the main list.")
        else:
            logging.info("Wikidata lookup phase completed for this batch, no new results added.")
    return master_results_list # Return the (potentially modified) list.

async def save_final_results_and_cleanup(final_results_list, purged_shared, purged_multi_id):
    """Saves final results and purged features to JSON files."""
    if final_results_list:
        try:
            # Sort results for consistent output, helpful for diffs and review.
            sorted_results = sorted(final_results_list, key=lambda x: (x.get('osm_id', 0), x.get('gnis_id', '')))
            with open(FINAL_RESULTS_FILENAME, 'w', encoding='utf-8') as f:
                json.dump(sorted_results, f, indent=2)
            logging.info(f"Saved {len(sorted_results)} total features to {FINAL_RESULTS_FILENAME}")
        except IOError as e:
            logging.error(f"Error saving final results to JSON: {e}")
    else: # No results to save.
        logging.info("No features with matching Wikidata entries found after processing.")

    # Consolidate saving of purged feature files.
    purged_data_to_save = [
        (purged_shared, PURGED_SHARED_GNIS_FILENAME, 'purged features (shared GNIS)'),
        (purged_multi_id, PURGED_MULTI_ID_FILENAME, 'purged features (multiple GNIS IDs)')
    ]

    for data, filename, context in purged_data_to_save:
        if data:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
                logging.info(f"Saved {len(data)} {context} to {filename}")
            except IOError as e:
                logging.error(f"Error saving {context} to {filename}: {e}")


async def main_async(query_timeout):
    """Main asynchronous function for the OSM feature processing workflow."""
    logging.info("Starting main asynchronous execution.")

    # Step 1: Fetch raw data
    raw_osm_data = fetch_raw_osm_data(query_timeout)

    # Step 2: Prepare data (filter, deduplicate)
    (
        features_for_processing_this_run,
        purged_shared,
        purged_multi_id,
    ) = prepare_osm_data(raw_osm_data)


    # Early exit if no features to process.
    if not features_for_processing_this_run:
        logging.info("No features to process this run. Exiting.")
        return

    # Step 3: Process Wikidata Lookups.
    results = await process_wikidata_lookups(
        features_for_processing_this_run,
        [] # Start with an empty list of results.
    )

    # Step 4: Save final results.
    await save_final_results_and_cleanup(results, purged_shared, purged_multi_id)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch OSM features with GNIS IDs and find corresponding Wikidata entries.")
    parser.add_argument(
        "--timeout",
        type=int,
        default=None, # Handled by prompt or default value if not given.
        help="Timeout in seconds for Overpass API query. Prompts if not set."
    )
    args = parser.parse_args()

    effective_timeout = args.timeout
    DEFAULT_TIMEOUT = 10000

    if effective_timeout is None:
        try:
            user_input_timeout_str = input(f"Enter Overpass API timeout in seconds (e.g., {DEFAULT_TIMEOUT}, Enter for default {DEFAULT_TIMEOUT}s): ").strip()
            if not user_input_timeout_str: # User pressed Enter.
                effective_timeout = DEFAULT_TIMEOUT
            else:
                effective_timeout = int(user_input_timeout_str)
                if effective_timeout <= 0:
                    logging.warning(f"Timeout must be positive. Using default {DEFAULT_TIMEOUT}s.")
                    effective_timeout = DEFAULT_TIMEOUT
                else:
                    logging.info(f"User-defined Overpass API timeout: {effective_timeout}s.")
        except ValueError: # Non-integer input.
            logging.warning(f"Invalid timeout input. Using default {DEFAULT_TIMEOUT}s.")
            effective_timeout = DEFAULT_TIMEOUT
    else: # Timeout provided via CLI.
        if effective_timeout <= 0:
            logging.warning(f"CLI timeout --timeout {args.timeout} not positive. Using default {DEFAULT_TIMEOUT}s.")
            effective_timeout = DEFAULT_TIMEOUT
        else:
            logging.info(f"Using Overpass API timeout from CLI: {effective_timeout}s.")

    logging.info(f"Overpass API timeout for this session: {effective_timeout}s.")

    start_time = time.time()
    asyncio.run(main_async(effective_timeout)) # Run the main async workflow.
    end_time = time.time()
    logging.info(f"Total execution time: {end_time - start_time:.2f}s.")
    logging.info("Processing complete. It is safe to exit the terminal.")
