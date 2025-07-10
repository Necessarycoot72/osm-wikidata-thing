import asyncio
import aiohttp
import requests
import logging
import json
import os
import argparse
import time
import signal
import sys
import collections
import pickle
import gzip
import csv
import io

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variables for managing state, primarily for signal handling and resume capabilities.
# current_features_to_check_for_resume: List of features that define the current processing scope for resume purposes.
current_features_to_check_for_resume = []
# current_results_for_resume: List of results accumulated so far, including from loaded state.
current_results_for_resume = []
# global_raw_overpass_data: Cache of the raw data fetched from Overpass, used for resuming if user chooses.
global_raw_overpass_data = None

# Custom Exception
class OverpassTimeoutError(Exception):
    pass

# Constants
OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
BASE_OVERPASS_QUERY = """
[out:csv(::type, ::id, "gnis:feature_id"; true; "\\t")][timeout:{timeout}];
(
  nwr["gnis:feature_id"][!"wikidata"];
);
out;
"""
SPARQL_QUERY = """
SELECT ?item WHERE {{
  ?item wdt:P590 "{gnis_id}" .
}}
"""

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
        csvfile = io.StringIO(csv_text) # Treat CSV string as a file for csv.DictReader
        # Overpass CSV uses tab delimiter by default. Expects headers like '@type', '@id'.
        reader = csv.DictReader(csvfile, delimiter='\t')

        for row in reader:
            try:
                # Validate essential keys before processing the row.
                if '@type' not in row or '@id' not in row or 'gnis:feature_id' not in row:
                    logging.warning(f"Skipping CSV row due to missing required columns: {row}")
                    continue

                elements.append({
                    'type': row['@type'],
                    'id': int(row['@id']), # OSM IDs must be integers.
                    'tags': {'gnis:feature_id': row['gnis:feature_id']}
                })
            except ValueError as ve:
                logging.warning(f"Skipping CSV row due to invalid data (e.g., non-integer ID): {row} - Error: {ve}")
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
    except Exception as e: # Catch any other unexpected errors during CSV processing.
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

def _save_current_progress(raw_overpass_data_cache, features_to_check, results_to_save):
    """Core logic for saving current progress to a compressed pickle file."""
    target_file = 'resume_state.pkl.gz'
    logging.info(f"Attempting to save current progress to {target_file}...")

    if results_to_save:
        logging.info("Items being saved to resume state:") # Clarified context
        for item in results_to_save:
            osm_id = item.get('osm_id', 'N/A')
            gnis_id = item.get('gnis_id', 'N/A')
            logging.info(f"  - OSM ID: {osm_id}, GNIS ID: {gnis_id}")
    else:
        logging.info("No new results to add to resume state at this time.") # More specific

    temp_file = f"{target_file}.tmp"

    data_to_save = {
        'raw_overpass_data_cache': raw_overpass_data_cache,
        'features_to_check': features_to_check,
        'results': results_to_save
    }

    # Phase 1: Write to temporary file
    try:
        with gzip.open(temp_file, 'wb') as f:
            pickle.dump(data_to_save, f)
    except Exception as e: # Catch issues during temp file write (I/O or pickling).
        logging.error(f"Failed to write progress to temporary file {temp_file}: {e}")
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
                logging.info(f"Cleaned up temporary file {temp_file} after write failure.")
            except OSError as oe:
                logging.error(f"Failed to remove temporary file {temp_file} after write failure: {oe}")
        return # Abort save if temp file write fails.

    # Phase 2: Atomically replace the target file with the temporary file.
    try:
        os.replace(temp_file, target_file) # This is generally atomic.
        logging.info(f"Successfully saved progress to {target_file}: {len(results_to_save)} results, {len(features_to_check)} features to check. It is now safe to close the terminal.")
    except OSError as e: # Catch errors during file replacement.
        logging.error(f"Failed to replace {target_file} with {temp_file}: {e}")
        if os.path.exists(temp_file): # Attempt to clean up temp file if replacement failed.
            try:
                os.remove(temp_file)
                logging.info(f"Cleaned up temporary file {temp_file} after replace failure.")
            except OSError as oe:
                logging.error(f"Failed to remove temporary file {temp_file} after replace failure: {oe}")
        return # Abort if replacement fails.

# Signal handler for SIGINT (Ctrl+C)
def sigint_handler(signum, frame):
    """Handles SIGINT signal (Ctrl+C) by saving progress and exiting."""
    logging.info(f"SIGINT (Ctrl+C) received (signal {signum}). Attempting to save progress before exiting...")
    # Access global state variables for saving.
    _save_current_progress(global_raw_overpass_data, current_features_to_check_for_resume, current_results_for_resume)
    sys.exit(0) # Attempt to exit gracefully after saving.

def setup_signal_handlers():
    """Registers signal handlers for graceful shutdown."""
    signal.signal(signal.SIGINT, sigint_handler)
    # TODO: Register other signal handlers like SIGHUP or SIGTERM here if needed.

# Note: Old Windows-specific console event handler code and related imports (PYWIN32_AVAILABLE, IS_WINDOWS, win32api, win32con)
# have been previously removed as the solution now focuses on standard signal handling.

# Sentinel value for queue termination
LOGGING_SENTINEL = object()

async def log_results_sequentially(results_queue, total_features_to_log):
    """
    Consumes results from a queue, buffers them, and logs them in sequential order.
    """
    buffered_results = {}
    next_expected_log_index = 0
    logged_items_count = 0 # Tracks items logged for completion stats.

    # Loop until all expected items are logged or termination is signaled.
    while logged_items_count < total_features_to_log:
        try:
            result = await results_queue.get() # Wait for an item from producers.
        except asyncio.CancelledError:
            logging.info("Logging task cancelled, attempting to flush buffer.")
            break # Exit loop to flush buffer.

        if result is LOGGING_SENTINEL:
            results_queue.task_done() # Acknowledge sentinel.
            break # Producers are done; exit loop to flush buffer.

        if result: # Should always be a dict unless a None sentinel is used differently
            buffered_results[result['original_index']] = result

        results_queue.task_done() # Indicate this queue item has been processed (buffered).

        # Log all available contiguous items from the buffer.
        while next_expected_log_index in buffered_results:
            item_to_log = buffered_results.pop(next_expected_log_index)

            log_line = f"Item {item_to_log['original_index'] + 1}/{total_features_to_log}: {item_to_log['status']} Wikidata for GNIS ID {item_to_log['gnis_id']}"
            if item_to_log['wikidata_id'] and item_to_log['status'] == "Found":
                log_line += f": {item_to_log['wikidata_id']}"

            logging.info(log_line)
            next_expected_log_index += 1
            logged_items_count +=1

    # After loop (sentinel or cancellation), flush any remaining out-of-order items.
    if buffered_results:
        logging.debug(f"Flushing {len(buffered_results)} remaining buffered items from logging queue...")
        for index in sorted(buffered_results.keys()): # Process sorted by original index.
            # This check 'if index >= next_expected_log_index' might be redundant
            # if pop always removes, but safe for ensuring no re-logging.
            if index >= next_expected_log_index:
                item_to_log = buffered_results.pop(index)
                log_line = f"Item {item_to_log['original_index'] + 1}/{total_features_to_log}: {item_to_log['status']} Wikidata for GNIS ID {item_to_log['gnis_id']}"
                if item_to_log['wikidata_id'] and item_to_log['status'] == "Found":
                    log_line += f": {item_to_log['wikidata_id']}"
                logging.info(log_line)
                logged_items_count +=1

    # Final status logging for this task.
    if logged_items_count < total_features_to_log and total_features_to_log > 0:
         logging.warning(f"Logging task finished, but only {logged_items_count}/{total_features_to_log} items were logged. This might indicate an issue with producers or sentinel handling.")
    elif total_features_to_log == 0:
        logging.info("Logging task initiated for zero features; nothing to log.")
    else:
        logging.info(f"Logging task completed. All {logged_items_count}/{total_features_to_log} items logged.")


def load_progress():
    """Loads progress from resume_state.pkl.gz if it exists."""
    target_file = 'resume_state.pkl.gz'
    if os.path.exists(target_file):
        try:
            with gzip.open(target_file, 'rb') as f:
                data = pickle.load(f)
            loaded_raw_overpass = data.get('raw_overpass_data_cache', None)
            loaded_features = data.get('features_to_check', [])
            loaded_results = data.get('results', [])

            log_message = f"Progress loaded from {target_file}: {len(loaded_results)} results, {len(loaded_features)} features to potentially check."
            if loaded_raw_overpass is not None:
                log_message += " Found cached raw Overpass data."
            else:
                log_message += " No cached raw Overpass data found."
            logging.info(log_message)

            if not isinstance(loaded_features, list) or not isinstance(loaded_results, list):
                logging.error(f"Invalid data format in {target_file} for features_to_check or results. Expected lists.")
                os.remove(target_file)
                logging.info(f"Corrupted or invalid {target_file} removed.")
                return None, [], []
            return loaded_raw_overpass, loaded_features, loaded_results
        except (IOError, pickle.UnpicklingError, EOFError, AttributeError, ImportError, IndexError) as e: # Catch common unpickling issues.
            logging.error(f"Error loading or parsing {target_file}: {e}. The file might be corrupted or unreadable.")
            try:
                os.remove(target_file) # Attempt to remove the corrupted file.
                logging.info(f"Removed corrupted or unreadable {target_file}.")
            except OSError as oe:
                logging.error(f"Error removing corrupted {target_file} after load failure: {oe}")
            return None, [], []
    else:
        logging.info(f"No {target_file} found. Starting a fresh session.")
        return None, [], []

async def process_features_concurrently(features_to_check, session, master_results_list, results_lock, concurrency_limit=5):
    """Concurrently processes features for Wikidata entries, logs sequentially, and updates master_results_list."""
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
        # feature is {'type': ..., 'id': ..., 'tags': {'gnis:feature_id': ...}}
        gnis_id = feature['tags']['gnis:feature_id']
        # Name is no longer available/needed.
        osm_type = feature['type']
        osm_id = feature['id']
        wikidata_id = None
        status = "Not found"

        try:
            async with semaphore: # Limit concurrent external API calls.
                wikidata_id = await find_wikidata_entry_by_gnis_id(http_session, gnis_id)

            if wikidata_id:
                status = "Found"
                # Add successfully matched feature data to the shared list for saving.
                feature_data = {
                    'osm_type': osm_type,
                    'osm_id': osm_id,
                    # 'name': name, # Name field removed
                    'gnis_id': gnis_id,
                    'wikidata_id': wikidata_id
                }
                async with results_lock:
                    master_results_list.append(feature_data)

        except Exception as e:
            logging.error(f"Exception during Wikidata lookup for GNIS ID {gnis_id} (idx: {original_index}): {e}", exc_info=False)
            status = f"Error during lookup: {type(e).__name__}"
            # wikidata_id remains None.

        # Create dict for the logging queue.
        result_dict_for_logging = {
            'original_index': original_index,
            'gnis_id': gnis_id,
            'wikidata_id': wikidata_id,
            # 'name': name, # Name field removed
            'osm_type': osm_type,
            'osm_id': osm_id,
            'status': status
        }

        await queue_to_put_on.put(result_dict_for_logging)
        return result_dict_for_logging # Returned dict can be used for other post-processing if needed.

    # Create tasks, passing shared lists/locks for result accumulation and logging.
    feature_processing_tasks = [
        process_feature(feature, idx, results_queue, session, master_results_list, results_lock)
        for idx, feature in enumerate(features_to_check)
    ]

    # Gather logging dicts; actual results are added to master_results_list directly.
    gathered_logging_dicts = await asyncio.gather(*feature_processing_tasks, return_exceptions=True)

    await results_queue.put(LOGGING_SENTINEL) # Signal logging task that producers are done.

    try:
        await logging_task # Ensure all logs are flushed.
    except asyncio.CancelledError:
        logging.info("Logging task was cancelled during shutdown of process_features_concurrently.")

    final_master_results_len = len(master_results_list)
    newly_added_count = final_master_results_len - initial_master_results_len

    # Log details for any tasks that failed with an unhandled exception.
    for i, item_output in enumerate(gathered_logging_dicts):
        if isinstance(item_output, Exception):
            original_feature = features_to_check[i]
            gnis_id_for_error = original_feature.get('tags', {}).get('gnis:feature_id', f'Unknown_at_original_index_{i}')
            logging.error(f"Task for GNIS ID {gnis_id_for_error} (original index {i}) failed with unhandled exception: {item_output}", exc_info=False)
            # Note: if process_feature crashes before putting to queue, the logger won't know about this item.
            # The total_features_to_log for the logger might seem off by one in such rare cases if not handled.
            # However, process_feature is designed to catch its own lookup errors and return a dict.
            # This handles crashes within process_feature itself.

    return newly_added_count

# Note: The generate_paginated_html_report function was previously removed.

async def handle_resume_data_loading(input_current_results_for_resume, input_global_raw_overpass_data):
    """Loads progress from file and merges with any initial state.
    Returns state elements: processed IDs, pending features, and updated result/data caches."""

    loaded_raw_overpass, loaded_features, loaded_results = load_progress()

    # Prioritize loaded data; otherwise, use the initial state passed to the function.
    output_current_results_for_resume = loaded_results if loaded_results is not None else (input_current_results_for_resume or [])
    output_global_raw_overpass_data = loaded_raw_overpass if loaded_raw_overpass is not None else input_global_raw_overpass_data

    processed_gnis_ids = {res['gnis_id'] for res in output_current_results_for_resume if 'gnis_id' in res} # Set of GNIS IDs already processed.

    pending_features_from_load = []
    current_scope_for_resume_from_load = []

    if loaded_features:
        pending_features_from_load = [
            feature for feature in loaded_features
            if feature.get('tags', {}).get('gnis:feature_id') not in processed_gnis_ids
        ]
        if pending_features_from_load:
            logging.info(f"Found {len(pending_features_from_load)} pending features from loaded resume scope.")
            current_scope_for_resume_from_load = loaded_features
        else:
            logging.info("All features from loaded processing scope were already processed.")
            current_scope_for_resume_from_load = []

    return (processed_gnis_ids, pending_features_from_load, current_scope_for_resume_from_load,
            output_current_results_for_resume, output_global_raw_overpass_data)


async def fetch_and_prepare_osm_data(query_timeout, processed_gnis_ids_from_loaded_results, current_global_raw_overpass_data_state):
    """Fetches new OSM data (if not using cache), handles cache logic, and purges/filters features.
    Returns:
        - features_for_processing_this_run: List of features to be processed.
        - raw_data_defining_current_scope: Full list of features for the current scope (for resume).
        - effective_raw_overpass_cache: Updated raw Overpass data cache.
    """
    effective_raw_overpass_cache = current_global_raw_overpass_data_state
    source_of_new_raw_data = None
    initial_total_features = 0
    count_purged_due_to_shared_gnis = 0

    attempt_live_api_fetch = True
    # raw_data_defining_current_scope is the full set of features for this run, used for resume state.
    raw_data_defining_current_scope = effective_raw_overpass_cache

    if effective_raw_overpass_cache: # Check if cached raw data from a previous session exists.
        try:
            user_choice_cache = input(
                "Cached raw Overpass data found. Use this (y) or fetch fresh data (n)? [y/n]: "
            ).strip().lower()
            if user_choice_cache == 'y':
                source_of_new_raw_data = effective_raw_overpass_cache
                attempt_live_api_fetch = False
                logging.info("User opted to use cached raw Overpass data.")
            elif user_choice_cache == 'n':
                logging.info("User opted to fetch fresh Overpass data. Discarding previous cache.")
                effective_raw_overpass_cache = None
                raw_data_defining_current_scope = None
            else:
                logging.warning("Invalid choice for cached data. Defaulting to fetching fresh data.")
                effective_raw_overpass_cache = None
                raw_data_defining_current_scope = None
        except Exception as e: # Handle errors during input() if non-interactive.
            logging.warning(f"Could not get user input for cached Overpass data. Defaulting to fetching fresh. Error: {e}")
            effective_raw_overpass_cache = None
            raw_data_defining_current_scope = None

        if effective_raw_overpass_cache is None: # Ensure consistency if cache was discarded.
             raw_data_defining_current_scope = None

    if attempt_live_api_fetch:
        logging.info("Attempting to fetch new data from Overpass API. This may take some time")
        try:
            fetched_data_elements = fetch_osm_features_with_gnis_id(query_timeout)
            effective_raw_overpass_cache = fetched_data_elements
            source_of_new_raw_data = fetched_data_elements
            raw_data_defining_current_scope = fetched_data_elements
        except OverpassTimeoutError as e:
            logging.error(str(e))
            print(f"\nERROR: The Overpass API query timed out. Timeout: {query_timeout}s.")
            print("Consider increasing the timeout using the --timeout option.")
            sys.exit(1)
        except Exception as e:
            logging.error(f"An unexpected error occurred during Overpass API fetch: {e}")
            sys.exit(1)

    if not source_of_new_raw_data:
        logging.info("No Overpass data sourced (from cache or live fetch).")
        return [], [], effective_raw_overpass_cache # Return empty if no data.

    initial_total_features = len(source_of_new_raw_data)
    # This log was here: logging.info(f"Initial features from Overpass data source: {initial_total_features}")
    # It will be part of the summary log later.

    # Deduplication Stage 1: Purge OSM features if their GNIS ID is used by multiple OSM features.
    gnis_id_counts = collections.Counter()
    for feature in source_of_new_raw_data:
        gnis_id = feature.get('tags', {}).get('gnis:feature_id')
        if gnis_id:
            gnis_id_counts[gnis_id] += 1

    gnis_ids_to_purge_shared = {gnis_id for gnis_id, count in gnis_id_counts.items() if count > 1}
    purged_features_shared_gnis_json = []
    temp_features_after_shared_gnis_purge = []

    for feature in source_of_new_raw_data:
        gnis_id = feature.get('tags', {}).get('gnis:feature_id')
        if gnis_id and gnis_id in gnis_ids_to_purge_shared:
            purged_features_shared_gnis_json.append(feature)
        else:
            temp_features_after_shared_gnis_purge.append(feature)

    if gnis_ids_to_purge_shared:
        logging.info(f"Identified {len(gnis_ids_to_purge_shared)} GNIS IDs used by multiple OSM features. These {len(purged_features_shared_gnis_json)} OSM features will be purged.")
        # logging.info(f"GNIS IDs purged (shared): {', '.join(sorted(list(gnis_ids_to_purge_shared)))}") # Potentially very long list
        if purged_features_shared_gnis_json:
            target_purged_file = 'purged_duplicate_gnis_features.json'
            temp_purged_file = f"{target_purged_file}.tmp"
            try:
                with open(temp_purged_file, 'w', encoding='utf-8') as f:
                    json.dump(purged_features_shared_gnis_json, f, indent=2)
                os.replace(temp_purged_file, target_purged_file)
                logging.info(f"Saved {len(purged_features_shared_gnis_json)} purged features (shared GNIS) to {target_purged_file}.")
            except Exception as e:
                logging.error(f"Error saving purged (shared GNIS) features to {target_purged_file}: {e}")
                if os.path.exists(temp_purged_file): os.remove(temp_purged_file)
        count_purged_due_to_shared_gnis = len(purged_features_shared_gnis_json)

    # Deduplication Stage 2: Expunge features that themselves contain multiple GNIS IDs in their tag.
    features_with_multiple_ids_in_tag_list = []
    single_gnis_id_features_list = []

    for feature in temp_features_after_shared_gnis_purge: # Operate on features that passed the first purge.
        gnis_id_value = feature['tags']['gnis:feature_id']
        if ';' in gnis_id_value:
            features_with_multiple_ids_in_tag_list.append(feature)
        else:
            single_gnis_id_features_list.append(feature)

    count_purged_due_to_multiple_ids_in_tag = len(features_with_multiple_ids_in_tag_list)

    if features_with_multiple_ids_in_tag_list:
        target_multi_id_file = 'gnis_ids_on_multiple_features.json'
        temp_multi_id_file = f"{target_multi_id_file}.tmp"
        logging.info(f"Found {count_purged_due_to_multiple_ids_in_tag} features containing multiple GNIS IDs in their tag value.")
        try:
            with open(temp_multi_id_file, 'w', encoding='utf-8') as f:
                json.dump(features_with_multiple_ids_in_tag_list, f, indent=2)
            os.replace(temp_multi_id_file, target_multi_id_file)
            logging.info(f"Saved {count_purged_due_to_multiple_ids_in_tag} features with multiple GNIS IDs in tag to {target_multi_id_file}.")
        except Exception as e:
            logging.error(f"Error saving features with multiple GNIS IDs in tag to {target_multi_id_file}: {e}")
            if os.path.exists(temp_multi_id_file): os.remove(temp_multi_id_file)

    # Final Filtering: Remove features already processed (from loaded resume state).
    # This operates on features that have a single, non-shared GNIS ID.
    features_for_processing_this_run = [
        feature for feature in single_gnis_id_features_list
        if feature['tags']['gnis:feature_id'] not in processed_gnis_ids_from_loaded_results
    ]
    count_filtered_already_processed = len(single_gnis_id_features_list) - len(features_for_processing_this_run)

    # Enhanced Summary Logging
    logging.info("--- Data Preparation Summary ---")
    logging.info(f"Initial features from Overpass source: {initial_total_features}")

    percent_purged_shared_gnis = (count_purged_due_to_shared_gnis / initial_total_features * 100) if initial_total_features > 0 else 0
    logging.info(f"Purged (GNIS ID used by multiple OSM features): {count_purged_due_to_shared_gnis} ({percent_purged_shared_gnis:.2f}%)")

    percent_purged_multiple_ids_in_tag = (count_purged_due_to_multiple_ids_in_tag / initial_total_features * 100) if initial_total_features > 0 else 0
    logging.info(f"Purged (feature tag contains multiple GNIS IDs): {count_purged_due_to_multiple_ids_in_tag} ({percent_purged_multiple_ids_in_tag:.2f}%)")

    logging.info(f"Features remaining after all purges: {len(single_gnis_id_features_list)}")
    logging.info(f"Further filtered (already processed in previous runs): {count_filtered_already_processed}")
    logging.info(f"Net features for Wikidata lookup this run: {len(features_for_processing_this_run)}")
    logging.info("---------------------------------")

    # raw_data_defining_current_scope is the full list from this Overpass fetch/cache choice (for resume).
    # effective_raw_overpass_cache is the actual raw data (list of features) saved in resume_state.
    return features_for_processing_this_run, raw_data_defining_current_scope, effective_raw_overpass_cache


async def process_wikidata_lookups(features_to_process_list, master_results_list):
    """Processes features for Wikidata entries; `master_results_list` is updated in-place."""

    if not features_to_process_list:
        logging.info("No features provided for Wikidata lookup in this batch.")
        return master_results_list

    # master_results_list is passed by reference and modified by process_features_concurrently.
    results_lock = asyncio.Lock() # Lock for safe concurrent appends to master_results_list.

    headers = {'User-Agent': 'OSM-Wikidata-Updater/1.0 (contact: your.email@example.com; script: find_osm_features.py)'}
    async with aiohttp.ClientSession(headers=headers) as session:
        # process_features_concurrently updates master_results_list directly
        # and returns the count of newly added items.
        count_newly_added = await process_features_concurrently(
            features_to_process_list,
            session,
            master_results_list,
            results_lock,
            concurrency_limit=5
        )

        if count_newly_added > 0:
            logging.info(f"Wikidata lookup phase added {count_newly_added} new results to the main list.")
        else:
            logging.info("Wikidata lookup phase completed for this batch, no new results added to the main list.")

    return master_results_list

async def save_final_results_and_cleanup(final_results_list):
    """Saves final results to JSON and removes the resume state file."""
    if final_results_list:
        try:
            sorted_results = sorted(final_results_list, key=lambda x: (x.get('osm_id', 0), x.get('gnis_id', '')))
            with open('osm_features_to_update.json', 'w', encoding='utf-8') as f:
                json.dump(sorted_results, f, indent=2)
            logging.info(f"Saved {len(sorted_results)} total features to osm_features_to_update.json")

            # Successful save, now remove resume state
            resume_file_name = "resume_state.pkl.gz"
            if os.path.exists(resume_file_name):
                try:
                    os.remove(resume_file_name)
                    logging.info(f"Processing completed successfully. Resume state file '{resume_file_name}' removed.")
                except OSError as e:
                    logging.error(f"Error removing {resume_file_name}: {e}")
            else:
                logging.info("Processing completed successfully. No resume state file to remove.")
        except IOError as e:
            logging.error(f"Error saving results to JSON: {e}")
            # Do not proceed to delete resume_file_name if saving results failed
            return # Exit early
    else:
        logging.info("No features with matching Wikidata entries found after processing and loading.")
        # Even if no results, if the script ran fully, clean up.
        resume_file_name = "resume_state.pkl.gz"
        if os.path.exists(resume_file_name):
            try:
                os.remove(resume_file_name)
                logging.info(f"Processing completed (no results found). Resume state file '{resume_file_name}' removed.")
            except OSError as e:
                logging.error(f"Error removing {resume_file_name}: {e}")
        else:
            logging.info("Processing completed (no results found). No resume state file to remove.")


async def main_async(query_timeout):
    """Main asynchronous function to orchestrate the OSM feature processing workflow."""
    # These global variables are updated by main_async and read by the SIGINT handler
    # to save the most recent state upon interruption.
    global current_features_to_check_for_resume
    global current_results_for_resume
    global global_raw_overpass_data

    logging.info("Starting main asynchronous execution.")
    setup_signal_handlers() # Register Ctrl+C handler early.

    # Load existing progress. This updates global state variables based on resume file content.
    processed_gnis_ids, pending_features_from_loaded_scope, loaded_scope_for_resume, \
    current_results_for_resume, global_raw_overpass_data = await handle_resume_data_loading(
        current_results_for_resume, global_raw_overpass_data
    )

    features_for_processing_this_run = []

    if pending_features_from_loaded_scope:
        # If resuming, prioritize pending features from the loaded scope.
        logging.info(f"Resuming with {len(pending_features_from_loaded_scope)} pending features from loaded resume scope.")
        features_for_processing_this_run = pending_features_from_loaded_scope
        # Ensure global state for resume reflects the loaded scope if resuming.
        current_features_to_check_for_resume = loaded_scope_for_resume
        # global_raw_overpass_data is already updated by handle_resume_data_loading.
    else:
        # No pending features from a loaded scope, so fetch new data from Overpass.
        logging.info("No pending features from loaded scope. Attempting to source new Overpass data.")
        new_features_to_process, scope_defining_raw_data, \
        updated_raw_cache = await fetch_and_prepare_osm_data(
            query_timeout,
            processed_gnis_ids,       # Avoid re-processing GNIS IDs found in loaded results.
            global_raw_overpass_data  # Pass current raw Overpass data cache (if any).
        )
        features_for_processing_this_run.extend(new_features_to_process)
        current_features_to_check_for_resume = scope_defining_raw_data # Update global resume scope.
        global_raw_overpass_data = updated_raw_cache               # Update global raw data cache.

    # Early exit if no features to process and no existing results to save.
    if not features_for_processing_this_run and not current_results_for_resume:
        logging.info("No features to process this run and no existing results. Exiting.")
        resume_file_name = "resume_state.pkl.gz"
        if os.path.exists(resume_file_name):
            try:
                os.remove(resume_file_name)
                logging.info(f"Cleaned up {resume_file_name}.")
            except OSError as e:
                logging.error(f"Error removing {resume_file_name}: {e}")
        return

    # Step 3: Process Wikidata Lookups.
    # current_results_for_resume is passed and modified in-place.
    if features_for_processing_this_run:
        current_results_for_resume = await process_wikidata_lookups(
            features_for_processing_this_run,
            current_results_for_resume
        )
    else:
        logging.info("No features to submit for Wikidata lookup this run.")

    # Step 4: Save final results and clean up the resume state file.
    await save_final_results_and_cleanup(current_results_for_resume)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch OSM features with GNIS IDs and check for corresponding Wikidata entries.")
    parser.add_argument(
        "--timeout",
        type=int,
        default=None, # Handled by interactive prompt or hardcoded default if not provided.
        help="Timeout in seconds for the Overpass API query. Prompts if not provided."
    )
    args = parser.parse_args()

    effective_timeout = args.timeout

    if effective_timeout is None:
        try:
            user_input_timeout_str = input("Enter Overpass API timeout in seconds (e.g., 10000, Enter for default 10000s): ").strip()
            if not user_input_timeout_str: # User pressed Enter for default.
                effective_timeout = 10000
            else:
                effective_timeout = int(user_input_timeout_str)
                if effective_timeout <= 0:
                    logging.warning("Timeout must be positive. Using default 10000s.")
                    effective_timeout = 10000
                else:
                    logging.info(f"User-defined Overpass API timeout: {effective_timeout}s.")
        except ValueError: # Handle non-integer input.
            logging.warning("Invalid timeout input. Using default 10000s.")
            effective_timeout = 10000
    else: # Timeout provided via CLI.
        if effective_timeout <= 0:
            logging.warning(f"CLI timeout --timeout {args.timeout} not positive. Using default 10000s.")
            effective_timeout = 10000
        else:
            logging.info(f"Using Overpass API timeout from CLI: {effective_timeout}s.")

    logging.info(f"Overpass API timeout for this session: {effective_timeout}s.")

    start_time = time.time()
    asyncio.run(main_async(effective_timeout)) # Run the main asynchronous workflow.
    end_time = time.time()
    logging.info(f"Total execution time: {end_time - start_time:.2f}s.")
    logging.info("Data saved. It's ok to exit the terminal.")
