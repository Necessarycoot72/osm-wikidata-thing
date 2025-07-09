import asyncio
import aiohttp
import requests
import logging
import json
import os
import re
import argparse
import time
import signal
import sys
import collections

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
[out:json][timeout:{timeout}];
(
  nwr["gnis:feature_id"];
);
out body;
>;
out skel qt;
"""
SPARQL_QUERY = """
SELECT ?item WHERE {{
  ?item wdt:P590 "{gnis_id}" .
}}
"""

def fetch_osm_features_with_gnis_id(user_timeout):
    try:
        response = requests.get(OVERPASS_API_URL, params={'data': BASE_OVERPASS_QUERY.format(timeout=user_timeout)}, timeout=user_timeout)
        response.raise_for_status()
        data = response.json()
        return data.get('elements', [])
    except requests.exceptions.Timeout:
        raise OverpassTimeoutError("Timeout fetching data from Overpass API.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from Overpass API: {e}")
        return []
    except json.JSONDecodeError:
        logging.error("Failed to decode JSON from Overpass API response.")
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
    """Core logic for saving current progress to resume_state.json."""
    target_file = 'resume_state.json' # Define target_file at the beginning of the function
    logging.info(f"Attempting to save current progress to {target_file}...")

    # Log details of items being saved
    if results_to_save:
        logging.info("Items being saved:")
        for item in results_to_save:
            osm_id = item.get('osm_id', 'N/A')
            gnis_id = item.get('gnis_id', 'N/A')
            logging.info(f"  - OSM ID: {osm_id}, GNIS ID: {gnis_id}")
    else:
        logging.info("No results to save at this time.")

    temp_file = f"{target_file}.tmp" # Define temp_file based on target_file for clarity

    data_to_save = {
        'raw_overpass_data_cache': raw_overpass_data_cache,
        'features_to_check': features_to_check,
        'results': results_to_save
    }

    # Phase 1: Write to temporary file
    try:
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f) # Using compact JSON for efficiency
    except Exception as e: # Catching a broad exception as various issues can occur during file I/O
        logging.error(f"Failed to write progress to temporary file {temp_file}: {e}")
        if os.path.exists(temp_file): # Attempt cleanup if temp file was created
            try:
                os.remove(temp_file)
                logging.info(f"Cleaned up temporary file {temp_file} after write failure.")
            except OSError as oe:
                logging.error(f"Failed to remove temporary file {temp_file} after write failure: {oe}")
        return # Indicate that saving failed and exit function

    # Phase 2: Replace target file with temporary file
    try:
        os.replace(temp_file, target_file) # os.replace is generally atomic, ensuring integrity
        logging.info(f"Successfully saved progress to {target_file}: {len(results_to_save)} results, {len(features_to_check)} features to check. It is now safe to close the terminal.")
    except OSError as e: # Catch errors during file replacement
        logging.error(f"Failed to replace {target_file} with {temp_file}: {e}")
        if os.path.exists(temp_file): # Attempt cleanup if temp file still exists
            try:
                os.remove(temp_file)
                logging.info(f"Cleaned up temporary file {temp_file} after replace failure.")
            except OSError as oe:
                logging.error(f"Failed to remove temporary file {temp_file} after replace failure: {oe}")
        return # Indicate that saving failed and exit function

# Signal handler for SIGINT (Ctrl+C)
def sigint_handler(signum, frame):
    """Handles SIGINT signal (Ctrl+C) by saving progress and exiting."""
    logging.info(f"SIGINT (Ctrl+C) received (signal {signum}). Attempting to save progress before exiting...")
    # Access global variables directly as this is a signal handler context.
    _save_current_progress(global_raw_overpass_data, current_features_to_check_for_resume, current_results_for_resume)
    sys.exit(0) # Exit after attempting to save.

def setup_signal_handlers():
    """Registers signal handlers for graceful shutdown."""
    signal.signal(signal.SIGINT, sigint_handler)
    # Future: Register other signal handlers like SIGHUP or SIGTERM here if needed.

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
    logged_items_count = 0 # Tracks how many items have actually been logged

    # Loop until all items are logged or a clear termination signal for all items is processed.
    # This condition might need refinement based on how producer completion is signaled.
    while logged_items_count < total_features_to_log:
        try:
            # Wait for an item from the queue.
            result = await results_queue.get()
        except asyncio.CancelledError:
            logging.info("Logging task cancelled.")
            # Attempt to flush buffer before exiting on cancellation
            break

        if result is LOGGING_SENTINEL:
            # This sentinel typically means producers are done.
            # We break the loop and then try to flush any remaining items from the buffer.
            results_queue.task_done() # Acknowledge the sentinel
            break

        if result:
            buffered_results[result['original_index']] = result

        results_queue.task_done() # Signal that this item from queue is processed (either stored or was None)

        # Try to log all contiguous items starting from next_expected_log_index
        while next_expected_log_index in buffered_results:
            item_to_log = buffered_results.pop(next_expected_log_index)

            log_line = f"Item {item_to_log['original_index'] + 1}/{total_features_to_log}: {item_to_log['status']} Wikidata for GNIS ID {item_to_log['gnis_id']}"
            if item_to_log['wikidata_id'] and item_to_log['status'] == "Found":
                log_line += f": {item_to_log['wikidata_id']}"

            logging.info(log_line)
            next_expected_log_index += 1
            logged_items_count +=1

    # After the loop (e.g. sentinel received or task cancelled), flush any remaining buffered items.
    # This is crucial if items arrived out of order and the loop exited before they were all logged.
    if buffered_results:
        logging.debug(f"Flushing {len(buffered_results)} remaining buffered items...")
        for index in sorted(buffered_results.keys()): # Process in order
            if index >= next_expected_log_index: # Ensure not to re-log
                item_to_log = buffered_results.pop(index)
                log_line = f"Item {item_to_log['original_index'] + 1}/{total_features_to_log}: {item_to_log['status']} Wikidata for GNIS ID {item_to_log['gnis_id']}"
                if item_to_log['wikidata_id'] and item_to_log['status'] == "Found":
                    log_line += f": {item_to_log['wikidata_id']}"
                logging.info(log_line)
                logged_items_count +=1
                # next_expected_log_index = index + 1 # Update in case of gaps filled now

    if logged_items_count < total_features_to_log and total_features_to_log > 0:
         logging.warning(f"Logging task finished, but only {logged_items_count}/{total_features_to_log} items were logged. This might indicate an issue.")
    elif total_features_to_log == 0:
        logging.info("Logging task initiated for zero features; nothing to log.")
    else:
        logging.info(f"Logging task completed. All {logged_items_count}/{total_features_to_log} items logged.")


def load_progress():
    """Loads progress from resume_state.json if it exists."""
    if os.path.exists('resume_state.json'):
        try:
            with open('resume_state.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
            loaded_raw_overpass = data.get('raw_overpass_data_cache', None)
            loaded_features = data.get('features_to_check', [])
            loaded_results = data.get('results', [])

            log_message = f"Progress loaded from resume_state.json: {len(loaded_results)} results, {len(loaded_features)} features to potentially check."
            if loaded_raw_overpass is not None:
                log_message += " Found cached raw Overpass data."
            else:
                log_message += " No cached raw Overpass data found."
            logging.info(log_message)

            # Basic validation: ensure keys exist and values are lists for features and results
            if not isinstance(loaded_features, list) or not isinstance(loaded_results, list):
                logging.error("Invalid data format in resume_state.json for features_to_check or results. Expected lists.")
                os.remove('resume_state.json')
                logging.info("Corrupted or invalid resume_state.json removed.")
                return None, [], []
            return loaded_raw_overpass, loaded_features, loaded_results
        except (IOError, json.JSONDecodeError) as e: # Catch issues related to file reading or JSON parsing
            logging.error(f"Error loading or parsing resume_state.json: {e}. The file might be corrupted or unreadable.")
            try:
                os.remove('resume_state.json') # Attempt to remove the problematic file
                logging.info("Removed corrupted or unreadable resume_state.json.")
            except OSError as oe:
                logging.error(f"Error removing corrupted resume_state.json after load failure: {oe}")
            return None, [], []
    else:
        logging.info("No resume_state.json found. Starting a fresh session.")
        return None, [], []

async def process_features_concurrently(features_to_check, session, concurrency_limit=5):
    """Concurrently processes features to find Wikidata entries using a semaphore for rate limiting,
    with sequential logging of results."""
    if not features_to_check:
        logging.info("No features provided to process_features_concurrently for Wikidata lookup.")
        return []

    results_queue = asyncio.Queue()
    total_features = len(features_to_check)

    # Start the sequential logging coroutine as a background task
    # Note: `session` here is the aiohttp ClientSession passed to process_features_concurrently
    logging_task = asyncio.create_task(
        log_results_sequentially(results_queue, total_features)
    )

    semaphore = asyncio.Semaphore(concurrency_limit)

    async def process_feature(feature, original_index, queue_to_put_on, http_session):
        # This inner function processes a single feature.
        gnis_id = feature['tags']['gnis:feature_id']
        name = feature['tags'].get('name', 'N/A')
        osm_type = feature['type']
        osm_id = feature['id']
        wikidata_id = None
        status = "Not found" # Default status

        try:
            async with semaphore: # Acquire semaphore before processing
                wikidata_id = await find_wikidata_entry_by_gnis_id(http_session, gnis_id) # Use http_session

            if wikidata_id:
                status = "Found"

        except Exception as e:
            logging.error(f"Exception during Wikidata lookup for GNIS ID {gnis_id} (idx: {original_index}): {e}", exc_info=False)
            status = f"Error during lookup: {type(e).__name__}"
            # wikidata_id remains None

        result_dict = {
            'original_index': original_index,
            'gnis_id': gnis_id,
            'wikidata_id': wikidata_id,
            'name': name,
            'osm_type': osm_type,
            'osm_id': osm_id,
            'status': status
        }

        await queue_to_put_on.put(result_dict) # Use queue_to_put_on
        return result_dict

    # Create tasks for processing features
    # The outer `session` (aiohttp.ClientSession) is passed as `http_session` to process_feature
    feature_processing_tasks = [
        process_feature(feature, idx, results_queue, session)
        for idx, feature in enumerate(features_to_check)
    ]

    # Gather results from all feature processing tasks
    # These results are the dictionaries that were also put on the queue
    gathered_task_outputs = await asyncio.gather(*feature_processing_tasks, return_exceptions=True)

    # Signal the logging task that all producer tasks are done by putting sentinel on the queue
    await results_queue.put(LOGGING_SENTINEL)

    # Wait for the logging task to finish processing all queued items
    try:
        await logging_task
    except asyncio.CancelledError:
        logging.info("Logging task was cancelled during shutdown of process_features_concurrently.")


    # Now, build local_results from gathered_task_outputs for the final JSON output
    local_results = []
    for i, item_output in enumerate(gathered_task_outputs):
        if isinstance(item_output, Exception):
            # This means the process_feature task itself crashed.
            # The error should have been logged within process_feature's except block if it was a lookup error.
            # If it's a different error, or if process_feature didn't catch it before returning, log here.
            # The `result_dict` put on the queue by process_feature would have status "Error during lookup..."
            # This is for unhandled exceptions within the task body itself.
            # We need the original feature to log its GNIS ID if the task failed before creating result_dict
            original_feature = features_to_check[i] # Assuming gather preserves order for exceptions
            gnis_id_for_error = original_feature.get('tags', {}).get('gnis:feature_id', f'Unknown_at_original_index_{i}')
            logging.error(f"Task for GNIS ID {gnis_id_for_error} (original index {i}) failed with unhandled exception: {item_output}", exc_info=False)
        elif isinstance(item_output, dict): # This is the expected result_dict
            # We only add to local_results if Wikidata ID was found and status is "Found"
            if item_output.get('wikidata_id') and item_output.get('status') == "Found":
                feature_data = {
                    'osm_type': item_output['osm_type'],
                    'osm_id': item_output['osm_id'],
                    'name': item_output['name'],
                    'gnis_id': item_output['gnis_id'],
                    'wikidata_id': item_output['wikidata_id']
                }
                local_results.append(feature_data)
        # No action for other cases (e.g. status "Not found" or "Error during lookup" from successful task completion)
        # as these are logged by the logger, and we only want successful matches in local_results.

    return local_results

# Note: The generate_paginated_html_report function was previously removed.

async def handle_resume_data_loading(input_current_results_for_resume, input_global_raw_overpass_data):
    """Loads progress from file and merges with any initial state passed in.
    Returns various state elements including processed IDs, pending features, and updated result/data caches."""

    loaded_raw_overpass, loaded_features, loaded_results = load_progress()

    # Prioritize loaded data if available, otherwise use the initial state passed to the function.
    output_current_results_for_resume = loaded_results if loaded_results is not None else (input_current_results_for_resume or [])
    output_global_raw_overpass_data = loaded_raw_overpass if loaded_raw_overpass is not None else input_global_raw_overpass_data

    processed_gnis_ids = {res['gnis_id'] for res in output_current_results_for_resume if 'gnis_id' in res}

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
    """Fetches new OSM data if needed, handles cache, and purges duplicates.
    Returns features_for_processing, raw_data_for_resume_scope, and updated_raw_overpass_cache_state."""

    # Use a local variable for the raw Overpass data that might be modified by user choice or fetching.
    effective_raw_overpass_cache = current_global_raw_overpass_data_state
    source_of_new_raw_data = None # This will hold the raw elements list from Overpass/cache.
    attempt_live_api_fetch = True
    # This defines the full set of features for the current run, used for creating 'features_to_check' on interrupt.
    raw_data_defining_current_scope = effective_raw_overpass_cache

    if effective_raw_overpass_cache: # If there's cached raw data from a previous load/session.
        try:
            user_choice_cache = input(
                "Cached raw Overpass data found from a previous session. "
                "Use this data (y) or fetch fresh data from Overpass API (n)? [y/n]: "
            ).strip().lower()
            if user_choice_cache == 'y':
                source_of_new_raw_data = effective_raw_overpass_cache # User opts to use existing cache.
                attempt_live_api_fetch = False
                logging.info("User opted to use cached raw Overpass data.")
            elif user_choice_cache == 'n':
                logging.info("User opted to fetch fresh Overpass data. Discarding previous raw data cache.")
                effective_raw_overpass_cache = None # Discard cache.
                raw_data_defining_current_scope = None # Scope will be defined by new fetch.
            else: # Invalid input.
                logging.warning("Invalid choice regarding cached Overpass data. Defaulting to fetching fresh data.")
                effective_raw_overpass_cache = None
                raw_data_defining_current_scope = None
        except Exception as e: # Handle potential errors during input() e.g. if not interactive.
            logging.warning(f"Could not get user input for using cached Overpass data (perhaps running non-interactively). Defaulting to fetching fresh. Error: {e}")
            effective_raw_overpass_cache = None
            raw_data_defining_current_scope = None

        # If cache was discarded, ensure raw_data_defining_current_scope is also None.
        if effective_raw_overpass_cache is None:
             raw_data_defining_current_scope = None

    if attempt_live_api_fetch:
        logging.info("Attempting to fetch new data from Overpass API. This may take some time")
        try:
            fetched_data = fetch_osm_features_with_gnis_id(query_timeout)
            fetched_data_elements = fetch_osm_features_with_gnis_id(query_timeout)
            effective_raw_overpass_cache = fetched_data_elements # Update cache with newly fetched data.
            source_of_new_raw_data = fetched_data_elements
            raw_data_defining_current_scope = fetched_data_elements # This new fetch defines the scope.
        except OverpassTimeoutError as e: # Specific timeout error
            logging.error(str(e)) # Log the error.
            # Provide user-friendly message and exit, as this is a critical failure.
            print(f"\nERROR: The Overpass API query timed out. Timeout was set to {query_timeout} seconds.")
            print("Consider increasing the timeout using the --timeout option.")
            sys.exit(1)
        except Exception as e: # Catch other potential errors during fetch
            logging.error(f"An unexpected error occurred during Overpass API fetch: {e}")
            # Depending on policy, might exit or try to proceed without new data. Here, we exit.
            sys.exit(1)


    if not source_of_new_raw_data:
        logging.info("No new Overpass data sourced (either from cache or live fetch).")
        # Return empty lists for features and scope, along with the current (possibly None) cache state.
        return [], [], effective_raw_overpass_cache

    # Deduplication: Identify GNIS IDs used by multiple features.
    # These features will be logged and saved separately, then excluded from main processing.
    gnis_id_counts = collections.Counter()
    for feature in source_of_new_raw_data:
        gnis_id = feature.get('tags', {}).get('gnis:feature_id')
        if gnis_id:
            gnis_id_counts[gnis_id] += 1

    gnis_ids_to_purge = {gnis_id for gnis_id, count in gnis_id_counts.items() if count > 1}
    purged_features_for_json = []
    temp_features_for_further_processing = []

    for feature in source_of_new_raw_data:
        gnis_id = feature.get('tags', {}).get('gnis:feature_id')
        if gnis_id and gnis_id in gnis_ids_to_purge:
            purged_features_for_json.append(feature)
        else:
            temp_features_for_further_processing.append(feature)

    if gnis_ids_to_purge:
        logging.info(f"Identified {len(gnis_ids_to_purge)} GNIS IDs used by multiple OSM features. These {len(purged_features_for_json)} OSM features will be purged.")
        logging.info(f"GNIS IDs purged: {', '.join(sorted(list(gnis_ids_to_purge)))}")
        if purged_features_for_json:
            # Saving purged features (extracted to keep function focused or make utility)
            target_purged_file = 'purged_duplicate_gnis_features.json'
            temp_purged_file = 'purged_duplicate_gnis_features.json.tmp'
            try:
                with open(temp_purged_file, 'w', encoding='utf-8') as f:
                    json.dump(purged_features_for_json, f, indent=2)
                os.replace(temp_purged_file, target_purged_file)
                logging.info(f"Successfully saved {len(purged_features_for_json)} purged features to {target_purged_file}.")
            except Exception as e:
                logging.error(f"Error saving purged features to {target_purged_file}: {e}")
                # Clean up temp file if exists
                if os.path.exists(temp_purged_file): os.remove(temp_purged_file)


    # Filter features for actual processing: not having wikidata tag, having gnis:feature_id, and not already processed (from loaded results)
    features_for_processing_this_run = [
        feature for feature in temp_features_for_further_processing # Use de-duplicated list
        if 'wikidata' not in feature['tags']
        and 'gnis:feature_id' in feature['tags'] # Ensure it has GNIS ID
        and feature['tags']['gnis:feature_id'] not in processed_gnis_ids_from_loaded_results
    ]

    logging.info(f"Derived {len(features_for_processing_this_run)} features to process from new Overpass data source (after purging and filtering).")
    # raw_data_defining_current_scope: The full list of features from this Overpass fetch/cache choice. This is what `current_features_to_check_for_resume` will be set to.
    # effective_raw_overpass_cache: The actual raw data (list of features) that will be saved in `resume_state.json` under `raw_overpass_data_cache`.
    return features_for_processing_this_run, raw_data_defining_current_scope, effective_raw_overpass_cache


async def process_wikidata_lookups(features_to_process_list, current_results_list):
    """Processes a list of features to find corresponding Wikidata entries by their GNIS ID.
    Appends successfully found matches to the current_results_list."""

    if not features_to_process_list:
        logging.info("No features provided for Wikidata lookup in this batch.")
        return current_results_list # Return the list as is.

    # Ensure current_results_list is a list, even if None was passed.
    # This function will extend this list.
    output_results_list = list(current_results_list) if current_results_list is not None else []

    # Using a ClientSession for connection pooling and to set a User-Agent.
    # The User-Agent is important for responsible API interaction.
    headers = {'User-Agent': 'OSM-Wikidata-Updater/1.0 (contact: your.email@example.com; script: find_osm_features.py)'}
    async with aiohttp.ClientSession(headers=headers) as session:
        # process_features_concurrently handles the actual lookups and returns new findings.
        newly_found_results = await process_features_concurrently(features_to_process_list, session, concurrency_limit=5)

        if newly_found_results:
            output_results_list.extend(newly_found_results)
            logging.info(f"Wikidata lookup phase added {len(newly_found_results)} new results to the main list.")
        else:
            logging.info("Wikidata lookup phase completed, but no new results were found for this batch of features.")

    return output_results_list

async def save_final_results_and_cleanup(final_results_list):
    """Saves final results to JSON and cleans up resume state."""
    if final_results_list:
        try:
            sorted_results = sorted(final_results_list, key=lambda x: (x.get('osm_id', 0), x.get('gnis_id', '')))
            with open('osm_features_to_update.json', 'w', encoding='utf-8') as f:
                json.dump(sorted_results, f, indent=2)
            logging.info(f"Saved {len(sorted_results)} total features to osm_features_to_update.json")

            # Successful save, now remove resume state
            if os.path.exists("resume_state.json"):
                try:
                    os.remove("resume_state.json")
                    logging.info("Processing completed successfully. Resume state file 'resume_state.json' removed.")
                except OSError as e:
                    logging.error(f"Error removing resume_state.json: {e}")
            else:
                logging.info("Processing completed successfully. No resume state file to remove.")
        except IOError as e:
            logging.error(f"Error saving results to JSON: {e}")
            # Do not proceed to delete resume_state.json if saving results failed
            return # Exit early
    else:
        logging.info("No features with matching Wikidata entries found after processing and loading.")
        # Even if no results, if the script ran fully, clean up.
        if os.path.exists("resume_state.json"):
            try:
                os.remove("resume_state.json")
                logging.info("Processing completed (no results found). Resume state file 'resume_state.json' removed.")
            except OSError as e:
                logging.error(f"Error removing resume_state.json: {e}")
        else:
            logging.info("Processing completed (no results found). No resume state file to remove.")


async def main_async(query_timeout):
    """Main asynchronous function to orchestrate the OSM feature processing workflow."""
    # These global variables are updated by main_async and read by the sigint_handler.
    # This allows the signal handler to save the most recent state.
    global current_features_to_check_for_resume
    global current_results_for_resume
    global global_raw_overpass_data

    logging.info("Starting main_async execution.")
    setup_signal_handlers() # Register signal handlers early.

    # Initialize or use existing module-level state variables.
    # These are passed to helper functions, which return their modified versions.
    # main_async then updates the module-level globals with these returned states.

    # Step 1: Load any existing progress from resume_state.json.
    # This updates current_results_for_resume and global_raw_overpass_data based on file content.
    processed_gnis_ids, pending_features_from_loaded_scope, loaded_scope_for_resume, \
    current_results_for_resume, global_raw_overpass_data = await handle_resume_data_loading(
        current_results_for_resume, global_raw_overpass_data
    )

    features_for_processing_this_run = [] # Initialize list for features to be processed in this session.

    if pending_features_from_loaded_scope:
        # If there are features from a previous, interrupted run, process them first.
        logging.info(f"Resuming with {len(pending_features_from_loaded_scope)} pending features from loaded resume scope.")
        features_for_processing_this_run = pending_features_from_loaded_scope
        current_features_to_check_for_resume = loaded_scope_for_resume # Scope for resume is the original loaded list.
        # global_raw_overpass_data was already set by handle_resume_data_loading.
    else:
        # No pending features from a loaded scope; proceed to fetch new data.
        logging.info("No pending features from loaded scope. Attempting to source new Overpass data.")
        new_features_to_process, scope_defining_raw_data, \
        updated_raw_cache = await fetch_and_prepare_osm_data(
            query_timeout,
            processed_gnis_ids, # GNIS IDs from results loaded so far, to avoid re-processing.
            global_raw_overpass_data # Current raw Overpass data cache (might be None).
        )
        features_for_processing_this_run.extend(new_features_to_process)
        current_features_to_check_for_resume = scope_defining_raw_data # This is the full list for resume.
        global_raw_overpass_data = updated_raw_cache # Update the global cache.

    # Exit if there's nothing to process and no results to save.
    if not features_for_processing_this_run and not current_results_for_resume:
        logging.info("No features to process in this run and no existing results to save. Exiting.")
        if os.path.exists("resume_state.json"):
            try: os.remove("resume_state.json"); logging.info("Cleaned up resume_state.json.")
            except OSError as e: logging.error(f"Error removing resume_state.json: {e}")
        return

    # Step 3: Process Wikidata Lookups
    if features_for_processing_this_run:
        current_results_for_resume = await process_wikidata_lookups(
            features_for_processing_this_run, current_results_for_resume
        )
    else:
        logging.info("No features to submit for Wikidata lookup in this run.")

    # Step 4: Save Final Results and Cleanup
    await save_final_results_and_cleanup(current_results_for_resume)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch OSM features with GNIS IDs and check for corresponding Wikidata entries.")
    parser.add_argument(
        "--timeout",
        type=int,
        default=None, # Default to None, will be handled by interactive prompt or hardcoded default.
        help="Timeout in seconds for the Overpass API query. If not provided, script will prompt or use a default."
    )
    args = parser.parse_args()

    effective_timeout = args.timeout # Use timeout from CLI if provided.

    if effective_timeout is None: # No CLI timeout, prompt user or use hardcoded default.
        try:
            user_input_timeout_str = input("Enter Overpass API timeout in seconds (e.g., 10000, press Enter for default 10000s): ").strip()
            if not user_input_timeout_str: # User pressed Enter for default.
                effective_timeout = 10000
                logging.info("Using default Overpass API timeout: 10000 seconds.")
            else:
                effective_timeout = int(user_input_timeout_str)
                if effective_timeout <= 0: # Ensure positive timeout.
                    logging.warning("Timeout must be a positive integer. Using default 10000 seconds.")
                    effective_timeout = 10000
                else:
                    logging.info(f"User-defined Overpass API timeout: {effective_timeout} seconds.")
        except ValueError: # Non-integer input.
            logging.warning("Invalid input for timeout (not an integer). Using default 10000 seconds.")
            effective_timeout = 10000
    else: # CLI argument was provided and is not None.
        if effective_timeout <= 0: # Validate CLI timeout.
            logging.warning(f"CLI timeout value --timeout {args.timeout} is not positive. Using default 10000 seconds.")
            effective_timeout = 10000
        else:
            logging.info(f"Using Overpass API timeout from CLI argument: {effective_timeout} seconds.")

    # This final log confirms the timeout value being used for the session.
    logging.info(f"Overpass API timeout for this session set to: {effective_timeout} seconds.")

    start_time = time.time()
    asyncio.run(main_async(effective_timeout)) # Entry point for the async logic.
    end_time = time.time()
    logging.info(f"Total execution time: {end_time - start_time:.2f} seconds")
    logging.info("Data saved. It's ok to exit the terminal.")
