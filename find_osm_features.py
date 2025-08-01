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

# API endpoints and query templates.
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
SELECT ?item ?gnis_id WHERE {{
  VALUES ?gnis_id {{ {gnis_ids} }}
  ?item wdt:P590 ?gnis_id .
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
        sys.exit(1)
    except csv.Error as e:
        logging.error(f"Failed to parse CSV from Overpass API response: {e}")
        return []
    except Exception as e: # Catch any other unexpected errors
        logging.error(f"An unexpected error occurred while processing Overpass data: {e}")
        return []


def find_wikidata_entries_by_gnis_ids_batch(gnis_ids, max_retries=3):
    """
    Finds Wikidata entries for a batch of GNIS IDs using a single SPARQL query.
    Returns a dictionary mapping GNIS IDs to Wikidata QIDs.
    """
    if not gnis_ids:
        return {}

    # The VALUES clause in SPARQL requires space-separated strings in quotes.
    gnis_id_string = " ".join(f'"{gnis_id}"' for gnis_id in gnis_ids)
    query = SPARQL_QUERY.format(gnis_ids=gnis_id_string)

    headers = {
        'User-Agent': 'OSM-Wikidata-Updater/1.0 (your.email@example.com; find_osm_features.py)',
        'Accept': 'application/json'
    }

    for attempt in range(max_retries):
        try:
            response = requests.post(WIKIDATA_SPARQL_URL, data={'query': query}, headers=headers)
            if response.status_code == 200:
                data = response.json()
                results = {}
                for binding in data.get('results', {}).get('bindings', []):
                    gnis_id = binding.get('gnis_id', {}).get('value')
                    item_qid = binding.get('item', {}).get('value', '').split('/')[-1]
                    if gnis_id and item_qid:
                        results[gnis_id] = item_qid
                return results
            elif response.status_code == 429: # Rate limited
                retry_after = int(response.headers.get('Retry-After', '60'))
                logging.warning(f"Rate limited. Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                logging.error(f"Error fetching Wikidata entries: HTTP {response.status_code} - {response.text}")
                break # Don't retry on non-rate-limit errors
        except requests.RequestException as e:
            logging.error(f"Request error fetching Wikidata entries: {e}")
            if attempt < max_retries - 1:
                time.sleep(5) # Wait before retrying
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error when fetching Wikidata entries: {e} - Response: {response.text}")
            break # Don't retry on decode error

    return {}



def process_features_concurrently(features_to_check, master_results_list, batch_size):
    """
    Processes features in batches to find Wikidata entries.
    Updates master_results_list in place.
    """
    if not features_to_check:
        logging.info("No features for Wikidata lookup in this batch.")
        return 0

    initial_master_results_len = len(master_results_list)

    for i in range(0, len(features_to_check), batch_size):
        batch = features_to_check[i:i + batch_size]
        gnis_ids_in_batch = [feature['tags']['gnis:feature_id'] for feature in batch]

        logging.info(f"Processing batch {i//batch_size + 1}/{-(-len(features_to_check)//batch_size)}: {len(batch)} features.")

        # This is a synchronous, blocking call.
        wikidata_results_map = find_wikidata_entries_by_gnis_ids_batch(gnis_ids_in_batch)

        for feature in batch:
            gnis_id = feature['tags']['gnis:feature_id']
            wikidata_id = wikidata_results_map.get(gnis_id)
            status = "Found" if wikidata_id else "Not found"

            # Log the result for each feature.
            logging.info(f"GNIS ID: {gnis_id}, Status: {status}" + (f", Wikidata ID: {wikidata_id}" if wikidata_id else ""))

            if wikidata_id:
                master_results_list.append({
                    'osm_type': feature['type'],
                    'osm_id': feature['id'],
                    'gnis_id': gnis_id,
                    'wikidata_id': wikidata_id
                })

    return len(master_results_list) - initial_master_results_len


def fetch_and_prepare_osm_data(query_timeout):
    """Fetches OSM data, handles deduplication, and filters features."""
    logging.info("Attempting to fetch new data from Overpass API. This may take some time...")
    try:
        source_of_new_raw_data = fetch_osm_features_with_gnis_id(query_timeout)
    except OverpassTimeoutError as e:
        logging.error(str(e))
        print(f"\nERROR: The Overpass API query timed out. Timeout: {query_timeout}s.")
        print("Consider increasing the timeout using the --timeout option.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred during Overpass API fetch: {e}")
        sys.exit(1)

    if not source_of_new_raw_data:
        logging.info("No Overpass data sourced. Returning empty.")
        return [], None, None

    initial_total_features = len(source_of_new_raw_data)

    # Deduplication Stage 1: Purge OSM features if their GNIS ID is used by multiple OSM features.
    gnis_id_counts = collections.Counter(
        f['tags']['gnis:feature_id'] for f in source_of_new_raw_data if f.get('tags', {}).get('gnis:feature_id')
    )
    gnis_ids_to_purge_shared = {gnis_id for gnis_id, count in gnis_id_counts.items() if count > 1}
    purged_features_shared_gnis_json = []
    temp_features_after_shared_gnis_purge = []

    for feature in source_of_new_raw_data:
        gnis_id = feature.get('tags', {}).get('gnis:feature_id')
        if gnis_id and gnis_id in gnis_ids_to_purge_shared:
            purged_features_shared_gnis_json.append(feature)
        else:
            temp_features_after_shared_gnis_purge.append(feature)

    count_purged_due_to_shared_gnis = len(purged_features_shared_gnis_json)
    if gnis_ids_to_purge_shared:
        logging.info(f"Identified {len(gnis_ids_to_purge_shared)} GNIS IDs used by multiple OSM features. "
                     f"{count_purged_due_to_shared_gnis} OSM features will be purged.")
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
        target_multi_id_file = 'gnis_ids_on_multiple_features.json'
        temp_multi_id_file = f"{target_multi_id_file}.tmp"
        try:
            with open(temp_multi_id_file, 'w', encoding='utf-8') as f:
                json.dump(features_with_multiple_ids_in_tag_list, f, indent=2)
            os.replace(temp_multi_id_file, target_multi_id_file)
            logging.info(f"Saved {count_purged_due_to_multiple_ids_in_tag} features with multiple GNIS IDs in tag to {target_multi_id_file}.")
        except Exception as e:
            logging.error(f"Error saving features with multiple GNIS IDs in tag to {target_multi_id_file}: {e}")
            if os.path.exists(temp_multi_id_file): os.remove(temp_multi_id_file)

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


def process_wikidata_lookups(features_to_process_list, master_results_list, batch_size):
    """Processes features for Wikidata entries; `master_results_list` is updated in-place."""
    if not features_to_process_list:
        logging.info("No features provided for Wikidata lookup in this batch.")
        return master_results_list # Return the list as is.

    # Since we are batching, the async processing is simpler.
    # The `process_features_concurrently` function will be refactored to handle batches.
    count_newly_added = process_features_concurrently(
        features_to_process_list,
        master_results_list, # Passed by reference, updated in place.
        batch_size,
    )

    if count_newly_added > 0:
        logging.info(f"Wikidata lookup phase added {count_newly_added} new results to the main list.")
    else:
        logging.info("Wikidata lookup phase completed for this batch, no new results added.")
    return master_results_list

def save_final_results_and_cleanup(final_results_list, purged_shared, purged_multi_id):
    """Saves final results and purged features to JSON files."""
    if final_results_list:
        try:
            # Sort results for consistent output, helpful for diffs and review.
            sorted_results = sorted(final_results_list, key=lambda x: (x.get('osm_id', 0), x.get('gnis_id', '')))
            with open('osm_features_to_update.json', 'w', encoding='utf-8') as f:
                json.dump(sorted_results, f, indent=2)
            logging.info(f"Saved {len(sorted_results)} total features to osm_features_to_update.json")
        except IOError as e:
            logging.error(f"Error saving final results to JSON: {e}")
    else: # No results to save.
        logging.info("No features with matching Wikidata entries found after processing.")

    if purged_shared:
        try:
            with open('purged_duplicate_gnis_features.json', 'w', encoding='utf-8') as f:
                json.dump(purged_shared, f, indent=2)
            logging.info(f"Saved {len(purged_shared)} purged features (shared GNIS) to purged_duplicate_gnis_features.json")
        except IOError as e:
            logging.error(f"Error saving purged (shared GNIS) features: {e}")

    if purged_multi_id:
        try:
            with open('gnis_ids_on_multiple_features.json', 'w', encoding='utf-8') as f:
                json.dump(purged_multi_id, f, indent=2)
            logging.info(f"Saved {len(purged_multi_id)} purged features (multiple GNIS IDs) to gnis_ids_on_multiple_features.json")
        except IOError as e:
            logging.error(f"Error saving purged (multiple GNIS IDs) features: {e}")


def main_async(query_timeout, batch_size):
    """Main asynchronous function for the OSM feature processing workflow."""
    logging.info("Starting main asynchronous execution.")

    (
        features_for_processing_this_run,
        purged_shared,
        purged_multi_id,
    ) = fetch_and_prepare_osm_data(query_timeout)

    # Early exit if no features to process.
    if not features_for_processing_this_run:
        logging.info("No features to process this run. Exiting.")
        return

    # Process Wikidata Lookups.
    results = process_wikidata_lookups(
        features_for_processing_this_run,
        [], # Start with an empty list of results.
        batch_size,
    )

    # Save final results.
    save_final_results_and_cleanup(results, purged_shared, purged_multi_id)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch OSM features with GNIS IDs and find corresponding Wikidata entries.")
    parser.add_argument(
        "--timeout",
        type=int,
        default=None, # Handled by prompt or default value if not given.
        help="Timeout in seconds for Overpass API query. Prompts if not set."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Number of GNIS IDs to batch in a single Wikidata query."
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

    effective_batch_size = args.batch_size
    DEFAULT_BATCH_SIZE = 500

    if effective_batch_size is None:
        try:
            user_input_batch_size_str = input(f"Enter Wikidata query batch size (e.g., {DEFAULT_BATCH_SIZE}, Enter for default {DEFAULT_BATCH_SIZE}): ").strip()
            if not user_input_batch_size_str:
                effective_batch_size = DEFAULT_BATCH_SIZE
            else:
                effective_batch_size = int(user_input_batch_size_str)
                if effective_batch_size <= 0:
                    logging.warning(f"Batch size must be positive. Using default {DEFAULT_BATCH_SIZE}.")
                    effective_batch_size = DEFAULT_BATCH_SIZE
                else:
                    logging.info(f"User-defined batch size: {effective_batch_size}.")
        except ValueError:
            logging.warning(f"Invalid batch size input. Using default {DEFAULT_BATCH_SIZE}.")
            effective_batch_size = DEFAULT_BATCH_SIZE
    else:
        if effective_batch_size <= 0:
            logging.warning(f"CLI batch size --batch-size {args.batch_size} not positive. Using default {DEFAULT_BATCH_SIZE}.")
            effective_batch_size = DEFAULT_BATCH_SIZE
        else:
            logging.info(f"Using batch size from CLI: {effective_batch_size}.")

    logging.info(f"Wikidata query batch size for this session: {effective_batch_size}.")

    start_time = time.time()
    main_async(effective_timeout, effective_batch_size) # Run the main async workflow.
    end_time = time.time()
    logging.info(f"Total execution time: {end_time - start_time:.2f}s.")
    logging.info("Processing complete. It is safe to exit the terminal.")
