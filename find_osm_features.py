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
from typing import List, Dict, Tuple, Optional, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Custom Exception for Overpass API timeouts.
class OverpassTimeoutError(Exception):
    pass

class OsmWikidataProcessor:
    def __init__(self, config_path: str = 'config.json'):
        self.config_path = config_path
        self.config_data = self._load_config()

        # API endpoints and query templates from config
        self.overpass_api_url = self.config_data.get("OVERPASS_API_URL", "https://overpass-api.de/api/interpreter")
        self.wikidata_sparql_url = self.config_data.get("WIKIDATA_SPARQL_URL", "https://query.wikidata.org/sparql")
        self.base_overpass_query = self.config_data.get("BASE_OVERPASS_QUERY")
        self.sparql_query = self.config_data.get("SPARQL_QUERY")

        # Validate that essential queries are loaded
        if not all([self.base_overpass_query, self.sparql_query]):
            logging.error("BASE_OVERPASS_QUERY or SPARQL_QUERY is missing from the config file.")
            sys.exit(1)

        # --- Script Constants ---
        self.purged_shared_gnis_filename = "purged_duplicate_gnis_features.json"
        self.purged_multi_id_filename = "gnis_ids_on_multiple_features.json"
        self.final_results_filename = "osm_features_to_update.json"
        self.concurrency_limit = 5

        # Initialize state variables
        self.raw_osm_data: List[Dict[str, Any]] = []
        self.features_to_process: List[Dict[str, Any]] = []
        self.purged_shared: List[Dict[str, Any]] = []
        self.purged_multi_id: List[Dict[str, Any]] = []
        self.results: List[Dict[str, Any]] = []

    def _load_config(self) -> Dict[str, Any]:
        """Loads configuration from a JSON file."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as config_file:
                return json.load(config_file)
        except FileNotFoundError:
            logging.error(f"Configuration file not found at {self.config_path}. Please create it.")
            sys.exit(1)
        except json.JSONDecodeError as exception:
            logging.error(f"Error decoding JSON from the configuration file: {self.config_path}: {exception}")
            sys.exit(1)

    def _save_data_atomically(self, data: List[Dict[str, Any]], filename: str, log_context: str) -> None:
        """Saves data to a file atomically by writing to a temp file and then replacing."""
        if not data:
            return
        temp_filepath = f"{filename}.tmp"
        try:
            with open(temp_filepath, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, indent=2)
            os.replace(temp_filepath, filename)
            logging.info(f"Saved {len(data)} {log_context} to {filename}.")
        except Exception as exception:
            logging.error(f"Error saving {log_context} to {filename}: {exception}")
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)

    def _fetch_osm_features_with_gnis_id(self, user_timeout: int) -> List[Dict[str, Any]]:
        """Fetches features from Overpass API, expecting CSV output."""
        try:
            response = requests.get(self.overpass_api_url, params={'data': self.base_overpass_query.format(timeout=user_timeout)}, timeout=user_timeout)
            response.raise_for_status()
            csv_text = response.text
            elements = []
            csvfile = io.StringIO(csv_text)
            reader = csv.DictReader(csvfile, delimiter='\t')
            for row in reader:
                try:
                    if '@type' not in row or '@id' not in row or 'gnis:feature_id' not in row:
                        logging.warning(f"Skipping CSV row due to missing required columns: {row}")
                        continue
                    elements.append({'type': row['@type'], 'id': int(row['@id']), 'tags': {'gnis:feature_id': row['gnis:feature_id']}})
                except ValueError as ve:
                    logging.warning(f"Skipping CSV row due to invalid data: {row} - Error: {ve}")
                    continue
            return elements
        except requests.exceptions.Timeout:
            raise OverpassTimeoutError("Timeout fetching data from Overpass API.")
        except requests.exceptions.RequestException as exception:
            logging.error(f"Error fetching data from Overpass API: {exception}")
            return []
        except csv.Error as exception:
            logging.error(f"Failed to parse CSV from Overpass API response: {exception}")
            return []
        except Exception as exception:
            logging.error(f"An unexpected error occurred while processing Overpass data: {exception}")
            return []

    async def _find_wikidata_entry_by_gnis_id(self, session: aiohttp.ClientSession, gnis_id: str, max_retries: int = 3) -> Optional[str]:
        for attempt in range(max_retries):
            try:
                async with session.get(self.wikidata_sparql_url, params={'query': self.sparql_query.format(gnis_id=gnis_id), 'format': 'json'}) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('results', {}).get('bindings'):
                            return data['results']['bindings'][0]['item']['value'].split('/')[-1]
                    elif response.status == 429:
                        retry_after = response.headers.get('Retry-After')
                        if retry_after:
                            await asyncio.sleep(int(retry_after))
                        else:
                            break
                    else:
                        break
            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as exception:
                logging.error(f"Error for GNIS ID {gnis_id}: {exception}")
                break
        return None

    async def _log_results_sequentially(self, results_queue: asyncio.Queue, total_features_to_log: int) -> None:
        buffered_results: Dict[int, Dict[str, Any]] = {}
        next_expected_log_index = 0
        logged_items_count = 0
        LOGGING_SENTINEL = object()
        while logged_items_count < total_features_to_log:
            try:
                result = await results_queue.get()
                if result is LOGGING_SENTINEL:
                    results_queue.task_done()
                    break
                buffered_results[result['original_index']] = result
                results_queue.task_done()
                while next_expected_log_index in buffered_results:
                    item_to_log = buffered_results.pop(next_expected_log_index)
                    log_line = f"Item {item_to_log['original_index'] + 1}/{total_features_to_log}: {item_to_log['status']} Wikidata for GNIS ID {item_to_log['gnis_id']}"
                    if item_to_log['wikidata_id'] and item_to_log['status'] == "Found":
                        log_line += f": {item_to_log['wikidata_id']}"
                    logging.info(log_line)
                    next_expected_log_index += 1
                    logged_items_count += 1
            except asyncio.CancelledError:
                break

    async def _process_single_feature(self, feature: Dict[str, Any], original_index: int, http_session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> Dict[str, Any]:
        gnis_id = feature['tags']['gnis:feature_id']
        wikidata_id = None
        status = "Not found"
        try:
            async with semaphore:
                wikidata_id = await self._find_wikidata_entry_by_gnis_id(http_session, gnis_id)
            if wikidata_id:
                status = "Found"
                self.results.append({'osm_type': feature['type'], 'osm_id': feature['id'], 'gnis_id': gnis_id, 'wikidata_id': wikidata_id})
        except Exception as exception:
            status = f"Error: {type(exception).__name__}"
        return {'original_index': original_index, 'gnis_id': gnis_id, 'wikidata_id': wikidata_id, 'status': status}

    async def _process_features_concurrently(self) -> None:
        if not self.features_to_process:
            return
        results_queue: asyncio.Queue = asyncio.Queue()
        LOGGING_SENTINEL = object()
        logging_task = asyncio.create_task(self._log_results_sequentially(results_queue, len(self.features_to_process)))
        semaphore = asyncio.Semaphore(self.concurrency_limit)
        headers = {'User-Agent': 'OSM-Wikidata-Updater/1.0'}
        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = [self._process_single_feature(feature, idx, session, semaphore) for idx, feature in enumerate(self.features_to_process)]
            for future in asyncio.as_completed(tasks):
                result = await future
                await results_queue.put(result)
        await results_queue.put(LOGGING_SENTINEL)
        await logging_task

    def fetch_data(self, query_timeout: int) -> None:
        """Fetches raw OSM data from the Overpass API."""
        logging.info("Attempting to fetch new data from Overpass API...")
        try:
            self.raw_osm_data = self._fetch_osm_features_with_gnis_id(query_timeout)
        except OverpassTimeoutError as e:
            logging.error(f"{e} Timeout: {query_timeout}s.")
            sys.exit(1)
        except Exception as exception:
            logging.error(f"An unexpected error occurred during Overpass API fetch: {exception}")
            sys.exit(1)

    def prepare_data(self) -> None:
        """Handles deduplication and filtering of raw OSM data."""
        if not self.raw_osm_data:
            logging.info("No raw OSM data to prepare.")
            return

        gnis_id_counts = collections.Counter(f['tags']['gnis:feature_id'] for f in self.raw_osm_data if f.get('tags', {}).get('gnis:feature_id'))
        shared_gnis_ids = {gnis_id for gnis_id, count in gnis_id_counts.items() if count > 1}

        temp_features = []
        for feature in self.raw_osm_data:
            gnis_id = feature.get('tags', {}).get('gnis:feature_id')
            if gnis_id in shared_gnis_ids:
                self.purged_shared.append(feature)
            elif ';' in str(gnis_id):
                self.purged_multi_id.append(feature)
            else:
                temp_features.append(feature)

        self.features_to_process = temp_features
        logging.info(f"Data preparation complete. {len(self.features_to_process)} features to process.")

        if self.purged_shared:
            self._save_data_atomically(self.purged_shared, self.purged_shared_gnis_filename, "purged features (shared GNIS)")
        if self.purged_multi_id:
            self._save_data_atomically(self.purged_multi_id, self.purged_multi_id_filename, "features with multiple GNIS IDs in tag")

    async def process_features(self) -> None:
        """Processes features for Wikidata entries."""
        if not self.features_to_process:
            logging.info("No features to process for Wikidata lookup.")
            return
        await self._process_features_concurrently()

    async def save_results(self) -> None:
        """Saves final results to a JSON file."""
        if self.results:
            sorted_results = sorted(self.results, key=lambda x: (x.get('osm_id', 0), x.get('gnis_id', '')))
            self._save_data_atomically(sorted_results, self.final_results_filename, "total features")
        else:
            logging.info("No features with matching Wikidata entries found.")

    async def run(self, query_timeout: int):
        """Runs the entire feature processing workflow."""
        self.fetch_data(query_timeout)
        self.prepare_data()
        await self.process_features()
        await self.save_results()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch OSM features with GNIS IDs and find corresponding Wikidata entries.")
    parser.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Timeout in seconds for Overpass API query. Prompts if not set."
    )
    args = parser.parse_args()
    DEFAULT_TIMEOUT = 10000

    effective_timeout = args.timeout
    source_of_timeout = "CLI"

    if effective_timeout is None:
        try:
            user_input = input(f"Enter Overpass API timeout in seconds (e.g., {DEFAULT_TIMEOUT}, Enter for default): ")
            if user_input:
                effective_timeout = int(user_input)
                source_of_timeout = "user input"
            else:
                effective_timeout = DEFAULT_TIMEOUT
                source_of_timeout = "default"
        except ValueError:
            effective_timeout = DEFAULT_TIMEOUT
            source_of_timeout = "default due to invalid input"
            logging.warning(f"Invalid timeout input. Using default {DEFAULT_TIMEOUT}s.")

    if effective_timeout <= 0:
        logging.warning(f"Timeout from {source_of_timeout} ('{args.timeout or user_input}') is not positive. Using default {DEFAULT_TIMEOUT}s.")
        effective_timeout = DEFAULT_TIMEOUT

    logging.info(f"Using Overpass API timeout from {source_of_timeout}: {effective_timeout}s.")

    processor = OsmWikidataProcessor()
    start_time = time.time()
    asyncio.run(processor.run(effective_timeout))
    end_time = time.time()

    logging.info(f"Total execution time: {end_time - start_time:.2f}s.")
    logging.info("Processing complete.")
