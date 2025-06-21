
# openstreetmap - Wikidata gnis conflator

this script finds osm features that lack the `wikidata` tag by conflating the GNIS tag with a matching QID

the logic of the program goes as follows

1: call the overpass API, then cull the data to keep only unique GNIS features. (in testing, this tends to be rivers that should be in a relation, but that's out of scope for this program.) discarded features are exported to a separate JSON file.
2: call SPARQL for individual GNIS codes.
3: once completeed the script will exit and a JSON file named "osm_features_to_update" be created in the root folder.

## Requirements

*   Python 3.7+
*   The `requests` library (for OSM data fetching).
*   The `aiohttp` library (for asynchronous Wikidata queries).
(All dependencies are listed in `requirements.txt`.)

## Installation

1.  git clone https://github.com/Necessarycoot72/osm-wikidata-thing.git  
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
*   (Optional) `--timeout <seconds>`: Sets the timeout value in seconds. Default: 10000 seconds (when not provided via CLI and no input given at prompt).
## Output

*   **Console Logs:** The script prints info messages and error logs to the console during its execution.
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
    ## saving and loading

The script can be exited and resumed  during the conflation stage by pressing Ctrl + C. this will save the current state in the root directly. This may take some time, the console. 

## The overpass query

1.   The script queries the Overpass API to retrieve all OSM features that have a `gnis:feature_id` tag. The query used is:

[out:json][timeout:{timeout}];
(
  nwr["gnis:feature_id"];
);
out ids;

with `timeout` being determined by CLI argument or by the user prompt
    #### Overpass API Interaction & Timeout

## Running Tests

Unit tests are provided to verify the functionality of individual components of the script. To run the tests, execute:
```bash
python -m unittest tests.test_osm_processing.py
python -m unittest tests.test_resume_functionality.py
```
Ensure you are in the root directory of the project when running this command.
