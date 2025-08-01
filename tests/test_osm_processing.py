import unittest
from unittest.mock import patch, MagicMock
import json
import asyncio

# Ensure find_osm_features.py can be imported (assuming it's in the parent directory).
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from find_osm_features import (
    fetch_osm_features_with_gnis_id,
    find_wikidata_entries_by_gnis_ids_batch,
    process_features_concurrently,
    OVERPASS_API_URL,
    WIKIDATA_SPARQL_URL
)
from requests.exceptions import RequestException

class TestOsmProcessing(unittest.IsolatedAsyncioTestCase):

    @patch('find_osm_features.requests.get')
    def test_fetch_osm_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        # This should be a string of CSV data, not a dictionary
        mock_response.text = "@type\t@id\tgnis:feature_id\nnode\t1\t123\nway\t2\t456"
        mock_get.return_value = mock_response

        expected_output = [
            {'type': 'node', 'id': 1, 'tags': {'gnis:feature_id': '123'}},
            {'type': 'way', 'id': 2, 'tags': {'gnis:feature_id': '456'}},
        ]

        result = fetch_osm_features_with_gnis_id(user_timeout=10)
        self.assertEqual(result, expected_output)
        mock_get.assert_called_once()


    @patch('find_osm_features.requests.get')
    def test_fetch_osm_request_exception(self, mock_get):
        mock_get.side_effect = RequestException("Test network error")
        with self.assertRaises(SystemExit):
             fetch_osm_features_with_gnis_id(user_timeout=10)

    @patch('find_osm_features.requests.post')
    def test_find_wikidata_batch_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {"gnis_id": {"value": "123"}, "item": {"value": "http://www.wikidata.org/entity/Q1"}},
                    {"gnis_id": {"value": "456"}, "item": {"value": "http://www.wikidata.org/entity/Q2"}},
                ]
            }
        }
        mock_post.return_value = mock_response

        gnis_ids = ["123", "456"]
        result = find_wikidata_entries_by_gnis_ids_batch(gnis_ids)

        expected = {"123": "Q1", "456": "Q2"}
        self.assertEqual(result, expected)
        mock_post.assert_called_once()

    @patch('find_osm_features.requests.post')
    def test_find_wikidata_batch_empty(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": {"bindings": []}}
        mock_post.return_value = mock_response

        result = find_wikidata_entries_by_gnis_ids_batch(["789"])
        self.assertEqual(result, {})

    @patch('find_osm_features.find_wikidata_entries_by_gnis_ids_batch')
    async def test_process_features_batching(self, mock_batch_find):
        sample_features = [
            {"type": "node", "id": 1, "tags": {"gnis:feature_id": "111"}},
            {"type": "node", "id": 2, "tags": {"gnis:feature_id": "222"}},
            {"type": "node", "id": 3, "tags": {"gnis:feature_id": "333"}},
        ]

        # Mock the return value of the batch function
        mock_batch_find.return_value = {
            "111": "Q111",
            "333": "Q333"
        }

        master_results_list = []
        added_count = await process_features_concurrently(sample_features, master_results_list)

        self.assertEqual(added_count, 2)
        expected_results = [
            {'osm_type': 'node', 'osm_id': 1, 'gnis_id': '111', 'wikidata_id': 'Q111'},
            {'osm_type': 'node', 'osm_id': 3, 'gnis_id': '333', 'wikidata_id': 'Q333'},
        ]

        # Sort both lists by a common key to ensure comparison is order-independent
        self.assertCountEqual(master_results_list, expected_results)
        mock_batch_find.assert_called_once_with(["111", "222", "333"])

if __name__ == '__main__':
    unittest.main()
