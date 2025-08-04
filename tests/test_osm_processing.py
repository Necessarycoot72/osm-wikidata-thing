import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import json
import asyncio
import aiohttp

# Ensure find_osm_features.py can be imported (assuming it's in the parent directory).
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from find_osm_features import (
    fetch_osm_features_with_gnis_id,
    find_wikidata_entry_by_gnis_id,
    process_features_concurrently,
    OVERPASS_API_URL,
    WIKIDATA_SPARQL_URL
)
from requests.exceptions import RequestException

class TestOsmProcessing(unittest.IsolatedAsyncioTestCase):

    @patch('find_osm_features.requests.get')
    def test_fetch_osm_success(self, mock_get):
        # This test needs to be updated to reflect the new CSV-based parsing
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        # Overpass CSV format: @type, @id, gnis:feature_id
        mock_response.text = "@type\t@id\tgnis:feature_id\nnode\t1\t123\nway\t2\t456"
        mock_get.return_value = mock_response

        expected_output = [
            {'type': 'node', 'id': 1, 'tags': {'gnis:feature_id': '123'}},
            {'type': 'way', 'id': 2, 'tags': {'gnis:feature_id': '456'}},
        ]

        result = fetch_osm_features_with_gnis_id(user_timeout=10) # Pass timeout
        self.assertEqual(result, expected_output)
        mock_get.assert_called_once()


    @patch('find_osm_features.requests.get')
    def test_fetch_osm_request_exception(self, mock_get):
        mock_get.side_effect = RequestException("Test network error")
        result = fetch_osm_features_with_gnis_id(user_timeout=10) # Pass timeout
        self.assertEqual(result, [])

    @patch('find_osm_features.requests.get')
    def test_fetch_osm_json_decode_error(self, mock_get):
        # This test is now about CSV parsing errors, not JSON.
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.text = "invalid,csv\n" # Malformed CSV
        mock_get.return_value = mock_response
        result = fetch_osm_features_with_gnis_id(user_timeout=10) # Pass timeout
        self.assertEqual(result, [])

    async def helper_setup_mock_session_response(self):
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)

        # Mock the async context manager for session.get()
        mock_context_manager = AsyncMock()
        mock_session.get.return_value = mock_context_manager
        mock_context_manager.__aenter__.return_value = mock_response
        mock_context_manager.__aexit__.return_value = None # Ensure proper async context cleanup

        return mock_session, mock_response

    async def test_find_wikidata_success(self):
        mock_session, mock_response = await self.helper_setup_mock_session_response()
        mock_response.status = 200
        mock_response.json.return_value = {
            "results": {"bindings": [{"item": {"value": "http://www.wikidata.org/entity/Q123"}}]}
        }
        result = await find_wikidata_entry_by_gnis_id(mock_session, "test_gnis_123")
        self.assertEqual(result, "Q123")
        mock_session.get.assert_called_once()

    async def test_find_wikidata_no_match(self):
        mock_session, mock_response = await self.helper_setup_mock_session_response()
        mock_response.status = 200
        mock_response.json.return_value = {"results": {"bindings": []}}
        result = await find_wikidata_entry_by_gnis_id(mock_session, "test_gnis_no_match")
        self.assertIsNone(result)

    async def test_find_wikidata_aiohttp_client_error(self):
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_session.get.side_effect = aiohttp.ClientError("Test ClientError") # Mock .get() to raise ClientError
        result = await find_wikidata_entry_by_gnis_id(mock_session, "test_gnis_client_err")
        self.assertIsNone(result)

    async def test_find_wikidata_json_decode_error_async(self):
        mock_session, mock_response = await self.helper_setup_mock_session_response()
        mock_response.status = 200
        mock_response.json.side_effect = json.JSONDecodeError("Error decoding JSON", "doc", 0)
        mock_response.text = AsyncMock(return_value="Invalid JSON")
        result = await find_wikidata_entry_by_gnis_id(mock_session, "test_gnis_json_err")
        self.assertIsNone(result)

    async def test_find_wikidata_content_type_error(self):
        mock_session, mock_response = await self.helper_setup_mock_session_response()
        mock_response.status = 200
        mock_response.json.side_effect = aiohttp.ContentTypeError(MagicMock(), MagicMock())
        mock_response.text = AsyncMock(return_value="Not JSON content type")
        result = await find_wikidata_entry_by_gnis_id(mock_session, "test_gnis_content_err")
        self.assertIsNone(result)

    async def test_find_wikidata_timeout_error(self):
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        # Mock session.get() to raise asyncio.TimeoutError.
        mock_session.get.side_effect = asyncio.TimeoutError
        result = await find_wikidata_entry_by_gnis_id(mock_session, "test_gnis_timeout")
        self.assertIsNone(result)

    async def test_find_wikidata_non_200_status(self):
        mock_session, mock_response = await self.helper_setup_mock_session_response()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Server error")
        result = await find_wikidata_entry_by_gnis_id(mock_session, "test_gnis_500_err")
        self.assertIsNone(result)

    # --- Tests for asynchronous process_features_concurrently ---
    async def test_process_concurrently_empty_input(self):
        # Setup mocks
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        master_results_list = []
        mock_lock = AsyncMock(spec=asyncio.Lock)

        count = await process_features_concurrently([], mock_session, master_results_list, mock_lock)
        self.assertEqual(count, 0)
        self.assertEqual(master_results_list, [])


    @patch('find_osm_features.find_wikidata_entry_by_gnis_id', new_callable=AsyncMock)
    async def test_process_concurrently_success_all_match(self, mock_find_wikidata):
        # Setup mocks and inputs
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        master_results_list = []
        mock_lock = AsyncMock(spec=asyncio.Lock)
        sample_features = [
            {"type": "node", "id": 1, "tags": {"gnis:feature_id": "111"}},
            {"type": "way", "id": 2, "tags": {"gnis:feature_id": "222"}},
        ]
        async def side_effect_func(session, gnis_id):
            return f"Q{gnis_id}"
        mock_find_wikidata.side_effect = side_effect_func

        # Execute
        count = await process_features_concurrently(sample_features, mock_session, master_results_list, mock_lock, concurrency_limit=2)

        # Assert
        self.assertEqual(count, 2)
        expected = [
            {'osm_type': 'node', 'osm_id': 1, 'gnis_id': '111', 'wikidata_id': 'Q111'},
            {'osm_type': 'way', 'osm_id': 2, 'gnis_id': '222', 'wikidata_id': 'Q222'},
        ]
        self.assertCountEqual(master_results_list, expected)


    @patch('find_osm_features.find_wikidata_entry_by_gnis_id', new_callable=AsyncMock)
    async def test_process_concurrently_some_match_some_none(self, mock_find_wikidata):
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        master_results_list = []
        mock_lock = AsyncMock(spec=asyncio.Lock)
        sample_features = [
            {"type": "node", "id": 1, "tags": {"gnis:feature_id": "111"}},
            {"type": "node", "id": 2, "tags": {"gnis:feature_id": "222"}}, # No match
            {"type": "node", "id": 3, "tags": {"gnis:feature_id": "333"}},
        ]
        async def side_effect_func(session, gnis_id):
            if gnis_id in ["111", "333"]: return f"Q{gnis_id}"
            return None
        mock_find_wikidata.side_effect = side_effect_func

        count = await process_features_concurrently(sample_features, mock_session, master_results_list, mock_lock)

        self.assertEqual(count, 2)
        expected = [
            {'osm_type': 'node', 'osm_id': 1, 'gnis_id': '111', 'wikidata_id': 'Q111'},
            {'osm_type': 'node', 'osm_id': 3, 'gnis_id': '333', 'wikidata_id': 'Q333'},
        ]
        self.assertCountEqual(master_results_list, expected)


    @patch('find_osm_features.find_wikidata_entry_by_gnis_id', new_callable=AsyncMock)
    async def test_process_concurrently_with_exceptions(self, mock_find_wikidata):
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        master_results_list = []
        mock_lock = AsyncMock(spec=asyncio.Lock)
        sample_features = [
            {"type": "node", "id": 1, "tags": {"gnis:feature_id": "111"}},
            {"type": "node", "id": 2, "tags": {"gnis:feature_id": "222"}}, # Will raise error
            {"type": "node", "id": 3, "tags": {"gnis:feature_id": "333"}}, # No match
        ]
        async def side_effect_func(session, gnis_id):
            if gnis_id == "111": return "Q111"
            if gnis_id == "222": raise aiohttp.ClientError("Simulated API error")
            return None
        mock_find_wikidata.side_effect = side_effect_func

        count = await process_features_concurrently(sample_features, mock_session, master_results_list, mock_lock)

        self.assertEqual(count, 1)
        expected = [{'osm_type': 'node', 'osm_id': 1, 'gnis_id': '111', 'wikidata_id': 'Q111'}]
        self.assertCountEqual(master_results_list, expected)


    @patch('find_osm_features.find_wikidata_entry_by_gnis_id', new_callable=AsyncMock)
    async def test_process_concurrently_chunking(self, mock_find_wikidata):
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        master_results_list = []
        mock_lock = AsyncMock(spec=asyncio.Lock)
        sample_features = [
            {"id": i, "type": "node", "tags": {"gnis:feature_id": str(i)*3}} for i in range(1, 6) # 5 features
        ]
        async def side_effect_func(session, gnis_id):
            return f"Q{gnis_id}"
        mock_find_wikidata.side_effect = side_effect_func

        count = await process_features_concurrently(sample_features, mock_session, master_results_list, mock_lock, concurrency_limit=2)

        self.assertEqual(count, 5)
        self.assertEqual(len(master_results_list), 5)
        self.assertEqual(mock_find_wikidata.call_count, 5)
        expected_qids = {f"Q{str(i)*3}" for i in range(1,6)}
        returned_qids = {item['wikidata_id'] for item in master_results_list}
        self.assertSetEqual(returned_qids, expected_qids)

if __name__ == '__main__':
    unittest.main()
