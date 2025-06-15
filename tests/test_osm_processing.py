import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import json
import asyncio
import aiohttp

# Adjust the import path if your find_osm_features.py is in a different location relative to tests
# Assuming find_osm_features.py is in the parent directory of 'tests'
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
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_osm_data = {
            "elements": [
                {"type": "node", "id": 1, "tags": {"gnis:feature_id": "123", "name": "Test Node 1"}},
                {"type": "way", "id": 2, "tags": {"gnis:feature_id": "456", "name": "Test Way 1"}},
            ]
        }
        mock_response.json.return_value = mock_osm_data
        mock_get.return_value = mock_response
        expected_output = [
            {'id': 1, 'type': 'node', 'tags': {'gnis:feature_id': '123', 'name': 'Test Node 1'}},
            {'id': 2, 'type': 'way', 'tags': {'gnis:feature_id': '456', 'name': 'Test Way 1'}},
        ]
        result = fetch_osm_features_with_gnis_id()
        self.assertEqual(result, expected_output)
        mock_get.assert_called_once_with(
            OVERPASS_API_URL,
            params={'data': unittest.mock.ANY},
            headers=unittest.mock.ANY
        )

    @patch('find_osm_features.requests.get')
    def test_fetch_osm_request_exception(self, mock_get):
        mock_get.side_effect = RequestException("Test network error")
        result = fetch_osm_features_with_gnis_id()
        self.assertEqual(result, [])

    @patch('find_osm_features.requests.get')
    def test_fetch_osm_json_decode_error(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Error decoding JSON", "doc", 0)
        mock_get.return_value = mock_response
        result = fetch_osm_features_with_gnis_id()
        self.assertEqual(result, [])

    async def helper_setup_mock_session_response(self):
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)

        # Setup the async context manager behavior for session.get()
        mock_context_manager = AsyncMock()
        mock_session.get.return_value = mock_context_manager
        mock_context_manager.__aenter__.return_value = mock_response
        mock_context_manager.__aexit__.return_value = None # Important for proper cleanup

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
        mock_session = AsyncMock(spec=aiohttp.ClientSession) # session itself
        mock_session.get.side_effect = aiohttp.ClientError("Test ClientError") # .get() call raises
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
        # Ensure the .get call returns an object that, when awaited in async with, raises TimeoutError
        # The timeout can occur during different phases of session.get(), so this is a common way to mock it.
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
        result = await process_features_concurrently([])
        self.assertEqual(result, [])

    @patch('find_osm_features.find_wikidata_entry_by_gnis_id', new_callable=AsyncMock)
    async def test_process_concurrently_success_all_match(self, mock_find_wikidata):
        sample_features = [
            {"id": 1, "type": "node", "tags": {"gnis:feature_id": "111", "name": "Feature 1"}},
            {"id": 2, "type": "way", "tags": {"gnis:feature_id": "222", "name": "Feature 2"}},
        ]
        async def side_effect_func(session, gnis_id): # Make side_effect async
            if gnis_id == "111": return "Q111"
            if gnis_id == "222": return "Q222"
            return None
        mock_find_wikidata.side_effect = side_effect_func

        result = await process_features_concurrently(sample_features, concurrency_limit=2)
        expected = [
            {'osm_id': 1, 'osm_type': 'node', 'tags': {'gnis:feature_id': '111', 'name': 'Feature 1'}, 'matched_wikidata_qid': 'Q111'},
            {'osm_id': 2, 'osm_type': 'way', 'tags': {'gnis:feature_id': '222', 'name': 'Feature 2'}, 'matched_wikidata_qid': 'Q222'},
        ]
        self.assertCountEqual(result, expected)

    @patch('find_osm_features.find_wikidata_entry_by_gnis_id', new_callable=AsyncMock)
    async def test_process_concurrently_some_match_some_none(self, mock_find_wikidata):
        sample_features = [
            {"id": 1, "type": "node", "tags": {"gnis:feature_id": "111"}},
            {"id": 2, "type": "node", "tags": {"gnis:feature_id": "222"}},
            {"id": 3, "type": "node", "tags": {"gnis:feature_id": "333"}},
        ]
        async def side_effect_func(session, gnis_id): # Make side_effect async
            if gnis_id == "111": return "Q111"
            if gnis_id == "333": return "Q333"
            return None
        mock_find_wikidata.side_effect = side_effect_func

        result = await process_features_concurrently(sample_features, concurrency_limit=2)
        expected = [
            {'osm_id': 1, 'osm_type': 'node', 'tags': {'gnis:feature_id': '111'}, 'matched_wikidata_qid': 'Q111'},
            {'osm_id': 3, 'osm_type': 'node', 'tags': {'gnis:feature_id': '333'}, 'matched_wikidata_qid': 'Q333'},
        ]
        self.assertCountEqual(result, expected)

    @patch('find_osm_features.find_wikidata_entry_by_gnis_id', new_callable=AsyncMock)
    async def test_process_concurrently_with_exceptions(self, mock_find_wikidata):
        sample_features = [
            {"id": 1, "type": "node", "tags": {"gnis:feature_id": "111"}},
            {"id": 2, "type": "node", "tags": {"gnis:feature_id": "222"}},
            {"id": 3, "type": "node", "tags": {"gnis:feature_id": "333"}},
        ]
        async def side_effect_func(session, gnis_id): # Make side_effect async
            if gnis_id == "111": return "Q111"
            if gnis_id == "222": raise aiohttp.ClientError("Simulated API error")
            return None
        mock_find_wikidata.side_effect = side_effect_func

        result = await process_features_concurrently(sample_features, concurrency_limit=3)
        expected = [
            {'osm_id': 1, 'osm_type': 'node', 'tags': {'gnis:feature_id': '111'}, 'matched_wikidata_qid': 'Q111'},
        ]
        self.assertCountEqual(result, expected)

    @patch('find_osm_features.find_wikidata_entry_by_gnis_id', new_callable=AsyncMock)
    async def test_process_concurrently_chunking(self, mock_find_wikidata):
        sample_features = [
            {"id": i, "type": "node", "tags": {"gnis:feature_id": str(i)*3}} for i in range(1, 6)
        ]
        async def side_effect_func(session, gnis_id): # Make side_effect async
            return f"Q{gnis_id}"
        mock_find_wikidata.side_effect = side_effect_func

        result = await process_features_concurrently(sample_features, concurrency_limit=2)
        self.assertEqual(len(result), 5)
        self.assertEqual(mock_find_wikidata.call_count, 5)
        expected_qids = {f"Q{str(i)*3}" for i in range(1,6)}
        returned_qids = {item['matched_wikidata_qid'] for item in result}
        self.assertSetEqual(returned_qids, expected_qids)

if __name__ == '__main__':
    unittest.main()
