import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import json
import asyncio
import aiohttp
import os
import sys

# Ensure find_osm_features.py can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from find_osm_features import OsmWikidataProcessor

class TestOsmProcessing(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        """Set up a processor instance for each test."""
        # Mock config file loading
        with patch.object(OsmWikidataProcessor, '_load_config', return_value={
            "BASE_OVERPASS_QUERY": "query_template_{timeout}",
            "SPARQL_QUERY": "sparql_template_{gnis_id}"
        }):
            self.processor = OsmWikidataProcessor()

    @patch('find_osm_features.OsmWikidataProcessor._fetch_osm_features_with_gnis_id')
    def test_fetch_data_success(self, mock_fetch):
        """Test the fetch_data method for successful data retrieval."""
        mock_fetch.return_value = [{"id": 1, "type": "node"}]
        self.processor.fetch_data(query_timeout=30)
        self.assertEqual(self.processor.raw_osm_data, [{"id": 1, "type": "node"}])
        mock_fetch.assert_called_once_with(30)

    def test_prepare_data_no_data(self):
        """Test prepare_data with no raw data."""
        self.processor.raw_osm_data = []
        self.processor.prepare_data()
        self.assertEqual(self.processor.features_to_process, [])

    def test_prepare_data_with_deduplication(self):
        """Test prepare_data correctly separates features."""
        self.processor.raw_osm_data = [
            {"id": 1, "tags": {"gnis:feature_id": "123"}},      # To process
            {"id": 2, "tags": {"gnis:feature_id": "456"}},      # Shared GNIS
            {"id": 3, "tags": {"gnis:feature_id": "456"}},      # Shared GNIS
            {"id": 4, "tags": {"gnis:feature_id": "789;abc"}}, # Multi-ID
        ]

        with patch.object(self.processor, '_save_data_atomically') as mock_save:
            self.processor.prepare_data()

            self.assertEqual(len(self.processor.features_to_process), 1)
            self.assertEqual(self.processor.features_to_process[0]['id'], 1)

            self.assertEqual(len(self.processor.purged_shared), 2)
            self.assertEqual(self.processor.purged_shared[0]['id'], 2)

            self.assertEqual(len(self.processor.purged_multi_id), 1)
            self.assertEqual(self.processor.purged_multi_id[0]['id'], 4)

            # Check that save was called for purged files
            self.assertEqual(mock_save.call_count, 2)

    @patch('find_osm_features.OsmWikidataProcessor._process_features_concurrently', new_callable=AsyncMock)
    async def test_process_features(self, mock_process_concurrently):
        """Test the main process_features method."""
        self.processor.features_to_process = [{"id": 1}]
        await self.processor.process_features()
        mock_process_concurrently.assert_called_once()

    @patch('find_osm_features.OsmWikidataProcessor._save_data_atomically')
    async def test_save_results(self, mock_save):
        """Test the save_results method."""
        self.processor.results = [{"osm_id": 1}]
        await self.processor.save_results()
        mock_save.assert_called_once()
        # Verify it saves the sorted list to the correct file
        mock_save.assert_called_with(
            [{"osm_id": 1}], self.processor.final_results_filename, unittest.mock.ANY
        )

    @patch('find_osm_features.OsmWikidataProcessor._find_wikidata_entry_by_gnis_id', new_callable=AsyncMock)
    async def test_concurrent_processing_logic(self, mock_find_wikidata):
        """Test the internal concurrent processing logic."""
        self.processor.features_to_process = [
            {"id": 1, "type": "node", "tags": {"gnis:feature_id": "111"}},
            {"id": 2, "type": "node", "tags": {"gnis:feature_id": "222"}},
        ]

        async def side_effect(session, gnis_id):
            return f"Q{gnis_id}"
        mock_find_wikidata.side_effect = side_effect

        await self.processor._process_features_concurrently()

        self.assertEqual(len(self.processor.results), 2)
        expected_results = [
            {'osm_type': 'node', 'osm_id': 1, 'gnis_id': '111', 'wikidata_id': 'Q111'},
            {'osm_type': 'node', 'osm_id': 2, 'gnis_id': '222', 'wikidata_id': 'Q222'},
        ]
        self.assertCountEqual(self.processor.results, expected_results)

if __name__ == '__main__':
    unittest.main()
