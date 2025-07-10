import unittest
import os
import json
import sys
import gzip # Added for .pkl.gz
import pickle # Added for .pkl.gz

# Ensure find_osm_features.py can be imported (assuming it's in the parent directory).
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from find_osm_features import load_progress, _save_current_progress # Import _save_current_progress for testing saves

class TestResumeFunctionality(unittest.TestCase):

    resume_file_path = "resume_state.pkl.gz" # Updated file extension

    def setUp(self):
        # Ensure the resume file does not exist before each test.
        if os.path.exists(self.resume_file_path):
            os.remove(self.resume_file_path)

    def tearDown(self):
        # Clean up the resume file after each test.
        if os.path.exists(self.resume_file_path):
            os.remove(self.resume_file_path)

    def test_load_progress_no_file(self):
        """Test load_progress when the resume file does not exist."""
        self.assertFalse(os.path.exists(self.resume_file_path)) # Pre-condition check

        loaded_raw_overpass, loaded_features, loaded_results = load_progress()

        self.assertIsNone(loaded_raw_overpass)
        self.assertEqual(loaded_features, [])
        self.assertEqual(loaded_results, [])

    def test_load_progress_valid_file(self):
        """Test load_progress with a valid resume file."""
        expected_raw_overpass = [{"type": "node", "id": 100, "tags": {"gnis:feature_id": "raw1"}}]
        expected_features = [{"id": 1, "type": "node", "tags": {"gnis:feature_id": "123"}}]
        expected_results = [{"osm_type": "node", "osm_id": 1, "name": "Test Feature", "gnis_id": "123", "wikidata_id": "Q456"}]

        # Use _save_current_progress to create the resume file
        _save_current_progress(expected_raw_overpass, expected_features, expected_results)

        loaded_raw_overpass, loaded_features, loaded_results = load_progress()

        self.assertEqual(loaded_raw_overpass, expected_raw_overpass)
        self.assertEqual(loaded_features, expected_features)
        self.assertEqual(loaded_results, expected_results)

    def test_load_progress_corrupted_file_invalid_pickle(self):
        """Test load_progress with a corrupted (invalid pickle) resume file."""
        with gzip.open(self.resume_file_path, 'wb') as f:
            f.write(b"this is not valid pickle data") # Invalid pickle data

        loaded_raw_overpass, loaded_features, loaded_results = load_progress()

        self.assertIsNone(loaded_raw_overpass)
        self.assertEqual(loaded_features, [])
        self.assertEqual(loaded_results, [])
        self.assertFalse(os.path.exists(self.resume_file_path)) # Corrupted file should be deleted.

    def test_load_progress_corrupted_file_wrong_structure(self):
        """Test load_progress with a file that is valid pickle but has wrong data structure (e.g., missing keys)."""
        mock_data = {"not_features": [], "not_results": [], "not_raw_overpass": None} # Valid pickle, wrong keys

        with gzip.open(self.resume_file_path, 'wb') as f:
            pickle.dump(mock_data, f)

        loaded_raw_overpass, loaded_features, loaded_results = load_progress()

        # .get with default [] or None handles missing keys gracefully.
        self.assertIsNone(loaded_raw_overpass)
        self.assertEqual(loaded_features, [])
        self.assertEqual(loaded_results, [])
        # File is valid pickle, so it's not deleted by load_progress's corruption check for unpickling errors.
        # The check for `isinstance(list)` will pass for `[]`.
        self.assertTrue(os.path.exists(self.resume_file_path))

    def test_load_progress_empty_lists_and_none_raw_in_file(self):
        """Test load_progress with a valid file containing empty lists and None for raw_overpass_data."""
        expected_raw_overpass = None
        expected_features = []
        expected_results = []

        _save_current_progress(expected_raw_overpass, expected_features, expected_results)

        loaded_raw_overpass, loaded_features, loaded_results = load_progress()

        self.assertIsNone(loaded_raw_overpass)
        self.assertEqual(loaded_features, expected_features)
        self.assertEqual(loaded_results, expected_results)

    def test_load_progress_missing_some_keys(self):
        """Test load_progress when some keys are missing in the pickled data."""
        # Case 1: 'results' key missing
        mock_data_missing_results = {
            "raw_overpass_data_cache": [{"id": "raw"}],
            "features_to_check": [{"id": 1}]
        }
        with gzip.open(self.resume_file_path, 'wb') as f:
            pickle.dump(mock_data_missing_results, f)
        loaded_raw, loaded_feat, loaded_res = load_progress()
        self.assertEqual(loaded_raw, [{"id": "raw"}])
        self.assertEqual(loaded_feat, [{"id": 1}])
        self.assertEqual(loaded_res, []) # Defaults to empty list
        os.remove(self.resume_file_path) # Clean up for next sub-test

        # Case 2: 'features_to_check' key missing
        mock_data_missing_features = {
            "raw_overpass_data_cache": [{"id": "raw2"}],
            "results": [{"id": 2}]
        }
        with gzip.open(self.resume_file_path, 'wb') as f:
            pickle.dump(mock_data_missing_features, f)
        loaded_raw, loaded_feat, loaded_res = load_progress()
        self.assertEqual(loaded_raw, [{"id": "raw2"}])
        self.assertEqual(loaded_feat, []) # Defaults to empty list
        self.assertEqual(loaded_res, [{"id": 2}])
        os.remove(self.resume_file_path)

        # Case 3: 'raw_overpass_data_cache' key missing
        mock_data_missing_raw = {
            "features_to_check": [{"id": 3}],
            "results": [{"id": 4}]
        }
        with gzip.open(self.resume_file_path, 'wb') as f:
            pickle.dump(mock_data_missing_raw, f)
        loaded_raw, loaded_feat, loaded_res = load_progress()
        self.assertIsNone(loaded_raw) # Defaults to None
        self.assertEqual(loaded_feat, [{"id": 3}])
        self.assertEqual(loaded_res, [{"id": 4}])


    def test_load_progress_file_with_non_list_data_for_features_or_results(self):
        """Test load_progress when file contains non-list data for features_to_check or results."""
        mock_data_non_list_features = {
            "raw_overpass_data_cache": None,
            "features_to_check": {"id": 1}, # Should be a list
            "results": []
        }
        with gzip.open(self.resume_file_path, 'wb') as f:
            pickle.dump(mock_data_non_list_features, f)
        loaded_raw, loaded_feat, loaded_res = load_progress()
        self.assertIsNone(loaded_raw)
        self.assertEqual(loaded_feat, []) # Default due to corruption
        self.assertEqual(loaded_res, []) # Default due to corruption
        self.assertFalse(os.path.exists(self.resume_file_path), "Corrupted file (non-list features) should be deleted.")

        # Reset for next sub-test
        if os.path.exists(self.resume_file_path): os.remove(self.resume_file_path)

        mock_data_non_list_results = {
            "raw_overpass_data_cache": None,
            "features_to_check": [],
            "results": {"id": 2} # Should be a list
        }
        with gzip.open(self.resume_file_path, 'wb') as f:
            pickle.dump(mock_data_non_list_results, f)
        loaded_raw, loaded_feat, loaded_res = load_progress()
        self.assertIsNone(loaded_raw)
        self.assertEqual(loaded_feat, []) # Default due to corruption
        self.assertEqual(loaded_res, []) # Default due to corruption
        self.assertFalse(os.path.exists(self.resume_file_path), "Corrupted file (non-list results) should be deleted.")


if __name__ == '__main__':
    unittest.main()
