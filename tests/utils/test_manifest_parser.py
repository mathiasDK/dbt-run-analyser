import unittest
from dbt_run_analyser.utils.manifest_parser import manifest_parser

class ManifestParserTest(unittest.TestCase):
    def setUp(self):
        self.PATH_TO_MANIFEST = "test_data/manifest/manifest.json"

    def test_expected_output(self):
        expected = {
            "e_order_event_1": None,
            "e_order_event_2": None,
            "e_order_event_3": None,
            "e_order_event_4": None,
            "e_order_event_5": None,
            "e_order_event_6": None,
            "e_order_event_7": None,
            
            "stg_order_some": ["e_order_event_1", "e_order_event_2", "e_order_event_3"],
            "stg_order": ["e_order_event_1", "e_order_event_2", "e_order_event_3", "e_order_event_4", "e_order_event_5", "e_order_event_6", "e_order_event_7"],

            "dim_customer": ["stg_order"],
            "dim_store": ["stg_order"],
            "fct_order": ["stg_order"],
            "order_wide": ["dim_customer", "dim_store", "fct_order"]
        }

        actual = manifest_parser(self.PATH_TO_MANIFEST)

        self.assertEqual(actual, expected)