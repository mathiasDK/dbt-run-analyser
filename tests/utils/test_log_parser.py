import unittest
from src.utils.log_parser import LogParser
import polars as pl

class ManifestParserTest(unittest.TestCase):
    def setUp(self):
        self.CLI_PATH_TO_1_THREAD_LOG = "test_data/cli_output/dbt_1_thread.log"
        self.CLI_PATH_TO_2_THREAD_LOG = "test_data/cli_output/dbt_2_thread.log"
        self.CLI_PATH_TO_3_THREAD_LOG = "test_data/cli_output/dbt_3_thread.log"
        self.CLI_PATH_TO_4_THREAD_LOG = "test_data/cli_output/dbt_4_thread.log"

        self.DEBUG_PATH_TO_1_THREAD_LOG = "test_data/debug/dbt_1_thread.log"
        self.DEBUG_PATH_TO_2_THREAD_LOG = "test_data/debug/dbt_2_thread.log"
        self.DEBUG_PATH_TO_3_THREAD_LOG = "test_data/debug/dbt_3_thread.log"
        self.DEBUG_PATH_TO_4_THREAD_LOG = "test_data/debug/dbt_4_thread.log"

    def test_read_log(self):
        cli_log_lines = LogParser(self.CLI_PATH_TO_1_THREAD_LOG)._read_log()
        debug_log_lines = LogParser(self.DEBUG_PATH_TO_1_THREAD_LOG)._read_log()

        self.assertIsInstance(cli_log_lines, list)
        self.assertIsInstance(debug_log_lines, list)
        self.assertEqual(len(cli_log_lines), 38)
        self.assertEqual(len(debug_log_lines), 3310)

    def test_log_parser(self):
        expected = pl.DataFrame(data={
            "node": ["e_order_event_1","e_order_event_2","e_order_event_3","e_order_event_4","e_order_event_5","e_order_event_6","e_order_event_7","stg_order_some","stg_order","dim_customer","dim_store","fct_order","order_wide"],
            "run_time": [3.92, 2.46, 3.32, 4.67, 5.37, 6.32, 7.21, 5.22, 10.21, 8.26, 2.27, 4.5, 12.33],
            "relative_start_time": [],
            "relative_end_time": [],
        })
        actual = LogParser(self.CLI_PATH_TO_1_THREAD_LOG)._cli_parser()
        self.assertEqual(expected, actual)

    def test_debug_parser(self):
        expected = pl.DataFrame(data={
            "node": ["e_order_event_1","e_order_event_2","e_order_event_3","e_order_event_4","e_order_event_5","e_order_event_6","e_order_event_7","stg_order_some","stg_order","dim_customer","dim_store","fct_order","order_wide"],
            "start_time": [],
            "start_time_relative": [],
            "thread": [],
            "runtime": [],
        })
        actual = LogParser(self.DEBUG_PATH_TO_1_THREAD_LOG)._debug_parser()
        self.assertEqual(expected, actual)

    
    