import unittest
from dbt_run_analyser.utils.log_parser import LogParser
import polars as pl
from polars.testing import assert_frame_equal
from datetime import timedelta as td
import datetime


class ManifestParserTest(unittest.TestCase):
    def setUp(self):
        self.CLI_PATH_TO_1_THREAD_LOG = "test_data/cli_output/dbt_1_thread.log"
        self.CLI_PATH_TO_1_THREAD_LOG_DATETIME = (
            "test_data/cli_output/dbt_1_thread_datetime.log"
        )
        self.CLI_PATH_TO_2_THREAD_LOG = "test_data/cli_output/dbt_2_thread.log"
        self.CLI_PATH_TO_3_THREAD_LOG = "test_data/cli_output/dbt_3_thread.log"
        self.CLI_PATH_TO_4_THREAD_LOG = "test_data/cli_output/dbt_4_thread.log"

    def test_read_log(self):
        cli_log_lines = LogParser(self.CLI_PATH_TO_1_THREAD_LOG)._read_log()

        self.assertIsInstance(cli_log_lines, str)

    def test_parse_timestamp_datetime(self):
        expected = datetime.datetime(2025, 1, 1, 20, 56, 23)
        s = "2025-02-05T20:56:23+0000 [base] 20:56:23  1 of 13 OK created incremental table model main_event.e_order_event_1 ............... [OK in 3.92s]"
        actual = LogParser(self.CLI_PATH_TO_1_THREAD_LOG)._parse_timestamp(s)
        self.assertEqual(expected, actual)

    def test_parse_timestamp(self):
        expected = datetime.datetime(2025, 1, 1, 20, 56, 23)
        s = "20:56:23  1 of 13 OK created incremental table model main_event.e_order_event_1 ............... [OK in 3.92s]"
        actual = LogParser(self.CLI_PATH_TO_1_THREAD_LOG)._parse_timestamp(s)
        self.assertEqual(expected, actual)

    def test_parse_model_name_incremental(self):
        expected = "e_order_event_1"
        s = "20:56:23  1 of 13 OK created incremental table model main_event.e_order_event_1 ............... [OK in 3.92s]"
        actual = LogParser(self.CLI_PATH_TO_1_THREAD_LOG)._parse_model_name(s)
        self.assertEqual(expected, actual)

    def test_parse_model_name_python(self):
        expected = "e_order_event_1"
        s = "20:56:23  1 of 13 OK created python table model some_schema.e_order_event_1 ............... [OK in 3.92s]"
        actual = LogParser(self.CLI_PATH_TO_1_THREAD_LOG)._parse_model_name(s)
        self.assertEqual(expected, actual)

    def test_parse_model_name_view(self):
        expected = "e_order_event_1"
        s = "2025-02-05T20:56:23+0000 [base] 20:56:23  1 of 13 OK created view table model mart.e_order_event_1 ........ [OK in 3.92s]"
        actual = LogParser(self.CLI_PATH_TO_1_THREAD_LOG)._parse_model_name(s)
        self.assertEqual(expected, actual)

    def test_log_parser(self):
        expected = pl.DataFrame(
            data={
                "model_name": [
                    "e_order_event_1",
                    "e_order_event_2",
                    "e_order_event_3",
                    "e_order_event_4",
                    "e_order_event_5",
                ],
                "run_time": [3.99, 1.99, 3.99, 3.99, 5.99],
                "relative_start_time": [0, 4, 6, 10, 14],
                "relative_end_time": [3.99, 5.99, 9.99, 13.99, 19.99],
            }
        )
        actual = LogParser(self.CLI_PATH_TO_1_THREAD_LOG).parse_logs().head(5)
        assert_frame_equal(expected, actual)

    def test_log_parser_datetime(self):
        expected = pl.DataFrame(
            data={
                "model_name": [
                    "e_order_event_1",
                    "e_order_event_2",
                    "e_order_event_3",
                    "e_order_event_4",
                    "e_order_event_5",
                ],
                "run_time": [3.99, 1.99, 3.99, 3.99, 5.99],
                "relative_start_time": [0, 4, 6, 10, 14],
                "relative_end_time": [3.99, 5.99, 9.99, 13.99, 19.99],
            }
        )
        actual = LogParser(self.CLI_PATH_TO_1_THREAD_LOG_DATETIME).parse_logs().head(5)
        assert_frame_equal(expected, actual)
