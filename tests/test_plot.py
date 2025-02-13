import unittest
from dbt_run_analyser.plot import ShowDBTRun
import polars as pl
from datetime import timedelta as td

class DAGTest(unittest.TestCase):

    def test_no_data(self):
        s = ShowDBTRun(
            manifest_path="test_data/manifest/manifest.json", 
            log_file="test_data/cli_output/dbt_1_thread.log"
        )
        s.df = pl.DataFrame()
        with self.assertRaises(Exception):
            s.plot_run_time()
    
    def test_add_run_time(self):
        s = ShowDBTRun(
            manifest_path="test_data/manifest/manifest.json", 
            log_file="test_data/cli_output/dbt_1_thread.log"
        )
        s._add_run_time(
            thread=0,
            start=0,
            end=2,
            fillcolor="grey",
            model_name="Model"
        )
        self.assertEqual(1, len(s.figure.layout.shapes))

    def test_add_run_times(self):
        s = ShowDBTRun(
            manifest_path="test_data/manifest/manifest.json", 
            log_file="test_data/cli_output/dbt_1_thread.log"
        )
        s.df = pl.DataFrame(data={
            "model_name": ["e_order_event_7","stg_order","fct_order","order_wide"],
            "run_time": [6.99, 10.99, 4.99, 11.99],
            "relative_start_time": [0, 7, 18, 23],
            "relative_end_time": [6.99, 17.99, 22.99, 34.99],
            "thread": [0, 0, 0, 0]
        })
        s.plot_run_time()
        self.assertEqual(4, len(s.figure.layout.shapes))

    def test_run_time_cutoff(self):
        s = ShowDBTRun(
            manifest_path="test_data/manifest/manifest.json", 
            log_file="test_data/cli_output/dbt_1_thread.log"
        )
        s.df = pl.DataFrame(data={
            "model_name": ["e_order_event_7","stg_order","fct_order","order_wide"],
            "run_time": [6.99, 10.99, 4.99, 11.99],
            "relative_start_time": [0, 7, 18, 23],
            "relative_end_time": [6.99, 17.99, 22.99, 34.99],
            "thread": [0, 0, 0, 0]
        })
        s.plot_run_time(run_time_starting_point=10)
        self.assertEqual(2, len(s.figure.layout.shapes))

    def test_highlight_node(self):
        s = ShowDBTRun(
            manifest_path="test_data/manifest/manifest.json", 
            log_file="test_data/cli_output/dbt_1_thread.log"
        )
        s.df = pl.DataFrame(data={
            "model_name": ["e_order_event_7","stg_order","fct_order","order_wide"],
            "run_time": [6.99, 10.99, 4.99, 11.99],
            "relative_start_time": [0, 7, 18, 23],
            "relative_end_time": [6.99, 17.99, 22.99, 34.99],
            "thread": [0, 0, 0, 0]
        })
        fig = s.plot_run_time()
        s._highlight_node("e_order_event_7")
        actual_shape_colors = [shape.fillcolor for shape in fig.layout.shapes]
        expected_shape_colors = ["#940C08", "#c2c2c2", "#c2c2c2", "#c2c2c2"]
        self.assertEqual(expected_shape_colors, actual_shape_colors)

    def test_highlight_critical_path(self):
        s = ShowDBTRun(
            manifest_path="test_data/manifest/manifest.json", 
            log_file="test_data/cli_output/dbt_1_thread.log"
        )
        s.df = pl.DataFrame(data={
            "model_name": ["e_order_event_7","stg_order","fct_order","order_wide"],
            "run_time": [6.99, 10.99, 4.99, 11.99],
            "relative_start_time": [0, 7, 18, 23],
            "relative_end_time": [6.99, 17.99, 22.99, 34.99],
            "thread": [0, 0, 0, 0]
        })
        s.plot_critical_path("e_order_event_7")
        actual_shape_colors = [shape.fillcolor for shape in s.figure.layout.shapes]
        expected_shape_colors = ["#940C08", "#c2c2c2", "#c2c2c2", "#c2c2c2"]
        self.assertEqual(expected_shape_colors, actual_shape_colors)