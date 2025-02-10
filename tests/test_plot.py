import unittest
from dbt_run_analyser.plot import ShowDBTRun
import polars as pl
from datetime import timedelta as td

class DAGTest(unittest.TestCase):

    def test_no_data(self):
        s = ShowDBTRun()
        self.assertRaises(Exception, s.plot_run_time())
    
    def test_add_run_time(self):
        s = ShowDBTRun()
        s._add_run_time(
            thread=0,
            start=0,
            end=2,
            fillcolor="grey",
            model_name="Model"
        )
        self.assertEqual(1, len(s.figure.layout.shapes))

    def test_add_run_times(self):
        s = ShowDBTRun()
        s.df = pl.DataFrame(data={
            "model_name": ["e_order_event_7","stg_order","fct_order","order_wide"],
            "run_time": [7.21, 10.21, 4.5, 12.33],
            "relative_start_time": [td(seconds=0), td(seconds=13), td(seconds=33, milliseconds=710), td(seconds=37, milliseconds=880)],
            "relative_end_time": [td(seconds=7, milliseconds=210), td(seconds=23, milliseconds=210), td(seconds=38, milliseconds=210), td(seconds=50, milliseconds=210)],
            "thread": [0, 0, 0, 1]
        })
        s.plot_run_time()
        self.assertEqual(4, len(s.figure.layout.shapes))

    def test_run_time_cutoff(self):
        s = ShowDBTRun()
        s.df = pl.DataFrame(data={
            "model_name": ["e_order_event_7","stg_order","fct_order","order_wide"],
            "run_time": [7.21, 10.21, 4.5, 12.33],
            "relative_start_time": [td(seconds=0), td(seconds=13), td(seconds=33, milliseconds=710), td(seconds=37, milliseconds=880)],
            "relative_end_time": [td(seconds=7, milliseconds=210), td(seconds=23, milliseconds=210), td(seconds=38, milliseconds=210), td(seconds=50, milliseconds=210)],
            "thread": [0, 0, 0, 1]
        })
        s.plot_run_time(run_time_starting_point=td(seconds=10))
        self.assertEqual(3, len(s.figure.layout.shapes))