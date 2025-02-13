import unittest
from dbt_run_analyser.dag import DAG
from dbt_run_analyser.node import Node
import polars as pl
from polars.testing import assert_frame_equal
from datetime import timedelta as td

class DAGTest(unittest.TestCase):

    # @classmethod
    # def setUpClass(self):
    #     self.DAG = DAG

    #     e_orders = Node("e_orders", children="stg_orders")
    #     e_orders_legacy = Node("e_orders_legacy", children="stg_orders")
    #     stg_orders = Node("stg_orders", parents=["e_orders", "e_orders_legacy"], children=["fct_orders"])
    #     fct_orders = Node("fct_orders", parents=["stg_orders"])
    #     self.DAG.add_node(e_orders)
    #     self.DAG.add_node(e_orders_legacy)
    #     self.DAG.add_node(stg_orders)
    #     self.DAG.add_node(fct_orders)

    # def test_get_upstream_dependencies(self):
    #     expected = ["stg_orders", "e_orders_legacy", "e_orders"]

    #     actual = self.DAG.get_upstream_dependencies(table_name="fct_orders")
    #     assert actual == expected

    def test_add_node(self):
        fct_orders = Node("fct_orders", parents=["stg_orders"])
        d = DAG()
        d.add_node(fct_orders)

        actual = d.nodes["fct_orders"]
        assert fct_orders == actual

    def test_parent_dict(self):
        fct_orders = Node("fct_orders", parents=["stg_orders"])
        d = DAG()
        d.add_node(fct_orders)

        actual = d.node_parents
        expected = {
            "fct_orders": ["stg_orders"]
        }
        assert expected == actual

    def test_multiple_parents_dict(self):
        fct_orders = Node("fct_orders", parents=["stg_orders", "stg_user"])
        d = DAG()
        d.add_node(fct_orders)

        actual = d.node_parents
        expected = {
            "fct_orders": ["stg_orders", "stg_user"]
        }
        assert expected == actual

    def test_children_dict(self):
        fct_orders = Node("fct_orders", parents=["stg_orders"])
        d = DAG()
        d.add_node(fct_orders)

        actual = d.node_children
        expected = {
            "stg_orders": ["fct_orders"]
        }
        assert expected == actual

    def test_multiple_children_dict(self):
        fct_orders = Node("fct_orders", parents=["stg_orders"])
        fct_order_conversion = Node("fct_order_conversion", parents=["stg_orders"])
        d = DAG()
        d.add_node(fct_orders)
        d.add_node(fct_order_conversion)

        actual = d.node_children
        expected = {
            "stg_orders": ["fct_orders", "fct_order_conversion"]
        }
        assert expected == actual

    def test_add_same_node_name(self):
        fct_orders = Node("fct_orders", parents=["stg_orders"])
        fct_orders2 = Node("fct_orders", parents=["stg_orders2"])
        d = DAG()
        d.add_node(fct_orders)
        d.add_node(fct_orders2)

        actual = d.node_children
        expected = {
            "stg_orders": ["fct_orders"]
        }
        assert expected == actual

    def test_remove_node(self):
        e_orders = Node("e_orders", children="stg_orders")
        e_orders_legacy = Node("e_orders_legacy", children="stg_orders")
        stg_orders = Node("stg_orders", parents=["e_orders", "e_orders_legacy"], children=["fct_orders"])
        fct_orders = Node("fct_orders", parents=["stg_orders"])
        d = DAG()
        d.add_node(e_orders)
        d.add_node(e_orders_legacy)
        d.add_node(stg_orders)
        d.add_node(fct_orders)

        d.remove_node(fct_orders)

        actual_node_children = d.node_children
        expected_node_children = {
            "e_orders": ["stg_orders"],
            "e_orders_legacy": ["stg_orders"],
            "stg_orders": []
        }

        actual_node_parents = d.node_parents
        expected_node_parents = {
            "e_orders": None,
            "e_orders_legacy": None,
            "stg_orders": ["e_orders", "e_orders_legacy"],
        }
        
        assert expected_node_children == actual_node_children
        assert expected_node_parents == actual_node_parents

    def test_upstream_dependencies(self):
        e_orders = Node("e_orders", children="stg_orders")
        e_orders_legacy = Node("e_orders_legacy", children="stg_orders")
        stg_orders = Node("stg_orders", parents=["e_orders", "e_orders_legacy"], children=["fct_orders"])
        fct_orders = Node("fct_orders", parents=["stg_orders"])
        order_conversion = Node("order_conversion", parents=["fct_orders", "stg_orders"])
        d = DAG()
        d.add_node(e_orders)
        d.add_node(e_orders_legacy)
        d.add_node(stg_orders)
        d.add_node(fct_orders)
        d.add_node(order_conversion)

        expected = set(["fct_orders", "stg_orders", "e_orders_legacy", "e_orders"])
        actual = set(d.get_upstream_dependencies("order_conversion"))

        assert expected == actual

    def test_bulk_add_nodes(self):
        node_dict = {
            "e_orders": Node("e_orders", children="stg_orders"),
            "e_orders_legacy": Node("e_orders_legacy", children="stg_orders"),
            "stg_orders": Node("stg_orders", parents=["e_orders", "e_orders_legacy"], children=["fct_orders"]),
            "fct_orders": Node("fct_orders", parents=["stg_orders"]),
            "order_conversion": Node("order_conversion", parents=["fct_orders", "stg_orders"]),
        }
        d = DAG()
        d.bulk_add_nodes(nodes=node_dict)

        actual_node_children = d.node_children
        expected_node_children = {
            "e_orders": ["stg_orders"],
            "e_orders_legacy": ["stg_orders"],
            "stg_orders": ["fct_orders", "order_conversion"],
            "fct_orders": ["order_conversion"]
        }

        actual_node_parents = d.node_parents
        expected_node_parents = {
            "e_orders": None,
            "e_orders_legacy": None,
            "stg_orders": ["e_orders", "e_orders_legacy"],
            "fct_orders": ["stg_orders"],
            "order_conversion": ["fct_orders", "stg_orders"]
        }

        assert expected_node_children == actual_node_children
        assert expected_node_parents == actual_node_parents

    def test_find_paths(self):
        node_dict = {
            "e_orders": Node("e_orders", children="stg_orders", run_time=2),
            "e_orders_legacy": Node("e_orders_legacy", children="stg_orders", run_time=3),
            "stg_orders": Node("stg_orders", parents=["e_orders", "e_orders_legacy"], children=["fct_orders"], run_time=1),
            "fct_orders": Node("fct_orders", parents=["e_orders_legacy"], run_time=10),
            "order_conversion": Node("order_conversion", parents=["fct_orders", "stg_orders"], run_time=2),
        }
        d = DAG()
        d.bulk_add_nodes(nodes=node_dict)
        actual = d.find_all_paths_to_node("order_conversion")
        expected = [['e_orders_legacy', 'fct_orders', 'order_conversion'], ['e_orders', 'stg_orders', 'order_conversion'], ['e_orders_legacy', 'stg_orders', 'order_conversion']]
        self.assertEqual(expected, actual)

    def test_get_critical_path(self):
        node_dict = {
            "e_orders": Node("e_orders", children="stg_orders", run_time=2),
            "e_orders_legacy": Node("e_orders_legacy", children="stg_orders", run_time=3),
            "stg_orders": Node("stg_orders", parents=["e_orders", "e_orders_legacy"], children=["fct_orders"], run_time=1),
            "fct_orders": Node("fct_orders", parents=["e_orders_legacy"], run_time=10),
            "order_conversion": Node("order_conversion", parents=["fct_orders", "stg_orders"], run_time=2),
        }
        d = DAG()
        d.bulk_add_nodes(nodes=node_dict)
        actual = d.get_critial_path("order_conversion")
        expected = {
            'e_orders_legacy': {
                'path': ['e_orders_legacy', 'stg_orders', 'order_conversion'], 
                'total_run_time': 6, 
                'run_time_dict': {
                    'e_orders_legacy': 3, 
                    'order_conversion': 2, 
                    'stg_orders': 1
                }
            }, 
            'e_orders': {
                'path': ['e_orders', 'stg_orders', 'order_conversion'], 
                'total_run_time': 5, 
                'run_time_dict': {
                    'e_orders': 2, 
                    'order_conversion': 2, 
                    'stg_orders': 1
                }
            }
        }
        self.assertEqual(expected, actual)

    def test_init_manifest_to_nodes(self):
        MANIFEST_PATH = "test_data/manifest/manifest.json"
        d = DAG(manifest_path=MANIFEST_PATH)
        actual = set(list(d.nodes.keys()))
        expected = set([
            "e_order_event_1", "e_order_event_2", "e_order_event_3", "e_order_event_4", "e_order_event_5", "e_order_event_6", "e_order_event_7",             
            "stg_order_some", "stg_order",
            "dim_customer", "dim_store", "fct_order", "order_wide",
        ])
        self.assertEqual(expected, actual)

    def test_manifest_to_nodes(self):
        MANIFEST_PATH = "test_data/manifest/manifest.json"
        d = DAG()
        d.manifest_to_nodes(MANIFEST_PATH)
        actual = set(list(d.nodes.keys()))
        expected = set([
            "e_order_event_1", "e_order_event_2", "e_order_event_3", "e_order_event_4", "e_order_event_5", "e_order_event_6", "e_order_event_7",             
            "stg_order_some", "stg_order",
            "dim_customer", "dim_store", "fct_order", "order_wide",
        ])
        self.assertEqual(expected, actual)

    def test_init_log_file(self):
        d = DAG(log_file="test_data/cli_output/dbt_1_thread.log")
        expected_run_time = {
            "e_order_event_1": 3.99, 
            "e_order_event_2": 1.99
        }
        for model, expected_run_time in expected_run_time.items():
            self.assertEqual(expected_run_time, d.get_run_time(model))

    def test_add_run_time(self):
        d = DAG()
        d.log_to_run_time("test_data/cli_output/dbt_1_thread.log")
        expected_run_time = {
            "e_order_event_1": 3.99, 
            "e_order_event_2": 1.99
        }
        for model, expected_run_time in expected_run_time.items():
            self.assertEqual(expected_run_time, d.get_run_time(model))

    def test_model_without_runtime(self):
        d = DAG()
        d.log_to_run_time("test_data/cli_output/dbt_1_thread.log")
        self.assertIsNone(d.get_run_time("some_random_non_existing_model"))

    def test_estimate_thread_1(self):
        d = DAG(
            manifest_path="test_data/manifest/manifest.json", 
            log_file="test_data/cli_output/dbt_1_thread.log"
        )
        actual = d._estimate_thread(d.df).head(5)
        expected = pl.DataFrame(data={
            "model_name": ["e_order_event_1","e_order_event_2","e_order_event_3","e_order_event_4","e_order_event_5"],
            "run_time": [3.99, 1.99, 3.99, 3.99, 5.99],
            "relative_start_time": [0, 4, 6, 10, 14],
            "relative_end_time": [3.99, 5.99, 9.99, 13.99, 19.99],
            "thread": [0, 0, 0, 0, 0]
        })
        assert_frame_equal(expected, actual, check_dtypes=False)

    
    def test_estimate_thread_2(self):
        d = DAG(
            manifest_path="test_data/manifest/manifest.json", 
            log_file="test_data/cli_output/dbt_2_thread.log"
        )
        actual = d._estimate_thread(d.df).head(5)
        expected = pl.DataFrame(data={
            "model_name": ["e_order_event_1","e_order_event_2","e_order_event_3","e_order_event_4","e_order_event_5"],
            "run_time": [2.99, 3.99, 3.99, 3.99, 4.99],
            "relative_start_time": [0, 0, 3, 4, 7],
            "relative_end_time": [2.99, 3.99, 6.99, 7.99, 11.99],
            "thread": [0, 1, 0, 1, 0]
        })
        print(actual)
        assert_frame_equal(expected, actual, check_dtypes=False)

    def test_to_df_all(self):
        d = DAG(
            manifest_path="test_data/manifest/manifest.json", 
            log_file="test_data/cli_output/dbt_1_thread.log"
        )
        actual = d.to_df().head(5)
        expected = pl.DataFrame(data={
            "model_name": ["e_order_event_1","e_order_event_2","e_order_event_3","e_order_event_4","e_order_event_5"],
            "run_time": [3.99, 1.99, 3.99, 3.99, 5.99],
            "relative_start_time": [0, 4, 6, 10, 14],
            "relative_end_time": [3.99, 5.99, 9.99, 13.99, 19.99],
            "thread": [0, 0, 0, 0, 0]
        })
        print(actual)
        assert_frame_equal(expected, actual, check_dtypes=False)

    def test_to_df_critical_path(self):
        d = DAG(
            manifest_path="test_data/manifest/manifest.json", 
            log_file="test_data/cli_output/dbt_1_thread.log"
        )
        actual = d.to_df(critical_path_model="order_wide")
        expected = pl.DataFrame(data={
            "model_name": ["e_order_event_7","stg_order","fct_order","order_wide"],
            "run_time": [6.99, 9.99, 4.99, 11.99],
            "relative_start_time": [0, 13, 33, 38],
            "relative_end_time": [6.99, 22.99, 37.99, 49.99],
            "thread": [0, 0, 0, 0]
        })
        assert_frame_equal(expected, actual, check_dtypes=False)