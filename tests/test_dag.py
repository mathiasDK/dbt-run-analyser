import unittest
from dbt_thread_optimiser.dag import DAG
from dbt_thread_optimiser.node import Node

class DAGTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.DAG = DAG

        e_orders = Node("e_orders", children="stg_orders")
        e_orders_legacy = Node("e_orders_legacy", children="stg_orders")
        stg_orders = Node("stg_orders", parents=["e_orders", "e_orders_legacy"], children=["fct_orders"])
        fct_orders = Node("fct_orders", parents=["stg_orders"])
        self.DAG.add_node(e_orders)
        self.DAG.add_node(e_orders_legacy)
        self.DAG.add_node(stg_orders)
        self.DAG.add_node(fct_orders)

    def test_get_upstream_dependencies(self):
        expected = ["stg_orders", "e_orders_legacy", "e_orders"]

        fct_orders = Node("fct_orders", parents=["stg_orders"])
        actual = self.DAG.get_upstream_dependencies(node=fct_orders)
        assert actual == expected
