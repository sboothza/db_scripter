import unittest
from typing import List
from toposort import toposort_flatten


class Dependency:
    name: str
    references: str

    def __init__(self, name: str, ref: str):
        self.name = name
        self.references = ref


class TestDependency(unittest.TestCase):

    def setUp(self):
        ...

    def calculate_dependencies(self, tables: List[str], deps: List[Dependency]) -> List[str]:
        dependencies = {d.name:[] for d in deps}
        for item in deps:
            dependencies[item.name].append(item.references)

        graph = dict(zip(dependencies.keys(), map(set, dependencies.values())))
        sorted_graph = toposort_flatten(graph, sort=True)

        remaining_list = [item for item in tables if item not in sorted_graph]

        sorted_graph.extend(remaining_list)
        return sorted_graph

    def test_no_dependencies(self):
        table_list: List[str] = ["table1", "table2", "table3"]
        table_dependencies: List[Dependency] = []

        ordered_tables = self.calculate_dependencies(table_list, table_dependencies)

        self.assertEqual(len(ordered_tables), 3)
        self.assertEqual("table1", ordered_tables[0])
        self.assertEqual("table2", ordered_tables[1])
        self.assertEqual("table3", ordered_tables[2])

    def test_one_dependency(self):
        table_list: List[str] = ["table1", "table2", "table3"]
        table_dependencies: List[Dependency] = [Dependency("table1", "table2")]

        ordered_tables = self.calculate_dependencies(table_list, table_dependencies)

        self.assertEqual(len(ordered_tables), 3)
        self.assertEqual("table2", ordered_tables[0])
        self.assertEqual("table1", ordered_tables[1])
        self.assertEqual("table3", ordered_tables[2])

    def test_many_dependencies(self):
        table_list: List[str] = ["table1", "table2", "table3"]
        table_dependencies: List[Dependency] = [Dependency("table1", "table2"), Dependency("table2", "table3"), Dependency("table1", "table3")]

        ordered_tables = self.calculate_dependencies(table_list, table_dependencies)

        self.assertEqual(len(ordered_tables), 3)
        self.assertEqual("table3", ordered_tables[0])
        self.assertEqual("table2", ordered_tables[1])
        self.assertEqual("table1", ordered_tables[2])
