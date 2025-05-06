from __future__ import annotations

import typing as tp

from . import external_sort as ex_sort, CompgraphException
from . import operations as ops


class Graph:
    """Computational graph implementation"""

    def __init__(self) -> None:
        self._operations: list[tp.Any] = []
        self._input_data: tp.Any = None

    @staticmethod
    def graph_from_iter(name: str) -> Graph:
        """Construct new graph which reads data from row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow
        Use ops.ReadIterFactory
        :param name: name of kwarg to use as data source
        """
        graph = Graph()
        graph._operations.append(ops.ReadIterFactory(name))
        return graph

    @staticmethod
    def graph_from_another_graph(graph: Graph) -> Graph:
        """
        Construct new graph which is a copy of passed one
        :param graph: graph to copy
        """
        new_graph = Graph()
        new_graph._operations = graph._operations.copy()
        return new_graph

    @staticmethod
    def graph_from_file(filename: str, parser: tp.Callable[[str], ops.TRow]) -> Graph:
        """Construct new graph extended with operation for reading rows from file
        Use ops.Read
        :param filename: filename to read from
        :param parser: parser from string to Row
        """
        graph = Graph()
        graph._operations.append(ops.Read(filename, parser))
        return graph

    def map(self, mapper: ops.Mapper) -> Graph:
        """Construct new graph extended with map operation with particular mapper
        :param mapper: mapper to use
        """
        self._operations.append(ops.Map(mapper))
        return self

    def reduce(self, reducer: ops.Reducer, keys: tp.Sequence[str]) -> Graph:
        """Construct new graph extended with reduce operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        self._operations.append(ops.Reduce(reducer, keys))
        return self

    def sort(self, keys: tp.Sequence[str]) -> Graph:
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        self._operations.append(ex_sort.ExternalSort(keys))
        return self

    def join(self, joiner: ops.Joiner, join_graph: Graph, keys: tp.Sequence[str]) -> Graph:
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        self._operations.append(lambda data: ops.Join(joiner, keys)(data, join_graph.run(**self._input_data)))
        return self

    def run(self, **kwargs: tp.Any) -> ops.TRowsIterable:
        """Single method to start execution; data sources passed as kwargs"""
        self._input_data = kwargs
        if not self._operations:
            raise CompgraphException("No operations in graph")
        data = self._operations[0](**kwargs)
        for operation in self._operations[1::]:
            data = operation(data)
        return data
