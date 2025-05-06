from pathlib import Path

import compgraph.operations as ops
import pytest
from compgraph.graph import Graph
from compgraph.operations import DummyMapper, FirstReducer


def test_graph_from_iter() -> None:
    graph = Graph.graph_from_iter("data")
    data = [{"a": 1}, {"a": 2}]
    result = list(graph.run(data=lambda: iter(data)))
    assert result == data


def test_graph_from_file(tmp_path: Path) -> None:
    file_path = tmp_path / "test_file.txt"
    file_path.write_text("{'a': 1}\n{'a': 2}\n")
    graph = Graph.graph_from_file(str(file_path), lambda x: eval(x))
    result = list(graph.run())
    assert result == [{"a": 1}, {"a": 2}]


def test_map() -> None:
    graph = Graph().graph_from_iter("data").map(DummyMapper())
    data = [{"a": 1}, {"a": 2}]
    result = list(graph.run(data=lambda: iter(data)))
    assert result == [{"a": 1}, {"a": 2}]


def test_reduce() -> None:
    graph = Graph().graph_from_iter("data").reduce(FirstReducer(), ["a"])
    data = [{"a": "group1", "value": 1}, {"a": "group1", "value": 2}]
    result = list(graph.run(data=lambda: iter(data)))
    assert result == [{"a": "group1", "value": 1}]


def test_sort() -> None:
    graph = Graph().graph_from_iter("data").sort(["a"])
    data = [{"a": 2}, {"a": 1}]
    result = list(graph.run(data=lambda: iter(data)))
    assert result == [{"a": 1}, {"a": 2}]


def test_join() -> None:
    graph1 = Graph.graph_from_iter("data1").map(DummyMapper())
    graph2 = Graph.graph_from_iter("data2")
    graph = graph1.join(ops.InnerJoiner(), graph2, ["a"])
    data1 = [{"a": 1, "b": 2}]
    data2 = [{"a": 1, "c": 3}]
    result = list(graph.run(data1=lambda: iter(data1), data2=lambda: iter(data2)))
    assert result == [{"a": 1, "b": 2, "c": 3}]


def test_run_no_operations() -> None:
    graph = Graph()
    with pytest.raises(Exception):
        graph.run()


def test_multiple_runs() -> None:
    graph = Graph.graph_from_iter("data").map(DummyMapper())
    data1 = [{"a": 1}, {"a": 2}]
    data2 = [{"a": 2}, {"a": 1}]
    result1 = list(graph.run(data=lambda: iter(data1)))
    result2 = list(graph.run(data=lambda: iter(data2)))

    assert result1 != result2
    assert result1 == [{"a": 1}, {"a": 2}]
    assert result2 == [{"a": 2}, {"a": 1}]
