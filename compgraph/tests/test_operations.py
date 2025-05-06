import dataclasses
import typing as tp

import pytest

import compgraph.operations as ops


@dataclasses.dataclass
class MapCase:
    mapper: ops.Mapper
    data: list[ops.TRow]
    ground_truth: list[ops.TRow]
    cmp_keys: tuple[str, ...]
    mapper_item: int = 0
    mapper_ground_truth_items: tuple[int, ...] = (0,)


MAP_CASES = [
    MapCase(
        mapper=ops.CalculateTimeAndDistance(result_distance_column="distance", result_time_column='time'),
        data=[
            {"edge_id": 1, "enter_time": "20210101T000000", "leave_time": "20210101T001000", "start": [0, 0],
             "end": [1, 1]},
            {"edge_id": 2, "enter_time": "20210101T000000", "leave_time": "20210101T001000", "start": [0, 0],
             "end": [1, 1]},
            {"edge_id": 3, "enter_time": "20210101T000000", "leave_time": "20210101T001000", "start": [0, 0],
             "end": [1, 1]},
            {"edge_id": 4, "enter_time": "20210101T000000", "leave_time": "20210101T001000", "start": [0, 0],
             "end": [1, 1]}
        ],
        ground_truth=[
            {'edge_id': 1, 'enter_time': '20210101T000000', 'leave_time': '20210101T001000', 'start': [0, 0],
             'end': [1, 1], 'distance': pytest.approx(157.29, 0.1), 'time': pytest.approx(0.16, 0.1), 'weekday': 'Fri',
             'hour': 0},
            {'edge_id': 2, 'enter_time': '20210101T000000', 'leave_time': '20210101T001000', 'start': [0, 0],
             'end': [1, 1], 'distance': pytest.approx(157.29, 0.1), 'time': pytest.approx(0.16, 0.1), 'weekday': 'Fri',
             'hour': 0},
            {'edge_id': 3, 'enter_time': '20210101T000000', 'leave_time': '20210101T001000', 'start': [0, 0],
             'end': [1, 1], 'distance': pytest.approx(157.29, 0.1), 'time': pytest.approx(0.16, 0.1), 'weekday': 'Fri',
             'hour': 0},
            {'edge_id': 4, 'enter_time': '20210101T000000', 'leave_time': '20210101T001000', 'start': [0, 0],
             'end': [1, 1], 'distance': pytest.approx(157.29, 0.1), 'time': pytest.approx(0.16, 0.1), 'weekday': 'Fri',
             'hour': 0},
        ],
        cmp_keys=()
    ),
    MapCase(
        mapper=ops.CalculateTimeAndDistance(result_distance_column="distance", result_time_column='time'),
        data=[
            {"edge_id": 1, "enter_time": "20210101T080000", "leave_time": "20210101T200000", "start": [0, 0],
             "end": [0.5, 2.5]},
            {"edge_id": 2, "enter_time": "20210101T090000", "leave_time": "20210101T180000", "start": [0, 0],
             "end": [0.25, 1]},
            {"edge_id": 3, "enter_time": "20210103T100000", "leave_time": "20210103T150000", "start": [0, 0],
             "end": [0, 3]},
            {"edge_id": 4, "enter_time": "20210101T230000", "leave_time": "20210102T100000", "start": [0, 0],
             "end": [0.856, 1.75]}
        ],
        ground_truth=[
            {'edge_id': 1, 'enter_time': '20210101T080000', 'leave_time': '20210101T200000', 'start': [0, 0],
             'end': [0.5, 2.5], 'distance': pytest.approx(283.5, 0.1), 'time': 12.0, 'weekday': 'Fri', 'hour': 8},
            {'edge_id': 2, 'enter_time': '20210101T090000', 'leave_time': '20210101T180000', 'start': [0, 0],
             'end': [0.25, 1], 'distance': pytest.approx(114.6, 0.1), 'time': 9.0, 'weekday': 'Fri', 'hour': 9},
            {'edge_id': 3, 'enter_time': '20210103T100000', 'leave_time': '20210103T150000', 'start': [0, 0],
             'end': [0, 3], 'distance': pytest.approx(333.6, 0.1), 'time': 5.0, 'weekday': 'Sun', 'hour': 10},
            {'edge_id': 4, 'enter_time': '20210101T230000', 'leave_time': '20210102T100000', 'start': [0, 0],
             'end': [0.856, 1.75], 'distance': pytest.approx(216.6, 0.1), 'time': 11.0, 'weekday': 'Fri', 'hour': 23}
        ],
        cmp_keys=()
    )
]


@pytest.mark.parametrize("case", MAP_CASES)
def test_calculate_time_and_speed(case: MapCase) -> None:
    result = ops.Map(case.mapper)(iter(case.data))
    assert isinstance(result, tp.Iterator)
    for i, line in enumerate(result):
        for key, value in case.ground_truth[i].items():
            assert line[key] == value


MAP_CASES = [
    MapCase(
        mapper=ops.CalculateIdf(total_docs_column="total_docs",
                                docs_with_word_column="docs_with_word",
                                idf_column="idf"),
        data=[
            {"total_docs": 10, "docs_with_word": 1},
            {"total_docs": 10, "docs_with_word": 2},
            {"total_docs": 10, "docs_with_word": 5},
            {"total_docs": 10, "docs_with_word": 10},
        ],
        ground_truth=[
            {"total_docs": 10, "docs_with_word": 1, "idf": pytest.approx(2.302585, 0.000001)},
            {"total_docs": 10, "docs_with_word": 2, "idf": pytest.approx(1.609438, 0.000001)},
            {"total_docs": 10, "docs_with_word": 5, "idf": pytest.approx(0.693147, 0.000001)},
            {"total_docs": 10, "docs_with_word": 10, "idf": pytest.approx(0.0, 0.000001)},
        ],
        cmp_keys=("total_docs", "docs_with_word", "idf")
    ),
    MapCase(
        mapper=ops.CalculateIdf(total_docs_column="total_docs",
                                docs_with_word_column="docs_with_word",
                                idf_column="idf"),
        data=[
            {"total_docs": 73, "docs_with_word": 1},
            {"total_docs": 73, "docs_with_word": 7},
            {"total_docs": 73, "docs_with_word": 19},
            {"total_docs": 73, "docs_with_word": 27},
        ],
        ground_truth=[
            {"total_docs": 73, "docs_with_word": 1, "idf": pytest.approx(4.29, 0.1)},
            {"total_docs": 73, "docs_with_word": 7, "idf": pytest.approx(2.34, 0.1)},
            {"total_docs": 73, "docs_with_word": 19, "idf": pytest.approx(1.34, 0.1)},
            {"total_docs": 73, "docs_with_word": 27, "idf": pytest.approx(0.99, 0.1)},
        ],
        cmp_keys=("total_docs", "docs_with_word", "idf")
    )
]


@pytest.mark.parametrize("case", MAP_CASES)
def test_calculate_idf(case: MapCase) -> None:
    result = ops.Map(case.mapper)(iter(case.data))
    assert isinstance(result, tp.Iterator)
    for i, line in enumerate(result):
        assert line == case.ground_truth[i]


MAP_CASES = [
    MapCase(
        mapper=ops.CalculatePMI(total_docs_column="total_docs",
                                total_words_in_docs="docs_with_word",
                                frequency_column="docs_with_both",
                                pmi_column="pmi"),
        data=[
            {"total_docs": 100, "docs_with_word": 10, "docs_with_both": 1},
            {"total_docs": 100, "docs_with_word": 20, "docs_with_both": 5},
            {"total_docs": 100, "docs_with_word": 50, "docs_with_both": 10},
            {"total_docs": 100, "docs_with_word": 100, "docs_with_both": 50},
        ],
        ground_truth=[
            {"total_docs": 100, "docs_with_word": 10, "docs_with_both": 1, "pmi": pytest.approx(2.302585, 0.000001)},
            {"total_docs": 100, "docs_with_word": 20, "docs_with_both": 5, "pmi": pytest.approx(3.218875, 0.000001)},
            {"total_docs": 100, "docs_with_word": 50, "docs_with_both": 10, "pmi": pytest.approx(2.99573, 0.000001)},
            {"total_docs": 100, "docs_with_word": 100, "docs_with_both": 50, "pmi": pytest.approx(3.91202, 0.000001)},
        ],
        cmp_keys=("total_docs", "docs_with_word", "docs_with_both", "pmi")
    ),
    MapCase(
        mapper=ops.CalculatePMI(total_docs_column="total_docs",
                                total_words_in_docs="docs_with_word",
                                frequency_column="docs_with_both",
                                pmi_column="pmi"),
        data=[
            {"total_docs": 200, "docs_with_word": 20, "docs_with_both": 2},
            {"total_docs": 200, "docs_with_word": 40, "docs_with_both": 8},
            {"total_docs": 200, "docs_with_word": 103, "docs_with_both": 21},
            {"total_docs": 200, "docs_with_word": 199, "docs_with_both": 107},
        ],
        ground_truth=[
            {"total_docs": 200, "docs_with_word": 20, "docs_with_both": 2, "pmi": pytest.approx(2.99573, 0.00001)},
            {"total_docs": 200, "docs_with_word": 40, "docs_with_both": 8, "pmi": pytest.approx(3.68887, 0.00001)},
            {"total_docs": 200, "docs_with_word": 103, "docs_with_both": 21, "pmi": pytest.approx(3.70811, 0.00001)},
            {"total_docs": 200, "docs_with_word": 199, "docs_with_both": 107, "pmi": pytest.approx(4.67784, 0.00001)},
        ],
        cmp_keys=("total_docs", "docs_with_word", "docs_with_both", "pmi")
    )
]


@pytest.mark.parametrize("case", MAP_CASES)
def test_calculate_pmi(case: MapCase) -> None:
    result = ops.Map(case.mapper)(iter(case.data))
    assert isinstance(result, tp.Iterator)
    for i, line in enumerate(result):
        for key, value in case.ground_truth[i].items():
            assert line[key] == value


@dataclasses.dataclass
class ReduceCase:
    reducer: ops.Reducer
    reducer_keys: tuple[str, ...]
    data: list[ops.TRow]
    ground_truth: list[ops.TRow]
    cmp_keys: tuple[str, ...]


REDUCE_CASES = [
    ReduceCase(
        reducer=ops.AverageSpeed(result_column="average"),
        reducer_keys=("group",),
        data=[
            {"group": "C", "time": 3, "distance": 150},
            {"group": "C", "time": 2, "distance": 120},
            {"group": "C", "time": 1, "distance": 70},
            {"group": "D", "time": 3, "distance": 10},
            {"group": "D", "time": 3, "distance": 100},
            {"group": "D", "time": 3, "distance": 1006},
        ],
        ground_truth=[
            {"group": "C", "average": pytest.approx(56.6, 0.1)},
            {"group": "D", "average": 124},
        ],
        cmp_keys=("group", "average")
    )
]


@pytest.mark.parametrize("case", REDUCE_CASES)
def test_calculate_average(case: ReduceCase) -> None:
    result = ops.Reduce(case.reducer, case.reducer_keys)(iter(case.data))
    assert isinstance(result, tp.Iterator)
    for i, line in enumerate(result):
        for key, value in case.ground_truth[i].items():
            assert line[key] == value


@dataclasses.dataclass
class JoinCase:
    joiner: ops.Joiner
    join_keys: tp.Sequence[str]
    data_left: list[ops.TRow]
    data_right: list[ops.TRow]
    ground_truth: list[ops.TRow]


JOIN_CASES = [
    JoinCase(
        joiner=ops.OuterJoiner(),
        join_keys=("player",),
        data_left=[
            {"player": 1, "duplicate": "b"},
            {"player": 2, "duplicate": "c"}
        ],
        data_right=[
            {"player": 0, "duplicate": 1},
            {"player": 1, "duplicate": 2},
        ],
        ground_truth=[
            {"player": 0, "duplicate_2": 1},
            {"player": 1, "duplicate_1": "b", "duplicate_2": 2},
            {"player": 2, "duplicate_1": "c"}
        ],
    )]


@pytest.mark.parametrize("case", JOIN_CASES)
def test_tricky_joiner(case: JoinCase) -> None:
    result = ops.Join(case.joiner, case.join_keys)(iter(case.data_left), iter(case.data_right))
    assert isinstance(result, tp.Iterator)
    assert [*result] == case.ground_truth
