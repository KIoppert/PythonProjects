# reducers.py
import collections
import heapq

from .base import Reducer, TRow, TRowsIterable, TRowsGenerator


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in rows:
            yield row
            break


class TopN(Reducer):
    """Calculate top N by value"""

    def __init__(self, column: str, n: int, ascending: bool = False) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.__column_max = column
        self.__n = n
        self.__ascending = ascending

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        __heap: list[tuple[int, int, TRow]] = []
        counter = 0
        for row in rows:
            value_for_cmp = row[self.__column_max]
            if len(__heap) != self.__n:
                heapq.heappush(__heap, (value_for_cmp, counter, row))
                counter += 1
            else:
                if value_for_cmp > __heap[0][0]:  # heap always contains smallest elm on zero pos
                    heapq.heappushpop(__heap, (value_for_cmp, counter, row))
                    counter += 1

        if self.__ascending:
            for _, _, row in heapq.nlargest(self.__n, __heap):
                yield row
        else:
            for _, _, row in heapq.nsmallest(self.__n, __heap):
                yield row


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""

    def __init__(self, words_column: str, result_column: str = "tf") -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.__words_column = words_column
        self.__result_column = result_column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        counter: dict[str, int] = collections.defaultdict(int)
        total = 0
        row = None  # want to save last row for "group_key" data
        for row in rows:
            counter[row[self.__words_column]] += 1
            total += 1

        important_columns = {key: row[key] for key in group_key}
        for word, count in counter.items():
            result = count / total
            yield {**important_columns, self.__words_column: word,
                   self.__result_column: result}


class Count(Reducer):
    """
    Count records by key
    Example for group_key=("a",) and column="d"
        {"a": 1, "b": 5, "c": 2}
        {"a": 1, "b": 6, "c": 1}
        =>
        {"a": 1, "d": 2}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for result column
        """
        self.__column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        counter: dict[str, int] = collections.defaultdict(int)
        row = None
        for row in rows:
            counter[row[group_key[0]]] += 1
        group_keys = {key: row[key] for key in group_key[1::]}
        for word, count in counter.items():
            yield {self.__column: count, group_key[0]: word, **group_keys}


class Sum(Reducer):
    """
    Sum values aggregated by key
    Example for key=("a",) and column="b"
        {"a": 1, "b": 2, "c": 4}
        {"a": 1, "b": 3, "c": 5}
        =>
        {"a": 1, "b": 5}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for sum column
        """
        self.__column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        result = 0
        row = None  # want to save last row for "group_key" data
        for row in rows:
            result += row[self.__column]
        group_keys = {key: row[key] for key in group_key}
        if group_keys:
            yield {**{group_key: row[group_key] for group_key in group_keys}, self.__column: result}
        else:
            yield {self.__column: result}
