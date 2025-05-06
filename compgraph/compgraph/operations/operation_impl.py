# operation_impl.py
from __future__ import annotations

import itertools
import typing as tp

from .base import Operation, TRow, TRowsIterable, TRowsGenerator, Mapper, Reducer, Joiner
from ..exception import CompgraphException

T = tp.TypeVar("T")
V = tp.TypeVar("V", bound=tp.Any)


class Read(Operation):
    """
    Read file and parse it line by line
    """

    def __init__(self, filename: str, parser: tp.Callable[[str], TRow]) -> None:
        self.__filename = filename
        self.__parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        with open(self.__filename) as f:
            for line in f:
                yield self.__parser(line)


class ReadIterFactory(Operation):
    """
    Take rows from iter
    """

    def __init__(self, name: str) -> None:
        self.__name = name

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in kwargs[self.__name]():
            yield row


class Map(Operation):
    """
    Apply mapper to each row
    """

    def __init__(self, mapper: Mapper) -> None:
        self.__mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in rows:
            yield from self.__mapper(row)


class SafeGroupBy(tp.Iterator[tuple[V | None, TRowsIterable]]):

    def __init__(self, iterator: TRowsIterable, keys: tp.Callable[[TRow], V],
                 sentinel: tuple[None, TRowsIterable] = (None, {})) -> None:
        self.__iterator = itertools.groupby(iterator, key=keys)
        self.__last_item: V | None = None
        self.__sentinel: tuple[None, TRowsIterable] = sentinel

    def __iter__(self) -> SafeGroupBy[V]:
        return self

    def __next__(self) -> tuple[V | None, TRowsIterable]:
        key, group = next(self.__iterator, self.__sentinel)
        if key is not None and self.__last_item is not None and key < self.__last_item:
            raise CompgraphException("Input is not sorted")
        self.__last_item = key
        return key, group


class Reduce(Operation):
    """
    Apply reducer to each group of rows
    """

    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        self.__reducer = reducer
        self.__keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        data_group: SafeGroupBy[tuple[str, ...]] = SafeGroupBy(rows, lambda row: tuple(row[k] for k in self.__keys))
        for key, group in data_group:
            if key is None:
                break
            yield from self.__reducer(tuple(self.__keys), group)


class Join(Operation):
    """
    Join two datasets
    """

    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self.__keys = keys
        self.__joiner = joiner

    def __make_keys(self, row: TRow) -> tuple[tp.Any, ...]:
        return tuple(row[k] for k in self.__keys)

    def __find_common_keys(self, group_left: TRowsIterable, group_right: TRowsIterable) -> \
            tuple[TRowsIterable, TRowsIterable]:
        group_left, group_left_copy = itertools.tee(group_left)
        group_right, group_right_copy = itertools.tee(group_right)
        common_keys: set[str] = dict(next(group_left_copy)).keys() & dict(next(group_right_copy)).keys() - set(
            self.__keys)
        self.__joiner._keys_that_were_before = common_keys
        return group_left, group_right

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        if not args or not isinstance(args[0], tp.Iterable):
            raise CompgraphException("Second argument should be iterable and not empty")

        data_group_left: SafeGroupBy[tuple[str, ...]] = SafeGroupBy(rows, self.__make_keys)
        data_group_right: SafeGroupBy[tuple[str, ...]] = SafeGroupBy(args[0], self.__make_keys)
        key_left, group_left = next(data_group_left)
        key_right, group_right = next(data_group_right)
        group_left, group_right = self.__find_common_keys(group_left, group_right)
        while key_left is not None and key_right is not None:
            if key_left == key_right:
                yield from self.__joiner(self.__keys, group_left, group_right)
                key_left, group_left = next(data_group_left)
                key_right, group_right = next(data_group_right)
            elif key_left < key_right:
                yield from self.__joiner(self.__keys, group_left, [])
                key_left, group_left = next(data_group_left)
            elif key_left > key_right:
                yield from self.__joiner(self.__keys, [], group_right)
                key_right, group_right = next(data_group_right)
        while key_left is not None:
            yield from self.__joiner(self.__keys, group_left, [])
            key_left, group_left = next(data_group_left)
        while key_right is not None:
            yield from self.__joiner(self.__keys, [], group_right)
            key_right, group_right = next(data_group_right)
