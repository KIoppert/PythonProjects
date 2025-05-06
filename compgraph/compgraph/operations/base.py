# base.py
import typing as tp
from abc import abstractmethod, ABC

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class Mapper(ABC):
    """Base class for mappers"""

    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Reducer(ABC):
    """Base class for reducers"""

    @abstractmethod
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Joiner(ABC):
    """Base class for joiners"""

    def __init__(self, suffix_a: str = "_1", suffix_b: str = "_2") -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b
        self._keys_that_were_before: set[str] = set()

    @property
    def keys_that_were_before(self) -> set[str]:
        return self._keys_that_were_before

    @keys_that_were_before.setter
    def keys_that_were_before(self, keys: set[str]) -> None:
        if not self._keys_that_were_before:  # this field set only once
            self._keys_that_were_before = keys

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass

    def _merge_rows(self, keys: tp.Sequence[str], rows_a: list[TRow], rows_b: TRowsIterable) \
            -> tp.Generator[TRow, None, bool | None]:
        assert len(rows_a) > 0
        b_was_empty = True
        for row_b in rows_b:
            b_was_empty = False
            for row_a in rows_a:
                yield self._add_suffixes(row_a, row_b, keys)
        return b_was_empty

    def _add_suffixes(self, row_a: TRow, row_b: TRow, keys: tp.Sequence[str]) -> TRow:
        new_row_a = row_a.copy()
        new_row_b = row_b.copy()
        if not self._keys_that_were_before:  # If smb call some kind of joiner without calling Join
            self.keys_that_were_before = new_row_a.keys() & new_row_b.keys()
        for key in self.keys_that_were_before:
            if key not in keys:
                if new_row_a:
                    new_row_a[f"{key}{self._a_suffix}"] = new_row_a.pop(key)
                if new_row_b:
                    new_row_b[f"{key}{self._b_suffix}"] = new_row_b.pop(key)
        return {**new_row_a, **new_row_b}
