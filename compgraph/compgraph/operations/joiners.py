# joiners.py
import typing as tp

from .base import Joiner, TRowsIterable, TRowsGenerator


class InnerJoiner(Joiner):
    """Join with inner strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_a = list(rows_a)
        if rows_a:
            yield from self._merge_rows(keys, rows_a, rows_b)


class OuterJoiner(Joiner):
    """Join with outer strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_a = list(rows_a)
        if not rows_a:
            for row in rows_b:
                yield self._add_suffixes({}, row, keys)
            return
        is_rows_b_empty = yield from self._merge_rows(keys, rows_a, rows_b)
        if is_rows_b_empty:
            for row in rows_a:
                yield self._add_suffixes(row, {}, keys)


class LeftJoiner(Joiner):
    """Join with left strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_a = list(rows_a)
        if not rows_a:
            return
        is_rows_b_empty = yield from self._merge_rows(keys, rows_a, rows_b)
        if is_rows_b_empty:
            for row in rows_a:
                yield self._add_suffixes(row, {}, keys)


class RightJoiner(Joiner):
    """Join with right strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_b = list(rows_b)
        if not rows_b:
            return
        is_rows_a_empty = yield from self._merge_rows(keys, rows_b, rows_a)
        if is_rows_a_empty:
            for row in rows_b:
                yield self._add_suffixes({}, row, keys)
