# mappers.py
import functools
import re
import string
import typing as tp

from .base import Mapper, TRow, TRowsGenerator


class DummyMapper(Mapper):
    """Yield exactly the row passed"""

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.__column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {**row,
               self.__column: re.sub(fr"[\\{string.punctuation}]", "", row[self.__column])}


class LowerCase(Mapper):
    """Replace column value with value in lower case"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.__column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {**row, self.__column: (row[self.__column]).lower()}


class Split(Mapper):
    """Split row on multiple rows by separator"""

    def __init__(self, column: str, separator: str | None = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.__column = column
        self.__separator = r"\s+"
        if separator is not None:
            self.__separator = separator

    def __call__(self, row: TRow) -> TRowsGenerator:
        match = None
        original_value = row[self.__column]
        matches = re.finditer(rf"{self.__separator}", original_value)
        current = 0
        for match in matches:
            yield {**row, self.__column: original_value[current:match.start()]}
            current = match.end()

        if current == 0:
            yield row
        elif match.end() != len(original_value):
            yield {**row, self.__column: original_value[match.end():]}


class Product(Mapper):
    """Calculates product of multiple columns"""

    def __init__(self, columns: tp.Sequence[str], result_column: str = "product") -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.__columns = columns
        self.__result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        result = functools.reduce(lambda x, y: x * y, (row[value] for value in self.__columns), 1)
        yield {**row, self.__result_column: result}


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""

    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self.__condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.__condition(row):
            yield row


class Project(Mapper):
    """Leave only mentioned columns"""

    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.__columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {key: row[key] for key in self.__columns}
