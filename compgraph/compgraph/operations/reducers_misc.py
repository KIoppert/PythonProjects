from .base import TRowsGenerator, TRowsIterable, Reducer


class AverageSpeed(Reducer):
    """Calculate average speed for 4th task"""

    def __init__(self, time_column: str = 'time', distance_column: str = 'distance',
                 result_column: str = "speed") -> None:
        """
        :param time_column: column name with time
        :param distance_column: column name with distance
        :param result_column: column name to save result in
        """
        self.__time_column = time_column
        self.__distance_column = distance_column
        self.__result_column = result_column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        distance = 0
        time = 0
        row = None
        for row in rows:
            distance += row[self.__distance_column]
            time += row[self.__time_column]
        row.pop(self.__time_column)
        row.pop(self.__distance_column)
        yield {**row, self.__result_column: distance / time}
