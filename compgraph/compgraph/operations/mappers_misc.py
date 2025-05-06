# mapper_misc.py
import math
from datetime import datetime

from .base import Mapper, TRow, TRowsGenerator


class CalculateIdf(Mapper):
    """Calculate idf for words"""

    def __init__(self, total_docs_column: str = "count_docs", docs_with_word_column: str = "count_docs_with_word",
                 idf_column: str = "idf") -> None:
        """
        :param idf_column: name for idf column
        :param total_docs_column: name for total docs column
        :param docs_with_word_column: name for docs with word column
        """
        self.__idf_column = idf_column
        self.__total_docs_column = total_docs_column
        self.__docs_with_word_column = docs_with_word_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        idf = math.log(row[self.__total_docs_column] / row[self.__docs_with_word_column])
        yield {**row, self.__idf_column: idf}


class CalculatePMI(Mapper):
    """Calculate pointwise mutual information"""

    def __init__(self, total_docs_column: str = "count_words_in_all_docs",
                 total_words_in_docs: str = "count_words_in_this_doc",
                 frequency_column: str = "tf", pmi_column: str = "pmi") -> None:
        """
        :param total_docs_column: name for total docs column
        :param total_words_in_docs: name for docs with word column
        :param frequency_column: name for docs with both words column
        :param pmi_column: name for pmi column
        """
        self.__total_docs_column = total_docs_column
        self.__docs_with_word_column = total_words_in_docs
        self.__frequency_column = frequency_column
        self.__pmi_column = pmi_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        pmi = math.log(
            row[self.__frequency_column] / (row[self.__docs_with_word_column] / row[self.__total_docs_column]))
        yield {**row, self.__pmi_column: pmi}


class CalculateTimeAndDistance(Mapper):
    """Calculate average speed for each group"""
    EARTH_RADIUS_IN_KM = 6373
    SECONDS_IN_HOUR = 3600

    def __init__(self, enter_time_column: str = "enter_time", leave_time_column: str = "leave_time",
                 start_coords_column: str = "start",
                 end_coords_column: str = "end", result_distance_column: str = "distance",
                 result_time_column: str = 'time') -> None:
        """
        :param enter_time_column: column name with enter time
        :param leave_time_column: column name with leave time
        :param start_coords_column: column name with start coordinates
        :param end_coords_column: column name with end coordinates
        :param result_column_column: column name to save speed in
        """
        self.__enter_time_column = enter_time_column
        self.__leave_time_column = leave_time_column
        self.__start_coords_column = start_coords_column
        self.__end_coords_column = end_coords_column
        self.__result_length_column = result_distance_column
        self.__result_time_column = result_time_column

    @staticmethod
    def __haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate the great circle distance between two points in km

        :param lat1: latitude of the first point
        :param lon1: longitude of the first point
        :param lat2: latitude of the second point
        :param lon2: longitude of the second point
        """

        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        distance = CalculateTimeAndDistance.EARTH_RADIUS_IN_KM * c
        return distance

    def __call__(self, row: TRow) -> TRowsGenerator:
        start_time = datetime.fromisoformat(row[self.__enter_time_column])
        end_time = datetime.fromisoformat(row[self.__leave_time_column])

        if end_time < start_time:
            return

        weekday_ = start_time.strftime("%A")[:3:]
        hour = start_time.hour
        start_lon, start_lat = row[self.__start_coords_column]
        end_lon, end_lat = row[self.__end_coords_column]
        distance = self.__haversine(start_lat, start_lon, end_lat, end_lon)
        time = (end_time - start_time).total_seconds() / self.SECONDS_IN_HOUR
        yield {**row,
               self.__result_length_column: distance,
               self.__result_time_column: time,
               "weekday": weekday_,
               "hour": hour, }
