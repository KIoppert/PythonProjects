import json

from . import Graph, operations


def word_count_graph(input_stream_name: str, text_column: str = "text", count_column: str = "count",
                     from_file: bool = False) -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""
    if from_file:
        graph = Graph.graph_from_file(input_stream_name, json.loads)
    else:
        graph = Graph.graph_from_iter(input_stream_name)
    return graph \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def inverted_index_graph(input_stream_name: str, doc_column: str = "doc_id", text_column: str = "text",
                         result_column: str = "tf_idf", from_file: bool = False) -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""

    n_docs_col = "count_docs"
    n_docs_with_word = "count_docs_with_word"
    tf = "tf"
    idf = "idf"

    if from_file:
        graph = Graph.graph_from_file(input_stream_name, json.loads)
    else:
        graph = Graph.graph_from_iter(input_stream_name)

    split_words = Graph.graph_from_another_graph(graph) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column))
    count_docs = Graph.graph_from_another_graph(graph) \
        .reduce(operations.Count(n_docs_col), [doc_column]) \
        .reduce(operations.Sum(n_docs_col), [n_docs_col])
    count_idf = Graph.graph_from_another_graph(split_words) \
        .sort([doc_column, text_column]) \
        .reduce(operations.FirstReducer(), [doc_column, text_column]) \
        .sort([text_column]) \
        .reduce(operations.Count(n_docs_with_word), [text_column]) \
        .join(operations.InnerJoiner(), count_docs, []) \
        .map(operations.CalculateIdf())
    count_tf = Graph.graph_from_another_graph(split_words) \
        .sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column), [doc_column])
    count_tf_idf = count_tf \
        .sort([text_column]) \
        .join(operations.RightJoiner(), count_idf, [text_column]) \
        .sort([doc_column, text_column]) \
        .map(operations.Product([tf, idf], result_column)) \
        .map(operations.Project([doc_column, text_column, result_column])) \
        .sort([text_column]) \
        .reduce(operations.TopN(result_column, 3), [text_column])
    return count_tf_idf


def pmi_graph(input_stream_name: str, doc_column: str = "doc_id", text_column: str = "text",
              result_column: str = "pmi", from_file: bool = False) -> Graph:
    """Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information"""

    n_words_col = "count_words"

    if from_file:
        graph = Graph.graph_from_file(input_stream_name, json.loads)
    else:
        graph = Graph.graph_from_iter(input_stream_name)

    split_words = graph \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column))

    second = split_words \
        .sort([doc_column, text_column]) \
        .map(operations.Filter(lambda row: len(row[text_column]) > 4))

    count_words = Graph.graph_from_another_graph(second) \
        .reduce(operations.Count(n_words_col), [doc_column, text_column]) \
        .map(operations.Filter(lambda x: x[n_words_col] >= 2))

    total_counts_for_words = Graph.graph_from_another_graph(count_words) \
        .reduce(operations.Sum(n_words_col), [])

    count_word_in_all = Graph.graph_from_another_graph(count_words) \
        .sort([text_column]) \
        .reduce(operations.Sum(n_words_col), [text_column])

    pmi_counter = Graph.graph_from_another_graph(count_words) \
        .join(operations.InnerJoiner(), second, [doc_column, text_column]) \
        .map(operations.Project([doc_column, text_column])) \
        .join(operations.InnerJoiner(), total_counts_for_words, []) \
        .reduce(operations.TermFrequency(text_column), [doc_column, n_words_col]) \
        .sort([text_column]) \
        .join(operations.InnerJoiner("_in_all_docs", "_in_this_doc"), count_word_in_all, [text_column]) \
        .map(operations.CalculatePMI(pmi_column=result_column)) \
        .map(operations.Project([doc_column, text_column, result_column])) \
        .sort([doc_column]) \
        .reduce(operations.TopN(result_column, 10, ascending=True), [doc_column])

    return pmi_counter


def yandex_maps_graph(input_stream_name_time: str, input_stream_name_length: str,
                      enter_time_column: str = "enter_time", leave_time_column: str = "leave_time",
                      edge_id_column: str = "edge_id", start_coord_column: str = "start", end_coord_column: str = "end",
                      weekday_result_column: str = "weekday", hour_result_column: str = "hour",
                      speed_result_column: str = "speed", from_file: bool = False) -> Graph:
    """Constructs graph which measures average speed in km/h depending on the weekday and hour"""

    if from_file:
        time = Graph.graph_from_file(input_stream_name_time, json.loads)
        length = Graph.graph_from_file(input_stream_name_length, json.loads)
    else:
        time = Graph.graph_from_iter(input_stream_name_time)
        length = Graph.graph_from_iter(input_stream_name_length)

    time_length = length \
        .sort([edge_id_column]) \
        .join(operations.InnerJoiner(), time.sort([edge_id_column]), [edge_id_column]) \
        .map(operations.CalculateTimeAndDistance(enter_time_column=enter_time_column,
                                                 leave_time_column=leave_time_column,
                                                 start_coords_column=start_coord_column,
                                                 end_coords_column=end_coord_column, )) \
        .sort([weekday_result_column, hour_result_column]) \
        .reduce(operations.AverageSpeed(result_column=speed_result_column),
                [weekday_result_column, hour_result_column]) \
        .map(operations.Project([weekday_result_column, hour_result_column, speed_result_column]))

    return time_length
