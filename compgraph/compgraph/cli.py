import json

import click
import pandas as pd
import plotly.express as px

from .algorithms import word_count_graph, inverted_index_graph, pmi_graph, yandex_maps_graph


@click.group()
def cli() -> None:
    pass


@click.command(help="Count words in {input_filename} and save to {output_filename}")
@click.argument("input_filename", type=click.Path(exists=True))
@click.argument("output_filename", type=click.Path())
def run_word_count(input_filename: str, output_filename: str) -> None:
    click.echo(f"Counting words in {input_filename} and saving to {output_filename}")
    graph = word_count_graph(input_stream_name=input_filename, text_column="text", count_column="count", from_file=True)

    result = graph.run()
    with open(output_filename, "w") as out:
        for row in result:
            out.write(json.dumps(row) + "\n")


@click.command(help="Count top-3 TF-IDF docs for each word in {input_filename} and save to {output_filename}")
@click.argument("input_filename", type=click.Path(exists=True))
@click.argument("output_filename", type=click.Path())
def run_inverted_index(input_filename: str, output_filename: str) -> None:
    graph = inverted_index_graph(input_stream_name=input_filename, doc_column="doc_id", text_column="text",
                                 result_column="tf_idf", from_file=True)

    result = graph.run()
    with open(output_filename, "w") as out:
        for row in result:
            out.write(json.dumps(row) + "\n")


@click.command(help="Count top-10 PMI words for each document in {input_filename} and save to {output_filename}")
@click.argument("input_filename", type=click.Path(exists=True))
@click.argument("output_filename", type=click.Path())
def run_pmi(input_filename: str, output_filename: str) -> None:
    graph = pmi_graph(input_stream_name=input_filename, doc_column="doc_id", text_column="text", result_column="pmi",
                      from_file=True)

    result = graph.run()
    with open(output_filename, "w") as out:
        for row in result:
            out.write(json.dumps(row) + "\n")


@click.command(help="Calculate average speed in km/h depending on the weekday and hour")
@click.option("-v", "--visualization", type=str,
              help="Visualize the graph. Pic will be saved on the specified path")
@click.argument("input_time_filename", type=click.Path(exists=True))
@click.argument("input_length_filename", type=click.Path(exists=True))
@click.argument("output_filename", type=click.Path())
def run_yandex_maps(visualization: str, input_time_filename: str, input_length_filename: str,
                    output_filename: str) -> None:
    graph = yandex_maps_graph(input_stream_name_time=input_time_filename,
                              input_stream_name_length=input_length_filename,
                              enter_time_column="enter_time", leave_time_column="leave_time",
                              edge_id_column="edge_id", start_coord_column="start", end_coord_column="end",
                              weekday_result_column="weekday", hour_result_column="hour",
                              speed_result_column="speed", from_file=True)

    result = graph.run()

    with open(output_filename, "w") as out:
        for row in result:
            out.write(json.dumps(row) + "\n")

    if visualization:
        data = []
        with open(output_filename, "r") as in_:
            for line in in_:
                data.append(json.loads(line))

        df = pd.DataFrame(data)
        df["weekday_hour"] = df["weekday"] + " " + df["hour"].astype(str)

        fig = px.line(df, x="weekday_hour", y="speed", title="Average Speed by Weekday and Hour", width=1600,
                      height=800)
        fig.update_layout(xaxis_title="Weekday and Hour", yaxis_title="Speed (km/h)")
        fig.write_image(visualization, engine="kaleido", format="png")


cli.add_command(run_word_count)
cli.add_command(run_inverted_index)
cli.add_command(run_pmi)
cli.add_command(run_yandex_maps)

if __name__ == "__main__":
    cli()
