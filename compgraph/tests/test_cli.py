import json
import tempfile
import typing as tp

import pytest
from click.testing import CliRunner

from compgraph.cli import cli

text_raw = [
    {"doc_id": 1, "text": "hello, little world"},
    {"doc_id": 2, "text": "little"},
    {"doc_id": 3, "text": "little little little"},
    {"doc_id": 4, "text": "little? hello little world"},
    {"doc_id": 5, "text": "HELLO HELLO! WORLD..."},
    {"doc_id": 6, "text": "world? world... world!!! WORLD!!! HELLO!!! HELLO!!!!!!!"},
]

times_raw = [
    {"leave_time": "20171020T112238.723000", "enter_time": "20171020T112237.427000", "edge_id": 8414926848168493057},
    {"leave_time": "20171011T145553.040000", "enter_time": "20171011T145551.957000", "edge_id": 8414926848168493057},
    {"leave_time": "20171020T090548.939000", "enter_time": "20171020T090547.463000", "edge_id": 8414926848168493057},
    {"leave_time": "20171024T144101.879000", "enter_time": "20171024T144059.102000", "edge_id": 8414926848168493057},
    {"leave_time": "20171022T131828.330000", "enter_time": "20171022T131820.842000", "edge_id": 5342768494149337085},
    {"leave_time": "20171014T134826.836000", "enter_time": "20171014T134825.215000", "edge_id": 5342768494149337085},
    {"leave_time": "20171010T060609.897000", "enter_time": "20171010T060608.344000", "edge_id": 5342768494149337085},
    {"leave_time": "20171027T082600.201000", "enter_time": "20171027T082557.571000", "edge_id": 5342768494149337085},
]
lengths_raw = [
    {
        "start": [37.84870228730142, 55.73853974696249],
        "end": [37.8490418381989, 55.73832445777953],
        "edge_id": 8414926848168493057,
    },
    {
        "start": [37.524768467992544, 55.88785375468433],
        "end": [37.52415172755718, 55.88807155843824],
        "edge_id": 5342768494149337085,
    },
    {
        "start": [37.56963176652789, 55.846845586784184],
        "end": [37.57018438540399, 55.8469259692356],
        "edge_id": 5123042926973124604,
    },
    {
        "start": [37.41463478654623, 55.654487907886505],
        "end": [37.41442892700434, 55.654839486815035],
        "edge_id": 5726148664276615162,
    },
    {
        "start": [37.584684155881405, 55.78285809606314],
        "end": [37.58415022864938, 55.78177368734032],
        "edge_id": 451916977441439743,
    },
    {
        "start": [37.736429711803794, 55.62696328852326],
        "end": [37.736344216391444, 55.626937723718584],
        "edge_id": 7639557040160407543,
    },
    {
        "start": [37.83196756616235, 55.76662947423756],
        "end": [37.83191015012562, 55.766647034324706],
        "edge_id": 1293255682152955894,
    },
]


@pytest.mark.parametrize("command_name", ["", "run-inverted-index", "run-pmi", "run-word-count", "run-yandex-maps"])
def test_cli_help(command_name: str) -> None:
    runner = CliRunner()
    if command_name == "":
        result = runner.invoke(cli, ["--help"])
    else:
        result = runner.invoke(cli, [command_name, "--help"])
    assert result.exit_code == 0, result.output
    assert len(result.output) > 0


answer_word_count = [
    {"count": 6, "text": "hello"},
    {"count": 7, "text": "little"},
    {"count": 7, "text": "world"},
]

answer_tf_idf = [
    {"doc_id": 1, "text": "hello", "tf_idf": 0.13515503603605478},
    {"doc_id": 6, "text": "hello", "tf_idf": 0.13515503603605478},
    {"doc_id": 5, "text": "hello", "tf_idf": 0.27031007207210955},
    {"doc_id": 4, "text": "little", "tf_idf": 0.2027325540540822},
    {"doc_id": 2, "text": "little", "tf_idf": 0.4054651081081644},
    {"doc_id": 3, "text": "little", "tf_idf": 0.4054651081081644},
    {"doc_id": 1, "text": "world", "tf_idf": 0.13515503603605478},
    {"doc_id": 5, "text": "world", "tf_idf": 0.13515503603605478},
    {"doc_id": 6, "text": "world", "tf_idf": 0.27031007207210955},
]
answer_pmi = [
    {"doc_id": 3, "text": "little", "pmi": pytest.approx(0.9555, 0.001)},
    {"doc_id": 4, "text": "little", "pmi": pytest.approx(0.9555, 0.001)},
    {"doc_id": 5, "text": "hello", "pmi": pytest.approx(1.1786, 0.001)},
    {"doc_id": 6, "text": "world", "pmi": pytest.approx(0.7731, 0.001)},
    {"doc_id": 6, "text": "hello", "pmi": pytest.approx(0.0800, 0.001)},
]


@pytest.mark.parametrize("command_name, answer", [("run-word-count", answer_word_count),
                                                  ("run-inverted-index", answer_tf_idf), ("run-pmi", answer_pmi)],
                         ids=["run-word-count", "run-inverted-index", "run-pmi"])
def test_cli(command_name: str, answer: tp.Any) -> None:
    runner = CliRunner()
    tmp_in_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
    tmp_out_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
    with open(tmp_in_file.name, "w") as file:
        for line in text_raw:
            file.write(json.dumps(line) + "\n")
    result = runner.invoke(cli, [command_name, tmp_in_file.name, tmp_out_file.name])
    assert result.exit_code == 0, result.output
    with open(tmp_out_file.name, "r") as file:
        for i, line in enumerate(answer):
            assert json.loads(file.readline()) == line
    tmp_in_file.close()
    tmp_out_file.close()


def test_cli_run_yandex_maps_example() -> None:
    answer = [
        {"weekday": "Fri", "hour": 8, "speed": pytest.approx(62.2322, 0.001)},
        {"weekday": "Fri", "hour": 9, "speed": pytest.approx(78.1070, 0.001)},
        {"weekday": "Fri", "hour": 11, "speed": pytest.approx(88.9552, 0.001)},
        {"weekday": "Sat", "hour": 13, "speed": pytest.approx(100.9690, 0.001)},
        {"weekday": "Sun", "hour": 13, "speed": pytest.approx(21.8577, 0.001)},
        {"weekday": "Tue", "hour": 6, "speed": pytest.approx(105.3901, 0.001)},
        {"weekday": "Tue", "hour": 14, "speed": pytest.approx(41.5145, 0.001)},
        {"weekday": "Wed", "hour": 14, "speed": pytest.approx(106.4505, 0.001)},
    ]
    runner = CliRunner()
    tmp_length_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
    tmp_time_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
    tmp_out_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
    with open(tmp_length_file.name, "w") as file:
        for line in lengths_raw:
            file.write(json.dumps(line) + "\n")
    with open(tmp_time_file.name, "w") as file:
        for line in times_raw:
            file.write(json.dumps(line) + "\n")
    result = runner.invoke(cli, ["run-yandex-maps", tmp_length_file.name, tmp_time_file.name, tmp_out_file.name])
    assert result.exit_code == 0, result.output
    with open(tmp_out_file.name, "r") as file:
        for i, line in enumerate(answer):
            assert line == json.loads(file.readline())
    tmp_length_file.close()
    tmp_time_file.close()
    tmp_out_file.close()
