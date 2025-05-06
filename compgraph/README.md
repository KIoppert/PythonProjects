# Compgraph

Вычислительный граф представленный в этом пакете, может решать следующие задачи:

- Подсчет количества слов в файле.
- Поиск топ-3 документов по метрике по метрике [tf-idf](https://ru.wikipedia.org/wiki/TF-IDF) для каждого слова.
- Поиск топ-10 слов по
  метрике [Pointwise mutual information](https://en.wikipedia.org/wiki/Pointwise_mutual_information) для каждого
  документа.
- Вычисление средней скорости движения по городу в зависимости от часа и дня недели.

### Установка

Установка производится с помощью __python wheels__. Для этого необходимо выполнить следующие команды из корня пакета:

```bash 
# Создание wheel
pip wheel --wheel-dir dist . 
```

```bash
# Установка wheel
pip install --prefer-binary --force-reinstall --find-links dist . 
```

## Запуск тестов

Для запуска тестов необходимо выполнить следующую команду из корня пакета:

```bash
# Запуск тестов
pytest . -vvv
```

## Запуск примеров

В папке `examples` находятся примеры использования вычислительного графа на 4-х задачах. Для запуска примеров необходимо
выполнить следующую команду из корня пакета:
Эти скрипты созданы исключительно для запуска примеров. Для запуска на своих файлах используйте `cli`. Подробнее ниже.

```bash
# Запуск word_count
python examples/run_word_count.py acrhive/extract_me.tgz text_corpus.txt acrhive/out.txt

# Запуск top3_tfidf
python examples/run_inverted_index.py acrhive/extract_me.tgz text_corpus.txt acrhive/out.txt

# Запуск top10_pmi
python examples/run_pmi.py acrhive/extract_me.tgz text_corpus.txt acrhive/out.txt

# Запуск average_speed
python examples/run_yandex_maps.py acrhive/extract_me.tgz travel_times.txt road_graph_data.txt acrhive/out.txt

# Запуск average_speed с визуализацией
python examples/run_yandex_maps.py -v acrhive/pic.png acrhive/extract_me.tgz travel_times.txt road_graph_data.txt acrhive/out.txt
```

## Использование

Для использования пакета с помощью `cli` необходимо выполнить следующие команды:

- `compgraph run-word-count <input-file> <output-file>` - подсчет количества слов в файле.
- `compgraph run_inverted_index <input-file> <output-file>` - поиск топ-3 документов по
  метрике [tf-idf](https://ru.wikipedia.org/wiki/TF-IDF) для каждого слова.
- `compgraph run_pmi <input-file> <output-file>` - поиск топ-10 слов по
  метрике [Pointwise mutual information](https://en.wikipedia.org/wiki/Pointwise_mutual_information) для каждого
  документа.
- `compgraph run_yandex_maps <input-file> <output-file>` - вычисление средней скорости движения по городу в зависимости
  от часа и дня недели.
- `compgraph run_yandex_maps -v <picture-path> <input-file> <output-file>` - визуализация предыдущей задачи, сама
  картинка будет лежат по пути `picture-path`.

## Change-list

В версии 1.1 была добавлена визуализация для задачи 4. Подробнее про
запуск в `Использование`.

## Автор

- **Иван Горобец** - *Разработчик пакета `compgraph`* 
