# Benchmark синхронизации

`sync_tick_hub_benchmark` измеряет производительность основных операций
синхронизации в сценарии, где центральный узел хранит порции тиковых данных от
множества origin-узлов, а реплика забирает эти изменения. Программа работает в
одном процессе через `DirectSyncPeer`, поэтому результаты отражают локальное
чтение changelog, постраничную выдачу, декодирование и применение через
`handle_push()`. Задержки сети и сериализация реального транспорта здесь не
измеряются.

## Сборка

```bash
cmake -S . -B tmp/build-bench \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=OFF \
    -DMDBXC_BUILD_EXAMPLES=OFF \
    -DMDBXC_BUILD_BENCHMARKS=ON \
    -DCMAKE_CXX_STANDARD=17

cmake --build tmp/build-bench --target sync_tick_hub_benchmark
```

Команды ниже используют пути в стиле Linux/MSYS2. На Windows имя исполняемого
файла заканчивается на `.exe`, например:

```powershell
.\tmp\build-bench\bin\benchmarks\sync_tick_hub_benchmark.exe --preset quick
```

## Готовые сценарии

Без аргументов запускается сценарий `quick`.

```bash
tmp/build-bench/bin/benchmarks/sync_tick_hub_benchmark --preset quick
tmp/build-bench/bin/benchmarks/sync_tick_hub_benchmark --preset realistic
```

| Сценарий | Назначение |
| --- | --- |
| `quick` | Короткий набор для проверки сборки и грубого сравнения изменений. |
| `realistic` | Более крупный ручной набор с большим числом origin-узлов и историей. Название означает нагрузку, более близкую к hub-сценарию, а не универсальную модель рабочей нагрузки. |

Вывести список доступных сценариев:

```bash
tmp/build-bench/bin/benchmarks/sync_tick_hub_benchmark --list-presets
```

## Пользовательский сценарий

```bash
tmp/build-bench/bin/benchmarks/sync_tick_hub_benchmark \
    origins historical_chunks_per_origin new_chunks_per_origin \
    ticks_per_chunk max_batches max_bytes
```

Позиционные аргументы задаются слева направо. Если пропустить параметры в
конце, benchmark использует значения по умолчанию из таблицы.

| Аргумент | По умолчанию | Значение |
| --- | ---: | --- |
| `origins` | 16 | Количество независимых origin-узлов, чьи batches находятся в changelog. |
| `historical_chunks_per_origin` | 512 | Число уже существующих batches каждого origin перед первым pull. |
| `new_chunks_per_origin` | 8 | Число новых batches каждого origin в каждой инкрементальной фазе. Значение используется дважды: в `incremental_hot` и после перезапуска. |
| `ticks_per_chunk` | 128 | Число тиковых записей внутри одного batch. |
| `max_batches` | 64 | Максимальное число batches в одной странице `PullResponse`. |
| `max_bytes` | 4194304 | Приблизительный предел размера одной страницы в байтах. |

## CSV

Каждый сценарий печатает три фазы:

- `full_cold_replica` - первичная синхронизация пустой реплики со всей
  накопленной историей.
- `incremental_hot` - добавление новых batches и повторная синхронизация без
  закрытия соединений и пересоздания `SyncEngine`.
- `incremental_after_restart` - закрытие и повторное открытие обеих БД,
  пересоздание `SyncEngine`, добавление ещё одного диапазона batches и
  инкрементальная синхронизация от сохранённого курсора получателя.

| Колонка | Значение |
| --- | --- |
| `scenario` | Имя сценария. |
| `phase` | Одна из трёх фаз, перечисленных выше. |
| `origins` | Количество origin-узлов в сценарии. |
| `historical_chunks_per_origin` | Исторические batches каждого origin перед первичной синхронизацией. |
| `new_chunks_per_origin` | Новые batches каждого origin для каждой инкрементальной фазы. |
| `chunks_per_origin` | Batches каждого origin, существующие в измеряемой фазе. |
| `ticks_per_chunk` | Тиковые записи внутри одного batch. |
| `max_batches` | Ограничение страницы по числу batches. |
| `max_bytes` | Приблизительное ограничение страницы по числу байтов. |
| `seeded_batches` | Batches, записанные локально перед измеряемым pull. |
| `pulled_batches` | Batches, полученные через страницы `PullResponse`. |
| `applied_batches` | Batches, применённые через `SyncEngine::handle_push()`. |
| `pull_pages` | Число pull-запросов, нужных для завершения фазы. |
| `origin_index_entries` | Число origin-узлов, зарегистрированных в `_mdbxc_origins` на primary. |
| `seed_ms` | Время локальной записи данных для фазы. |
| `restart_ms` | Время закрытия, повторного открытия и пересоздания объектов в фазе перезапуска. В остальных фазах равно нулю. |
| `pull_ms` | Время, затраченное на вызовы `DirectSyncPeer::pull()`. |
| `apply_ms` | Время, затраченное на вызовы `SyncEngine::handle_push()`. |
| `total_ms` | `seed_ms + restart_ms + pull_ms + apply_ms`. |
| `sync_ms` | `pull_ms + apply_ms`. Время подготовки данных и перезапуска не учитывается. |
| `pull_pct` | Доля `sync_ms`, затраченная на pull-вызовы. |
| `apply_pct` | Доля `sync_ms`, затраченная на локальное применение страниц. |
| `batches_per_page` | Среднее значение `pulled_batches / pull_pages`. |
| `batches_per_sec` | `applied_batches / (pull_ms + apply_ms)`. Время подготовки данных и перезапуска не учитывается. |
| `primary_bytes` | Размер занятых страниц MDBX у primary после фазы. |
| `replica_bytes` | Размер занятых страниц MDBX у replica после фазы. |

`origin_index_entries` после заполнения исторических данных обычно совпадает с
`origins`. Этот индекс позволяет `SyncEngine::handle_pull()` не открывать и не
позиционировать changelog cursor для origin-узлов, по которым получатель уже
дошёл до последнего известного `seq`. Для отстающих origin-узлов
движок выполняет точный поиск начиная с `have_seq + 1`.

Перед выбором направления оптимизации смотрите на `pull_pct` и `apply_pct`.
Если `pull_pct` низкий, текущий запуск в основном тратит время на локальное
применение страниц; изменение алгоритма pull слабо повлияет на общую
пропускную способность такой нагрузки.

## Как сравнивать результаты

1. Соберите обе версии в одинаковом режиме, желательно `Release`.
2. Запустите один и тот же готовый или пользовательский сценарий не меньше пяти
   раз.
3. Первый запуск можно считать прогревом файлового кэша, если результаты
   заметно плавают.
4. Сравнивайте медиану `pull_ms`, `apply_ms`, `pull_pct`, `apply_pct`,
   `pull_pages` и `batches_per_sec`.
5. Не сравнивайте запуски с разными компиляторами, настройками MDBX,
   сценариями или аргументами.
6. Используйте этот benchmark только для оценки изменений ядра синхронизации.
   Он не показывает стоимость HTTP, WebSocket, IPC или шифрования.

## Базовый benchmark destructive state

`ordered_destructive_state_benchmark` измеряет два намеренно линейных пути
проверки destructive schema version 2:

- `live_state_ids_for_key()` выполняет обратное сканирование всего state DBI,
  чтобы найти все живые element id одного canonical key и обнаружить записи,
  отсутствующие в DUPSORT index;
- `verify_introduced_high_water()` сканирует prefix одного origin перед выдачей
  нового id.

Это ручной baseline для границ нагрузки, а не заявление о пропускной
способности и не CTest target. Для сравнения ревизий используйте одинаковые
аргументы и настройки сборки:

```bash
cmake --build tmp/build-bench --target ordered_destructive_state_benchmark
tmp/build-bench/bin/benchmarks/ordered_destructive_state_benchmark
tmp/build-bench/bin/benchmarks/ordered_destructive_state_benchmark \
    16 512 32 100
```

Необязательные позиционные аргументы: `origins`, `elements_per_origin`,
`key_count` и `iterations`. CSV отдельно содержит `element_records` и
`state_records`; второе значение включает allocation counter и introduced
high-water record каждого origin. `elapsed_ms` включает открытие read
transaction, измеряемое сканирование и rollback. Результаты representative
workloads нужны до добавления index, cache или фиксированного лимита к любому
из путей корректности.
