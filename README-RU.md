# MDBX-Containers

[![MIT License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE) ![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue) ![C++ Standard](https://img.shields.io/badge/C++-11--17-orange) [![CI Windows](https://img.shields.io/github/actions/workflow/status/NewYaroslav/mdbx-containers/ci.yml?branch=main&label=Windows&logo=windows)](https://github.com/NewYaroslav/mdbx-containers/actions/workflows/ci.yml) [![CI Linux](https://img.shields.io/github/actions/workflow/status/NewYaroslav/mdbx-containers/ci.yml?branch=main&label=Linux&logo=linux)](https://github.com/NewYaroslav/mdbx-containers/actions/workflows/ci.yml) [![CI macOS](https://img.shields.io/github/actions/workflow/status/NewYaroslav/mdbx-containers/ci.yml?branch=main&label=macOS&logo=apple)](https://github.com/NewYaroslav/mdbx-containers/actions/workflows/ci.yml)

[English version](README.md)

**mdbx-containers** — лёгкая заголовочная библиотека C++11/17, которая соединяет
[libmdbx](https://github.com/erthink/libmdbx) с привычными STL-подобными API.
Она сохраняет key-value данные в MDBX и предоставляет высокую производительность
и удобные помощники для транзакций.

> Примечание  
> В этом проекте техническое качество важнее личных взглядов авторов.
> Подробнее см. [PHILOSOPHY.md](PHILOSOPHY.md).

## ⚙️ Особенности

### 🧱 API таблиц
- `KeyValueTable<K, V>` — основная таблица: одно значение на ключ, методы
  `insert`, `insert_or_assign`, `find`, `range`, `range_values`, `for_each_range`, `filter_range`,
  `lower_bound`, `upper_bound`, `range_reverse`, `erase_range`, `update`, `find_many`,
  `operator[]` и связанные помощники.
- `HashedKeyValueStore<K, V, H, Layout>` хранит одно значение на строковый или
  byte-vector ключ через hash-index и проверяет исходные байты ключа, чтобы
  корректно обрабатывать коллизии.
- `ValueTable<V>` хранит одно строго типизированное singleton-значение в
  именованной таблице: метаданные, состояние модуля, snapshots и конфигурацию.
- `AnyValueTable<K>` хранит значения разных типов по выбранному вызывающим кодом
  типу и поддерживает типизированные `set`, `insert`, `get`, `find`, `get_or`,
  `update`, `contains`, `erase` и `keys`.
- `KeyTable<K>` хранит уникальные ключи со `std::set`-подобным API: `insert`,
  `contains`, `range`, `for_each_range`, `filter_range`, `lower_bound`, `upper_bound`,
  `range_reverse`, `erase_range`, `clear`, `load`, `reconcile` и связанные помощники.
- `KeyMultiValueTable<K, V>` хранит несколько значений на один ключ со
  `std::multimap`-подобным API, потоковыми и материализованными range-scan методами,
  обратным сканированием, удалением диапазонов и сохранением повторяющихся одинаковых пар `(key, value)`.
- `KeyOrderedMultiValueTable<K, V>` хранит несколько значений на один ключ,
  когда текущий порядок append является частью API; повторяющиеся одинаковые значения
  остаются видимыми, а `find(key)` возвращает значения в порядке append.
- `SequenceTable<ValueT>` хранит значения по стабильному uint64_t id с
  append-only семантикой и разреженными индексами. Append возвращает
  стабильный id; удаление не переиндексирует следующие записи.
- Проверка type-tag prefix в `AnyValueTable` включается явно через
  `set_type_tag_check(true)` и по умолчанию выключена для совместимости с уже
  существующими raw-записями.
- `VectorStore` — MVP embedded vector store для локального RAG: persistent
  MDBX-хранилище с точным in-memory `FlatVectorIndex`.

### 🔁 Сериализация
- Автоматическая сериализация trivially copyable типов.
- Пользовательские типы через `to_bytes()` / `from_bytes()`.
- Поддержка вложенных STL-контейнеров, например `std::vector` и `std::list`.

### 🔒 Транзакции и потоки
- RAII-транзакции (`Transaction`).
- Повторное использование автоматических и ручных транзакций, привязанных к
  текущему потоку.
- Обычная модель: один общий `Connection` на MDBX environment и не более одной
  активной транзакции на поток.
- `Transaction`, raw `MDBX_txn*` и курсоры MDBX нельзя передавать или
  использовать между потоками.
- Переданный пользователем `Transaction` / raw `MDBX_txn*` также должен
  принадлежать тому же MDBX environment, что и таблица или sync engine,
  который его получает. Handles из другого environment отклоняются через
  `std::invalid_argument` до использования DBI handles.
- `configure()`, `connect()`, `disconnect()` и уничтожение `Connection` — это
  lifecycle-операции вне параллельной работы с таблицами.
- Используйте `shutdown()` для согласованной остановки: он запрещает новые
  транзакции, ждёт закрытия transaction handles в их потоках-владельцах и
  затем отключает environment. Используйте `shutdown_for(timeout)`, если нужно
  ограничить время ожидания.
- Используйте `disconnect()` только когда все транзакции/курсоры уже завершены;
  он возвращает ошибку `MDBX_BUSY`, а не abort'ит транзакции из других потоков.
- Модель следует правилам MDBX для `mdbx_txn_begin()` и `mdbx_env_close_ex()`:
  [Transactions](https://libmdbx.dqdkfa.ru/group__c__transactions.html) и
  [Opening & Closing](https://libmdbx.dqdkfa.ru/group__c__opening.html).

### 🔄 Sync-репликация
- Экспериментальный sync включается явно: определите `MDBXC_SYNC_ENABLED=1`
  перед подключением `mdbx_containers/sync.hpp`.
- v0.1 захватывает обычные write-path'ы `KeyValueTable`, `KeyTable`,
  `ValueTable` и `SequenceTable`; `VectorStore` реплицируется косвенно через
  внутренние `SequenceTable` и `KeyValueTable`.
- `AnyValueTable`, `KeyMultiValueTable`, `KeyOrderedMultiValueTable` и
  `HashedKeyValueStore` не реплицируются в v0.1. Для `KeyMultiValueTable` в
  `sync/DESIGN.md` описан отложенный unordered multiset sync design.
  `KeyOrderedMultiValueTable` является order-sensitive table API, но её
  sync capture/apply контракт остаётся выключенным, пока не появятся capture и
  round-trip tests. Для `AnyValueTable` и `HashedKeyValueStore` ещё нужны
  дизайны type tags и hash-index identity.
- Для поддерживаемых таблиц прикладной CRUD-код не нужно оборачивать
  отдельными sync-вызовами на каждый метод. Прикрепите
  `ThreadLocalChangeAccumulator` к пишущему `Connection`; используйте
  `SyncCaptureScope` для ограниченных фаз записи или более низкоуровневую пару
  `attach_sync_capture()` / `detach_sync_capture()` для намеренно долгоживущего
  lifecycle захвата на уровне приложения. Вложенные capture scopes должны
  завершаться строго в обратном порядке; не меняйте capture sink соединения
  напрямую, пока scope активен. Закоммиченные одиночные записи становятся
  одиночными sync batches, а явная транзакция, объединяющая несколько
  поддерживаемых таблиц, становится одним атомарным локальным batch.
  Read/search-вызовы не захватываются. Отдельный `SyncWorker` плюс транспорт
  `ISyncPeer` переносят закоммиченные batches между узлами.
- При активном sync capture мутирующие вызовы поддерживаемых таблиц должны
  использовать транзакции, созданные через `mdbx_containers::Transaction` или
  `Connection::begin()` / `commit()`. Caller-created raw writable `MDBX_txn*`
  не может вызвать capture pre-commit hook и отклоняется до мутации.
  Caller-created raw read-only transactions остаются допустимыми для
  read/search snapshot operations. Любое исключение из capture recording или
  flush делает transaction rollback-only; повторный `commit()` отклоняется.
- `SyncEngine` предоставляет pull/push/apply primitives, `DirectSyncPeer`
  используется для in-process синхронизации в тестах и примерах,
  `HttpSyncPeer` задаёт HTTP-shaped adapter seam, `WebSocketSyncPeer` задаёт
  binary message seam, а `SyncWorker` запускает фоновой polling.
  `mdbx_containers/sync/transport.hpp` - umbrella header транспортного слоя.
  Опциональные готовые Simple-Web HTTP/WebSocket binding headers находятся в
  `mdbx_containers/sync/transports/simple_web/`, а опциональный Kurlyk/libcurl
  HTTP client binding находится в `mdbx_containers/sync/transports/kurlyk/`.
  `MDBXC_SIMPLE_WEB_HTTP_TRANSPORT`,
  `MDBXC_SIMPLE_WEB_WEBSOCKET_TRANSPORT` и `MDBXC_KURLYK_HTTP_TRANSPORT`
  включают эти dependency targets. Concrete backend targets задают
  `MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT`,
  `MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT` или
  `MDBXC_HAS_KURLYK_HTTP_TRANSPORT` для условного подключения backend headers.
  Установленный package также экспортирует CMake provider functions для этих
  готовых transport targets.
  См. [sync transport production notes](guides/sync-transport-production.md)
  про TLS/WSS, ротацию токенов, graceful shutdown, structured logging и
  offline dependency builds. См.
  [sync table coverage matrix](guides/sync-table-coverage.md) про текущий
  статус поддержки wrapper'ов и
  [sync v0.1 readiness checklist](guides/sync-v0.1-readiness.md) про
  готовность релиза и отложенные задачи.
  Socket-backed примеры используют эти bindings вместо повторной реализации
  транспорта в каждом файле. Wire-format для специализированных таблиц отложен.
  HTTP auth,
  remote-address checks и rate-limit headers живут в adapter-local policy
  context, а не внутри sync DTO;
  см. `include/mdbx_containers/sync/DESIGN.md`.

### 🗄️ Структура и конфигурация
- Несколько логических таблиц внутри одного MDBX-файла.
- Гибкая конфигурация: `read_only`, `writemap_mode`, `readahead`, `no_subdir`,
  `sync_durable`, `max_readers`, `max_dbs`, `relative_to_exe`.
- В режиме `read_only` wrapper'ы таблиц открывают существующие DBI через
  read-only транзакцию и игнорируют `MDBX_CREATE`; записи всё равно падают через MDBX.
- Подробнее см. `docs/configuration.dox`.

### 🧰 Совместимость
- Header-only использование.
- Зависит только от [libmdbx](https://github.com/erthink/libmdbx).
- Требует C++11 или новее.
- **Windows (MSVC)** пока не поддерживается. Используйте MinGW-w64 (GCC) или
  Clang под Windows.

## 🛠️ Установка

1. Скопируйте каталог `include/` в свой проект или подключите репозиторий как
   submodule.
2. Убедитесь, что `libmdbx` доступна вашей системе сборки. Установите
   `MDBXC_DEPS_MODE=BUNDLED`, чтобы использовать bundled submodule в
   `external/libmdbx`, или `SYSTEM`/`AUTO` для установленного пакета. Если
   проект подключён как subproject, уже существующий parent target
   `mdbx::mdbx`, `mdbx::mdbx-static`, `libmdbx::mdbx` или
   `libmdbx::mdbx-static` переиспользуется до поиска пакета, submodule или
   FetchContent. Parent targets имеют приоритет над `MDBXC_DEPS_MODE`, включая
   `BUNDLED`.
3. Используйте компилятор с поддержкой C++11 или новее.

### Сборка через CMake

```bash
cmake -S . -B build \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=ON \
    -DMDBXC_BUILD_EXAMPLES=ON \
    -DMDBXC_USE_ASAN=ON \
    -DCMAKE_CXX_STANDARD=17
cmake --build build
ctest --test-dir build --output-on-failure
```

> **Предупреждение**
> Собирайте все translation unit'ы, использующие mdbx-containers, с одинаковым
> стандартом C++, настройками выравнивания/упаковки структур и набором feature
> macros. Смешивание C++11 и C++17 сборок или изменение ABI-чувствительных
> define'ов между файлами может привести к ODR-нарушениям и неопределённому
> поведению.

Пользователи Windows могут запускать `.bat`-скрипты, например
`build-mingw-17-examples.bat`, `build-mingw-17-tests.bat` или
`build-mingw-11-tests.bat`.

## 🧪 Примеры использования

### Базовая key-value таблица

```cpp
#include <mdbx_containers/KeyValueTable.hpp>
#include <iostream>
#include <map>

int main() {
    mdbxc::Config config;
    config.pathname = "example.mdbx";
    config.max_dbs = 4;

    auto conn = mdbxc::Connection::create(config);
    mdbxc::KeyValueTable<int, std::string> table(conn, "my_map");

    table.insert_or_assign(1, "Hello");
    table.insert_or_assign(2, "World");

    std::map<int, std::string> result;
    table.load(result);

    for (const auto& pair : result)
        std::cout << pair.first << ": " << pair.second << "\n";

    return 0;
}
```

### Сканирование диапазонов

`range()` использует тот же стиль контейнеров, что `retrieve_all()` и
`operator()()`: `KeyTable` по умолчанию возвращает `std::set`, `KeyValueTable`
возвращает `std::map`, а `KeyMultiValueTable` возвращает `std::multimap`.
Для упорядоченного результата в `KeyTable` и `KeyValueTable` используйте
`range<std::vector>()`; для всех физических пар `KeyMultiValueTable` используйте
`range_vector()`. `range_values()` по умолчанию возвращает `std::vector`, но
может заполнять и другие контейнеры, например `std::set`.

```cpp
auto by_key = table.range(10, 20);
auto ordered_pairs = table.range<std::vector>(10, 20);
auto unique_values = table.range_values<std::set>(10, 20);
```

Упорядоченные key-based таблицы также предоставляют `for_each_range()` для потокового обхода,
`filter_range()` как тонкий collecting-helper, `lower_bound()`/`upper_bound()`,
`first()`/`last()`, `min_key()`/`max_key()`, `range_reverse()`,
`contains_range()`, `count_range()` и `erase_range()`.

### Embedded vector store

`VectorStore` сохраняет embeddings, text и metadata в MDBX-таблицах и
пересобирает точный RAM-индекс при открытии. Это MVP для локального RAG: поиск
точный `O(N * dim)`, все embeddings загружаются в RAM, а ANN/HNSW,
metadata filtering и генерация embeddings не входят в область MVP.

Имена коллекций валидируются, а не переписываются: используйте непустые имена
только из ASCII-букв, цифр, `_` и `-`.

```cpp
#include <mdbx_containers/vector.hpp>
#include <iostream>

mdbxc::Config cfg;
cfg.pathname = "rag.mdbx";
cfg.max_dbs = 8;

mdbxc::VectorStore store(cfg, "docs");

mdbxc::Embedding e1;
e1.dim = 3;
e1.values = {1.0f, 0.0f, 0.0f};

uint64_t id = store.add(e1, "Hello world", "{\"source\":\"test\"}");

mdbxc::Embedding query;
query.dim = 3;
query.values = {1.0f, 0.1f, 0.0f};

auto results = store.search(query, 5);
for (const auto& r : results) {
    std::cout << r.id << " " << r.score << " " << r.text << "\n";
}
```

### Hash-indexed key-value store

```cpp
#include <mdbx_containers/HashedKeyValueStore.hpp>

// Layout LargeValues использует два DBI: hash-index и таблицу записей.
mdbxc::Config config;
config.pathname = "hashed.mdbx";
config.max_dbs = 4;

auto conn = mdbxc::Connection::create(config);
mdbxc::HashedKeyValueStore<std::string, std::string> cache(conn, "cache");

cache.insert_or_assign("url:https://example.test", "queued");
std::string state = cache.at("url:https://example.test");
```

### Таблица только для ключей

```cpp
#include <mdbx_containers/KeyTable.hpp>
#include <set>

mdbxc::KeyTable<std::string> keys(conn, "tags");
keys.insert("active");
keys.insert("archived");

std::set<std::string> restored = keys.retrieve_all();
```

### Таблица одного значения

```cpp
#include <mdbx_containers/ValueTable.hpp>

struct AppState {
    int schema_version = 1;
    int active_profiles = 0;

    std::vector<uint8_t> to_bytes() const;
    static AppState from_bytes(const void* data, size_t size);
};

mdbxc::ValueTable<AppState> state(conn, "app_state");
state.set(AppState{});

AppState loaded = state.get_or(AppState{});
```

### Таблица с несколькими значениями на ключ

```cpp
#include <mdbx_containers/KeyMultiValueTable.hpp>

mdbxc::KeyMultiValueTable<int, std::string> events(conn, "events");
events.insert(7, "created");
events.insert(7, "created"); // одинаковые повторы сохраняются
events.insert(7, "sent");

std::vector<std::string> values = events.find(7);
```

### Упорядоченная таблица с несколькими значениями на ключ

```cpp
#include <mdbx_containers/KeyOrderedMultiValueTable.hpp>

mdbxc::KeyOrderedMultiValueTable<int, std::string> timeline(conn, "timeline");
timeline.append(7, "created");
timeline.append(7, "created"); // одинаковые повторы сохраняются
timeline.append(7, "sent");

std::vector<std::string> ordered_values = timeline.find(7);
```

### Ручная транзакция

```cpp
mdbxc::Config config;
config.pathname = "txn.mdbx";
auto conn = mdbxc::Connection::create(config);
mdbxc::KeyValueTable<int, std::string> table(conn, "demo");
mdbxc::ValueTable<int> schema(conn, "schema");

auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
table.insert_or_assign(10, "ten", txn);
schema.set(1, txn);
txn.commit();
```

### Закрытие connection

Используйте `disconnect()`, когда lifecycle уже чистый: все транзакции и курсоры
закрыты.

```cpp
{
    mdbxc::KeyValueTable<int, std::string> table(conn, "items");
    table.insert_or_assign(1, "done");
}
conn->disconnect();
```

Используйте `shutdown()`, когда worker-потоки ещё могут завершать текущую
транзакцию. Метод запрещает новые транзакции, ждёт закрытия transaction handles
и затем отключает environment.

```cpp
stop_requested.store(true);
conn->shutdown();
worker.join();
```

Используйте `shutdown_for(timeout)`, когда остановка сервиса должна иметь
ограниченное время ожидания.

```cpp
if (!conn->shutdown_for(std::chrono::seconds(2))) {
    request_worker_stop();
    worker.join();
    conn->shutdown();
}
```

Полный runnable пример находится в `examples/connection_shutdown_example.cpp`.

### Сериализация пользовательской структуры

```cpp
struct MyData {
    int id;
    double value;

    std::vector<uint8_t> to_bytes() const {
        std::vector<uint8_t> bytes(sizeof(MyData));
        std::memcpy(bytes.data(), this, sizeof(MyData));
        return bytes;
    }

    static MyData from_bytes(const void* data, size_t size) {
        MyData out{};
        std::memcpy(&out, data, sizeof(MyData));
        return out;
    }
};

mdbxc::KeyValueTable<int, MyData> table(conn, "my_data");
table.insert_or_assign(42, MyData{42, 3.14});
```

## 📚 Документация

- Больше примеров см. в каталоге `examples/`. Примеры топологий sync кратко
  описаны в `examples/README-sync-RU.md`.
- Команды benchmark-а sync и описание CSV находятся в `benchmarks/README-sync-RU.md`.
- Информация об API и архитектуре находится в Doxygen-страницах `docs/*.dox`.
- Документацию можно сгенерировать через Doxygen; сгенерированные
  `docs/html/` и `docs/latex/` нельзя редактировать вручную.

## 📄 Лицензия

Проект распространяется под лицензией MIT.

Проект может использовать bundled [libmdbx](https://github.com/erthink/libmdbx)
из `external/libmdbx`, распространяемую под Apache License 2.0. Подробнее см.
`docs/libmdbx.LICENSE`.
