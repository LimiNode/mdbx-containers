# Проектирование выборочной репликации

Этот документ задаёт предлагаемый контракт для репликации выбранного набора raw
user DBI. Это проект будущей реализации, а не API v0.1. Нынешние observers с
фильтром по таблицам ограничивают только локальную доставку callback; они не
фильтруют захват, транспорт, применение, снимки или хранение истории.

Сначала прочитайте [восстановление и полные снимки sync](../docs/sync-recovery-RU.md)
и [матрицу покрытия таблиц sync](sync-table-coverage-RU.md). Английская версия:
[sync-selective-replication-design.md](sync-selective-replication-design.md).

## Цель и граница

Область репликации позволяет приложению независимо реплицировать явный
стабильный набор именованных user DBI, не затрагивая полную raw-базу. Она нужна,
когда получателю требуются лишь отдельные таблицы либо разные группы получателей
владеют разными наборами таблиц.

Это не фильтр строк, ключей, диапазонов, предикатов, tenants или callback. DBI
целиком входит в одну raw-область репликации либо не участвует в выборочной raw
репликации. Фильтр `ISyncApplyObserver` по таблицам остаётся удобством для
инвалидации и не означает изоляцию доставки.

Первый scoped-протокол ограничен raw DBI поддержанных таблиц. Из него исключены:

- служебные DBI `_mdbxc_`;
- DBI, которой владеет logical schema, ordered delivery или logical adapter;
- зарегистрированная DBI `VersionedKeyValueTable`;
- DBI, которой уже владеет другая активная raw-область репликации.

Для logical datasets нужна собственная семантика областей: их состояние включает
schema, replay, порядок и delivery конкретному получателю. Их нельзя копировать
как raw-подмножество.

## Обязательные инварианты

1. **Идентификатор области постоянный и явный.** `ScopeId` — непрозрачная,
   заданная приложением непустая каноническая строка байтов. Он не выводится из
   списка DBI, локального DBI handle или маршрута получателя.
2. **Состав неизменен для одного идентификатора области.** Постоянный descriptor
   сопоставляет одному `ScopeId` отсортированный уникальный manifest именованных
   DBI и требуемых DBI flags. До любой мутации источник и получатель сравнивают
   descriptor целиком.
3. **У raw DBI один владелец репликации.** При захвате и применении scoped DBI
   не может также попасть в существующий unscoped raw changelog. Транзакция,
   изменяющая DBI разных raw-владельцев, отклоняется до commit; реализация не
   вправе разбить одну транзакцию приложения на несвязанные реплицируемые commit.
4. **Прогресс ведётся по области и origin.** Существующий cursor
   `_mdbxc_applied` остаётся записью прогресса только для полной raw
   репликации. Scoped progress хранится в отдельном постоянном store по ключу
   `(ScopeId, origin_node_id)` и продвигается лишь после commit непрерывного
   scoped batch.
5. **История хранится по области.** Источник хранит и чистит scoped history по
   scoped cursor contract. Получатель, отставший от сохранённой истории,
   получает `SnapshotRequired` именно для области и не принимает поздние batch.
6. **Scoped baseline атомарен.** Завершённый baseline заменяет только manifest
   descriptor-а и в той же MDBX-транзакции записывает scoped tail всех origin.
   Он не пишет и не проверяет global raw cursor.
7. **Несовпадение descriptor-а отклоняется.** Разный состав, DBI flags,
   descriptor revision, source `DbId`, scope identity или неизменяемые
   metadata snapshot session делают request/session недействительным.
   Получатель не строит best-effort пересечение.

## Descriptor и изменение состава

Реализация должна постоянно хранить `ScopedReplicationDescriptor`:

```text
ScopeId
descriptor_revision
sorted unique [(dbi_name, dbi_flags)]
```

`descriptor_revision` выдаёт источник и монотонно увеличивает его для
диагностики и проверки snapshot session; он не заменяет полное сравнение
manifest-а.

Состав не меняется на месте. Добавление, удаление, переименование DBI или смена
её flags создаёт новый `ScopeId` и descriptor. Приложение должно завершить или
вывести из эксплуатации старую область по своей retention policy, bootstrap-ить
новую область и лишь затем направить на неё получателей. Это не даёт получателю
незаметно принять cursor для `{orders}` за cursor для `{orders, invoices}`.

Приложение может намеренно создать две неизменяемые области с разными наборами
DBI. Их составы должны быть непересекающимися. Поэтому перенос таблицы —
контролируемый cutover, а не автоматическое изменение состава.

## Захват, wire и применение

Scoped replication требует отдельного семейства протокола, а не дополнения
существующих raw `PullRequest`, `PushRequest`, `ChangeBatch` и
`FullSnapshotChunk` необязательным фильтром DBI. Иначе global cursor мог бы
выглядеть применённым после обработки лишь части его операций.

Предлагаемое семейство содержит следующие концептуальные записи:

```text
ScopedPullRequest     = DbId + requester + ScopeId + scoped cursor
ScopedPullResponse    = descriptor + непрерывные scoped batches или error
ScopedChangeBatch     = ScopeId + origin + sequence области + operations
ScopedSnapshotRequest = ScopeId + пустой scoped cursor
ScopedSnapshotChunk   = неизменяемый descriptor + scoped source tail + page data
```

Конкретная версия codec, имена capability и публичные C++ types определяются
только в implementation PR. Peer без scoped capability отклоняет request и не
переходит молча к полному raw pull/push.

Каждая операция `ScopedChangeBatch` называет DBI из manifest descriptor-а.
Источник выпускает batch только для транзакции, все захваченные изменения
которой принадлежат одной области. Получатель проверяет descriptor и каждую DBI
до открытия write transaction, применяет batch и атомарно продвигает
соответствующий scoped cursor. Duplicate, gap, foreign origin и batch вне
области отклоняются по тому же принципу непрерывной доставки, что и нынешний
raw cursor.

Unscoped raw replication не меняется для DBI без владельца области. Первая
реализация обязана находить владельца DBI во время capture, а не фильтровать
global changelog постфактум.

## Scoped baseline и resume

Scoped baseline нужен только для восстановления одной области. Он не расширяет
`ManifestOnly`; `ManifestOnly` остаётся ручной заменой физических DBI без
эффекта на прогресс репликации.

При scoped `SnapshotRequired` источник в одной стабильной read transaction
захватывает:

- неизменяемый descriptor и данные всех DBI его manifest-а;
- scope-local source tail по всем origin; и
- непрозрачный snapshot session id вместе с неизменяемыми metadata page zero.

Получатель проверяет и staging-ит все pages до изменения live DBI. На final page
он заменяет DBI manifest-а, записывает scoped progress, равный captured tail, и
удаляет staging одной транзакцией. DBI вне области и `_mdbxc_applied` остаются
без изменений.

Persistent resume, когда он появится, обязан ключевать staging как минимум по
`ScopeId`, source `DbId` и snapshot id. Resume допустим только для session с
тем же descriptor-ом и metadata page zero. Новый scoped baseline для того же
`ScopeId`, отключение persistent staging, отмена или
`SnapshotSessionInvalid` источника удаляют это staging. Session одной области
никогда не возобновляет другую область.

Первое правило допуска получателя должно быть консервативным: каждая DBI из
descriptor-а отсутствует либо явно доступна для замены через scoped-baseline
API, и никакой конфликтующий unscoped или logical progress на неё не претендует.
Точная overwrite policy определяется вместе с implementation API, но она должна
быть явной: partial baseline не является неявным ремонтом постороннего состояния
реплики.

## Retention и эксплуатационные правила

Источнику нужен scoped changelog и retention watermark для каждой области и
origin. Без этого нельзя безопасно чистить историю, ориентируясь на
`_mdbxc_applied`: получатель может подписаться на одну область, но не на другую.
Оператор хранит scoped history, пока каждый относящийся к ней получатель не
продвинется дальше либо объявленная recovery policy не разрешит scoped baseline.

Изменение membership области у получателя требует application-coordinated
cutover: остановить старый scoped worker, дождаться drain старого маршрута либо
сохранить его recovery horizon, bootstrap-ить новую область и запустить worker
с нового scoped cursor. Никакой worker не интерпретирует старый scoped progress
как progress нового descriptor-а.

Аутентификация и авторизация остаются transport-local, но production adapter
обязан авторизовать запрошенный `ScopeId` дополнительно к `DbId` и identity
узла. Названия областей могут раскрывать топологию приложения и сами по себе не
являются access control.

## Не-цели

Этот дизайн намеренно не предоставляет:

- фильтрацию строк, ключей, диапазонов, предикатов или tenants внутри DBI;
- атомарные записи через несколько областей и автоматическое разделение
  транзакции;
- автоматическое обнаружение или миграцию состава области;
- выборочную репликацию logical tables;
- fan-out на несколько peer-ов, разрешение конфликтов или CRDT semantics; и
- переиспользование global complete-database recovery state для partial scope.

## Последовательность реализации и acceptance tests

Реализацию следует разбить на проверяемые PR:

1. Постоянное хранение и проверка неизменяемых scope descriptor-ов, exclusive
   raw DBI ownership и negative registration tests.
2. Scope-local capture/changelog/cursor storage и доказательство, что mixed
   scope либо scoped-plus-unscoped write transaction отклоняется до commit.
3. Capability-gated scoped pull/push и tests для contiguous delivery,
   duplicates, gaps, foreign scope, descriptor mismatch и restart.
4. Scoped snapshot sessions, staging и optional persisted resume с atomic final
   replacement и bootstrap scoped cursor.
5. Retention/pruning и tests контролируемого membership cutover.

Каждая фаза запускает релевантные C++11 и C++17 sync tests. End-to-end tests
должны показать, что два получателя сходятся по разным непересекающимся областям,
что DBI вне области никогда не появляется у получателя и что scoped baseline не
может изменить global raw progress.
