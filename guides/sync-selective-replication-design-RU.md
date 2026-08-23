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

Выборочная область — дополнительная проекция полного raw stream, а не замена
исходного контура репликации. Каждая поддержанная raw DBI продолжает попадать в
существующий global changelog и снимок `CompleteUserDatabase` ровно как прежде.
Область добавляет собственные capture, progress, retention и delivery для
выбранных DBI. Поэтому существующие full-raw receivers и их cursors
`_mdbxc_applied` сохраняют корректность, а другой получатель может подписаться
на одну или несколько областей. Scoped receiver не выполняет одновременно
global raw pull для того же `DbId`.

```text
транзакция источника { orders в Scope X, catalog без scope }
                    |                            |
                    +------ global batch --------+
                    |      { orders, catalog }   | --> получатель полной репликации / global cursor
                    |
                    +------ scoped batch --------+ --> получатель Scope X / scoped cursor
                           { orders }
```

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
2. **Состав и право записи неизменны для одного идентификатора области.**
   Постоянный descriptor сопоставляет одному `ScopeId` один ненулевой
   назначенный origin с правом записи и отсортированный уникальный manifest
   именованных DBI и требуемых DBI flags. До любой мутации источник и получатель
   сравнивают descriptor целиком.
3. **Global history остаётся полной; selective membership эксклюзивен.** Scoped
   DBI продолжает попадать в global raw changelog, но может принадлежать не
   более чем одной selective области. Реализация захватывает global batch и
   scoped projection в той же фиксируемой MDBX-транзакции.
4. **Прогресс — один поток на область.** Существующий cursor `_mdbxc_applied`
   остаётся записью прогресса только для полной raw репликации. Scoped progress
   хранится в отдельном постоянном store по ключу `ScopeId`, отражает
   scope-local sequence назначенного origin с правом записи и продвигается лишь
   после commit непрерывного scoped batch.
5. **История хранится по области.** Назначенный origin с правом записи хранит и
   чистит scoped history по scoped cursor contract. Получатель, отставший от
   сохранённой истории, получает `SnapshotRequired` именно для области и не
   принимает поздние batch.
6. **Scoped baseline атомарен.** Завершённый baseline заменяет только manifest
   descriptor-а и в той же MDBX-транзакции записывает scoped tail назначенного
   origin с правом записи. Он не пишет и не проверяет global raw cursor.
7. **Несовпадение descriptor-а отклоняется.** Разный состав, DBI flags,
   назначенный origin с правом записи, source `DbId`, scope identity или
   неизменяемые metadata snapshot session делают request/session недействительным.
   Получатель не строит best-effort пересечение.

## Descriptor и изменение состава

Реализация должна постоянно хранить `ScopedReplicationDescriptor`:

```text
ScopeId
designated_writer_origin (ненулевой NodeId)
sorted unique [(dbi_name, dbi_flags)]
```

**Глобальный инвариант записи.** Selective scope v1 требует одного глобально
контролируемого origin с правом записи для каждой DBI из manifest-а. До
активации selective replication приложение обязано установить и проверить тот
же descriptor на каждом известном origin для `DbId`, способном открыть DBI из
manifest-а для локальной записи приложения. Назначенный origin с правом записи
принимает такие записи; каждый другой origin обязан отклонить их до мутации и
global capture. Origin без descriptor-а должен быть настроен без локального
доступа на запись в scoped DBI; иначе topology недействительна и область нельзя
активировать. Это правило обеспечивает приложение либо установленные scope
guards. Развёртывание descriptor-а и отзыв права записи координирует
приложение; в этом дизайне v1 нет динамического обнаружения origin с правом
записи.

Ни состав, ни право записи не меняются на месте. Добавление, удаление,
переименование DBI, смена её flags или назначенного origin с правом записи
создают новый `ScopeId` и descriptor. Приложение должно завершить или вывести из
эксплуатации старую область по своей retention policy, bootstrap-ить новую
область и лишь затем направить на неё получателей. Это не даёт получателю
незаметно принять cursor для `{orders}` за cursor для `{orders, invoices}` или
считать sequence нового origin с правом записи продолжением потока прежнего.

Приложение может намеренно создать две неизменяемые области с разными наборами
DBI. Их составы должны быть непересекающимися. Поэтому перенос таблицы —
контролируемый cutover, а не автоматическое изменение состава. Смена origin с
правом записи — такой же контролируемый cutover, а не failover внутри существующей
области.

## Захват, wire и применение

Scoped replication требует отдельного семейства протокола, а не дополнения
существующих raw `PullRequest`, `PushRequest`, `ChangeBatch` и
`FullSnapshotChunk` необязательным фильтром DBI. Иначе global cursor мог бы
выглядеть применённым после обработки лишь части его операций.

Существующий global raw protocol остаётся полным и неизменным: global pull и
push переносят все поддержанные raw DBI, а `CompleteUserDatabase` по-прежнему
экспортирует все именованные неслужебные user DBI. Scoped peer использует новое
семейство протокола только для своих manifest-ов. Среда получателя выбирает
один постоянный raw delivery mode для `DbId`:

- **full-global mode** потребляет существующий полный raw stream и не
  потребляет scoped streams; либо
- **selective mode** потребляет одну или несколько непересекающихся областей и
  не потребляет global raw stream для этого `DbId`.

Режим выбирается для всего получателя, а не для отдельной DBI: один global
`ChangeBatch` может атомарно содержать операции и scoped, и unscoped DBI.
После фильтрации таких операций у selective receiver-а неполный global cursor
снова выглядел бы корректным. Один full-global receiver и несколько
selective receivers могут сосуществовать для одной базы источника.

Предлагаемое семейство содержит следующие концептуальные записи:

```text
ScopedPullRequest     = DbId + requester + ScopeId + scoped cursor
ScopedPullResponse    = descriptor + непрерывные scoped batches или error
ScopedChangeBatch     = ScopeId + назначенный origin с правом записи + sequence области + operations
ScopedSnapshotRequest = ScopeId + пустой scoped cursor
ScopedSnapshotChunk   = неизменяемый descriptor + scoped source tail + page data
```

Конкретная версия codec, имена capability и публичные C++ types определяются
только в implementation PR. Peer без scoped capability отклоняет request и не
переходит молча к полному raw pull/push. Scoped request направляется
назначенному descriptor-ом origin с правом записи; другой peer отклоняет его,
а не передаёт scoped history дальше.

Каждая операция `ScopedChangeBatch` называет DBI из manifest descriptor-а, а
его origin обязан совпадать с назначенным в descriptor-е origin с правом
записи. Только этот origin может выполнять локальные мутации приложения в scoped DBI и
создавать её scoped projection; локальная мутация от другого origin
отклоняется до commit. Это не меняет full-global apply: узел по-прежнему может
применить raw history, полученную от другого origin, но не может выдавать её за
scoped relay.

Global batch всегда содержит полную транзакцию приложения. Если транзакция на
назначенном origin с правом записи изменяет DBI одной selective области,
источник атомарно добавляет scoped batch только с операциями этой области; при этом
транзакция может изменить и unscoped DBI. Транзакция, изменяющая DBI двух
selective областей, отклоняется до commit, а не разделяет scoped effects.
Получатель проверяет descriptor, назначенный origin с правом записи и каждую
DBI до открытия write transaction, применяет batch и атомарно продвигает
соответствующий scoped cursor. Duplicate, gap, origin, не совпадающий с
назначенным, и batch вне области отклоняются по тому же принципу непрерывной доставки, что и
нынешний raw cursor.

Первая реализация обязана находить membership области и назначенный origin с
правом записи во время capture и писать scoped projection рядом с global
capture, а не вместо него. Global changelog нельзя фильтровать постфактум.

## Scoped baseline и resume

Scoped baseline нужен только для восстановления одной области. Он не расширяет
`ManifestOnly`; `ManifestOnly` остаётся ручной заменой физических DBI без
эффекта на прогресс репликации.

При scoped `SnapshotRequired` источник в одной стабильной read transaction
захватывает:

- неизменяемый descriptor и данные всех DBI его manifest-а;
- scope-local source tail назначенного origin с правом записи; и
- непрозрачный snapshot session id вместе с неизменяемыми metadata page zero.

Scoped baseline выдаёт напрямую назначенный origin с правом записи; состояние
его DBI в захваченной read transaction считается каноническим для области.
Узел, который лишь получил его global raw history, не является источником
scoped baseline.
Получатель проверяет и staging-ит все pages до изменения live DBI. На final
page он заменяет DBI manifest-а, записывает scoped progress, равный captured
tail, и удаляет staging одной транзакцией. DBI вне области и `_mdbxc_applied`
остаются без изменений.

Persistent resume, когда он появится, обязан ключевать staging как минимум по
`ScopeId`, source `DbId` и snapshot id. Resume допустим только для session с
тем же descriptor-ом и metadata page zero. Новый scoped baseline для того же
`ScopeId`, отключение persistent staging, отмена или
`SnapshotSessionInvalid` источника удаляют это staging. Session одной области
никогда не возобновляет другую область.

Первое правило допуска получателя должно быть консервативным: каждая DBI из
descriptor-а отсутствует либо явно доступна для замены через scoped-baseline
API, у receiver-а нет active global raw cursor для этого `DbId`, и logical
apply state не претендует на эти DBI. Точная overwrite policy определяется
вместе с implementation API, но она должна быть явной: partial baseline не
является неявным ремонтом постороннего состояния реплики. Перевод
существующего full-global receiver-а в selective mode требует явной процедуры
fresh receiver либо reset-and-bootstrap; это не in-place conversion cursor-а.

## Retention и эксплуатационные правила

Назначенный origin с правом записи сохраняет существующий полный global
changelog по его нынешнему контракту и дополнительно нуждается в scoped changelog и
retention watermark для каждой области. Без этого нельзя безопасно чистить
scoped history, ориентируясь на `_mdbxc_applied`: получатель может подписаться
на одну область, но не на другую. Оператор хранит scoped history, пока каждый
относящийся к ней получатель не продвинется дальше либо объявленная recovery
policy не разрешит scoped baseline.

Изменение membership области у получателя требует application-coordinated
cutover: остановить старый scoped worker, дождаться drain старого маршрута либо
сохранить его recovery horizon, bootstrap-ить новую область и запустить worker
с нового scoped cursor. Никакой worker не интерпретирует старый scoped progress
как progress нового descriptor-а. Переключение full-global и selective receiver
modes — отдельный cutover с описанной выше процедурой fresh receiver либо
reset-and-bootstrap.

Аутентификация и авторизация остаются transport-local, но production adapter
обязан авторизовать запрошенный `ScopeId` дополнительно к `DbId` и identity
узла. Названия областей могут раскрывать топологию приложения и сами по себе не
являются access control.

## Не-цели

Этот дизайн намеренно не предоставляет:

- фильтрацию строк, ключей, диапазонов, предикатов или tenants внутри DBI;
- атомарные записи через несколько областей и автоматическое разделение
  транзакции;
- hybrid receiver, который потребляет global raw для одних DBI и scoped delivery
  для других;
- автоматическое обнаружение или миграцию состава области;
- выборочную репликацию logical tables;
- multi-origin selective replication, failover назначенного origin с правом записи или
  relay scoped history/baseline через узел, получивший лишь global raw history;
- fan-out на несколько peer-ов, разрешение конфликтов или CRDT semantics; и
- переиспользование global complete-database recovery state для partial scope.

## Последовательность реализации и acceptance tests

Реализацию следует разбить на проверяемые PR:

1. Постоянное хранение и проверка неизменяемых scope descriptor-ов, включая
   ненулевой назначенный origin с правом записи, exclusive selective membership
   и negative registration tests.
2. Scope-local capture/changelog/cursor storage и доказательство, что
   scoped-plus-unscoped transaction публикует атомарный полный global batch и
   его scoped projection, а mixed-scope transaction отклоняется до commit.
3. Capability-gated scoped pull/push и tests для contiguous delivery,
   duplicates, gaps, origin, не совпадающего с назначенным, foreign scope,
   descriptor mismatch и restart.
4. Scoped snapshot sessions, staging и optional persisted resume с atomic final
   replacement и bootstrap scoped cursor.
5. Retention/pruning и tests контролируемого membership cutover.

Каждая фаза запускает релевантные C++11 и C++17 sync tests. End-to-end tests
должны показать, что два получателя сходятся по разным непересекающимся областям,
что DBI вне области никогда не появляется у получателя и что scoped baseline не
может изменить global raw progress. Они также должны доказать, что добавление
области не удаляет её DBI из changelog или complete-database baseline
существующего full-raw receiver-а, что локальный origin, не назначенный для
записи,
отклоняется до мутации и global capture, что после restart этот origin
восстанавливает descriptor и всё ещё отклоняет запись, что scoped baseline
принимается только от назначенного origin с правом записи, что смена origin с
правом записи использует новый `ScopeId` и что один получатель не может
применить global и scoped delivery для одного `DbId`. В частности, test с
назначенным origin `A` и не назначенным origin `B`, использующими один
descriptor, должен доказать, что `B` не может записать scoped DBI ни до, ни
после restart.
