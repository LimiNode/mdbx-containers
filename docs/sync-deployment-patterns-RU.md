# Практические схемы развёртывания sync

Sync в `mdbx-containers` поддерживает multi-origin transport: реплика может
принимать закоммиченные истории изменений от нескольких устойчивых origin с
`NodeId`. Это делает несколько пишущих узлов практичными, когда их записи не
конкурируют за одну logical record.

При этом система пока не задаёт concurrent conflict resolution для двух
writers, меняющих один logical key. В v0.1 по умолчанию используется
`ConflictPolicy::Reject`; `LastWriterWins` зарезервирован для будущего дизайна
и отклоняется `SyncEngine`. Поэтому поддержка нескольких origins не является
контрактом last-writer-wins или CRDT.

English version: [sync-deployment-patterns.md](sync-deployment-patterns.md).

## Одна БД получателя, несколько origins

Несколько origins не требуют нескольких пользовательских БД у получателя. Их
истории, progress и replay state ведёт sync subsystem; входящие operations
применяются к одним и тем же пользовательским MDBX tables, выбранным по DBI
name. Поэтому storage node может принимать данные redundant collectors в одну
canonical database.

```text
collector A ──┐
              ├── одна MDBX database
collector B ──┘       user table: trades
```

Модель данных приложения, а не origin id, определяет, являются ли records одним
logical event. Identity origin-а сохраняет значение для delivery и diagnostics,
а при необходимости может быть отдельно записана как provenance data.

## Сначала выберите модель данных, затем топологию

Для реплицируемого набора данных используйте одну из двух схем:

1. Назначьте каждому writer непересекающийся диапазон logical keys.
2. Храните immutable records с глобально однозначными identities.

Вторая схема часто лучше подходит для event data. Она позволяет нескольким
writers работать без необходимости определять победителя среди concurrent
updates одной mutable record.

| Сценарий | Текущий контракт sync |
| --- | --- |
| Разные nodes пишут разные logical keys | Поддерживаемая multi-origin topology |
| Несколько nodes создают uniquely identified immutable records | Практичная multi-writer topology |
| Два nodes одновременно меняют один logical key | Нет определённой conflict-resolution semantics |
| Один node удаляет, а другой пишет тот же logical key | Нет гарантированной convergence semantics |
| Независимые вызовы `SequenceTable::append()` | Только single-writer; appenders надо сериализовать внешне |
| Destructive multi-writer операции `KeyMultiValueTable` | Нужен один writer или application-level causal serialization |

Не используйте только wall-clock time как authority порядка: часы разных nodes
могут расходиться. Если приложению нужно выбрать победителя, до обработки
данных как mutable replicated state определите и сохраните authority уровня
приложения, например upstream sequence number или deterministic version tuple.

## Отказоустойчивые рыночные данные

### Primary и hot standby

Один active collector публикует dataset, а standby следит за upstream feed и
может стать active после application-coordinated failover. Это простейшая
topology, потому что для каждого key range одновременно работает один writer.
Приложению всё ещё нужна failover procedure, которая продолжает upstream feed
без неучтённого gap.

### Active-active collectors для immutable events

Для trades и других immutable upstream events используйте canonical identity
биржи как ключ user table, например:

```text
(exchange, symbol, trade_id) -> Trade
```

Два collectors, получившие один trade, должны дать один logical key и одинаковый
serialized payload. Тогда любой из collectors может заполнить event gap другого
в одной БД получателя.

Текущий raw sync ещё не реализует canonical-event deduplication и integrity
error для одинаковых keys с разными payloads. Поэтому active-active deployment
должен проверять такую divergence в application code и не должен считать raw
upsert ordering integrity- или conflict-resolution guarantee. Одинаковый event
identity с разными payloads — это data-integrity anomaly, а не случай для
молчаливого выбора writer-а.

### Сохраняйте provenance observations, когда она нужна

Храните canonical events и observations collectors отдельно, когда нужно
измерять coverage или latency feed-а:

```text
trades:       trade_id -> Trade
observations: (trade_id, collector_id) -> Observation
```

Так сохраняется одна история рынка и достаточно данных для расчёта missing
coverage, collector lag или source divergence.

### Mutable bars и snapshots

Open bar, latest quote или upstream snapshot не являются immutable event. Два
collectors могут законно увидеть разные состояния одного key. У текущего raw
sync нет source-version-wins поведения для этого случая: используйте одного
authoritative projector-а или application-level serialization, пока не появится
versioned conflict policy.

Нужный будущий primitive — versioned register: put или delete несёт
application-provided source-authoritative version, отдельную от user value, а
получатель атомарно оставляет наибольшую version и deletion tombstones. Broker
timestamp может быть одним из компонентов, но при равных timestamps нужен source
sequence либо другой deterministic tie-breaker. Wall-clock time writer-а и время
приёма пакета не подходят как authority.

## Поддерживаемые пути таблиц

Raw capture v0.1 записывает обычные writes `KeyValueTable`, `KeyTable`,
`ValueTable` и `SequenceTable`, когда к пишущему `Connection` прикреплён
`ThreadLocalChangeAccumulator`.

`KeyMultiValueTable` использует explicit logical adapter вместо raw capture.
Его destructive operations требуют одного writer или application-level causal
serialization. Для raw replication `VectorStore` и его logical adapter также
нужен один authoritative либо externally serialized writer на collection.
Полный контракт для каждой таблицы приведён в
[sync table coverage matrix](../guides/sync-table-coverage.md).

## Коллекторы рыночных данных

Моделируйте raw market-data layer как immutable events. Каждый collector может
писать свои records и синхронизировать их с aggregation или storage nodes.

Для фидов с authoritative upstream sequence identity записи может быть такой:

```text
(exchange, symbol, stream, exchange_timestamp, exchange_sequence)
```

Если два collectors намеренно наблюдают один feed для redundancy, выберите одну
из двух явных моделей:

- сохранять оба observations с `(source_node, exchange, symbol,
  exchange_event_id)`;
- дедуплицировать по `(exchange, symbol, exchange_event_id)`, если биржа даёт
  стабильную event identity.

Не позволяйте collectors конкурентно перезаписывать mutable records наподобие
`latest_quote["BTCUSDT"]` или формирующейся OHLCV candle. Рассматривайте latest
quotes, candles и похожие aggregates как derived state: пересчитывайте их из
immutable ticks и trades либо назначайте одного authoritative projector для
каждого derived key.

```text
collector A ── immutable events ──┐
                                  ├── aggregation or storage node
collector B ── immutable events ──┘
```

Эта схема также поддерживает ownership или sharding, например по exchange,
symbol range, market type или identity collector-а.

## Records трейдеров и платформы

Представляйте trades, fills, ledger events и audit records как immutable events
с globally unique identities. Identity может включать выданный платформой trade
или execution id либо сгенерированный приложением id, scoped by origin.
Явно назначайте ownership, когда одна business record может появиться более чем
на одном node.

Реплицируйте event history, а balances, positions и reporting views выводите из
этой истории либо обновляйте через одного authoritative writer. Не полагайтесь
на raw sync для координации concurrent mutations одного account balance, order
state или risk limit: такое решение требует application-level ownership,
serialization или отдельно спроектированного conflict protocol.

## Базы знаний и RAG chunks

Храните source material и chunk revisions как immutable, versioned records.
Identity chunk-а может объединять stable document id, source revision и chunk
ordinal либо использовать content hash вместе с версиями codec и embedding.
Так разные ingestion nodes смогут добавлять отдельные document revisions без
конкуренции за один mutable key.

Считайте mutable projections derived state. К ним относятся current-document
pointer, search index, cached retrieval scores и vector collection, построенная
из chunks. Перестраивайте такие projections из immutable corpus либо назначайте
одного authoritative writer. В частности, текущие пути sync `VectorStore` не
дают independent multi-writer id allocation или conflict resolution для одной
collection.

## Чек-лист развёртывания

Перед включением sync для набора данных определите:

- durable `NodeId` каждого writer-а и общий `DbId` replication set-а;
- logical identity каждой реплицируемой record и writer-а, которому она
  принадлежит;
- является ли record immutable либо какая application authority сериализует её
  mutations и deletions;
- стратегию rebuild или ownership для derived state;
- table-specific sync path из
  [coverage matrix](../guides/sync-table-coverage.md).

Настройка capture, transport deployment и recovery описаны в
[sync overview](sync-RU.md),
[transport production notes](../guides/sync-transport-production.md) и
[recovery guide](sync-recovery-RU.md).
