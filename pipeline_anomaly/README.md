# Pipeline + Anomaly Detection Sandbox

Мини-ETL на ClickHouse с расчётом агрегатов и автодетектом аномалий.

## Архитектура

Hex-слои:

- `domain` — чистые объекты предметной области (батчи, отчёты, алерты).
- `application` — use-case'ы пайплайна.
- `infrastructure` — интеграция с ClickHouse, генератор синтетики, ML-алгоритмы.
- `presentation` — CLI-обёртка поверх use-case'ов.

## Быстрый старт

```bash
make infra-up
make pipeline
make infra-down
```

## Конфиг

`config/pipeline.yaml` контролит объём синтетики, батчи, источники данных и пороги алертов. Важные секции:

- `clickhouse`: по умолчанию `default/demo@localhost:8123` (пароль задаётся в `docker-compose`).
- `source.type`: `synthetic` (дефолт), `kafka` (newline-json мок в `data/kafka_mock`) или `web3` (заглушка с описанием API).
- `features.windows`: горизонты агрегатов по rolling окнам.
- `quality`: дедупликация (например, по `tx_hash`) и обязательные колонки.
- `alerting`: выбор sink (`stdout`, `http`, `slack`) и реквизиты вебхуков.

## Данные

Генератор теперь имитирует ончейн-потоки: у событий есть `chain_id`, `block_number`, `contract_address`, `tx_hash`, `gas_used`, `calldata_size`. Дополнительно в паре процентов батчей появляются дубли tx-hash — их отлавливает встроенный DQ-чекер. Можно переключиться на `source.type=kafka`, чтобы прочитать mock-топик из `data/kafka_mock/events.ndjson`. Секция `source.web3` описывает параметры будущего Web3-коннектора (stub, который возвращает пустой батч).

## Метрики

- Расчёт агрегатов по "горячим" окнам (`5m`, `1h`): count, mean, std, p95/p05, high-calldata ratio, разбивка по chain_id.
- Детекторы: Z-score, IQR, IsolationForest, DBSCAN и HDBSCAN (включается флагом в конфиге).
- Отчёт в виде JSON + Markdown (в ClickHouse хранится табличная витрина `anomaly_reports`).

## Технологии

- Python 3.11
- ClickHouse (docker-compose)
- scikit-learn, numpy, pandas
- loguru для логов

## Тесты

`make test` — лёгкая проверка доменных сервисов, генераторов и моков + unit-тесты на `ComputeAggregates` и `DetectAnomalies`.

## Алертинг

- `stdout` — дефолт, пишет отчёты в консоль.
- `http` — отправляет JSON отчёт на произвольный webhook URL.
- `slack` — форматирует markdown-сообщение и отправляет в Slack webhook (опционально можно задать `channel`).

## Airflow

- DAG лежит в `airflow/dags/pipeline_anomaly_dag.py` и просто вызывает CLI внутри `PythonOperator`. Переменная окружения `PIPELINE_CONFIG` позволяет подменить путь к yaml.
- Быстрый смоук можно сделать через `make airflow-dag-test`, который дергает `airflow dags test pipeline_anomaly <ts>`. Нужен установленный Apache Airflow (либо глобально, либо через отдельный virtualenv).

## Grafana

В `docker-compose` добавлена Grafana (порт `3000`). После `make infra-up`:

1. Авторизуйтесь в Grafana (`admin` / `admin` по умолчанию).
2. Datasource ClickHouse создаётся автоматически (используется официальный плагин `grafana-clickhouse-datasource`).
3. Импортируйте дэшборд `grafana/dashboards/pipeline.json` — на нём есть 24h метрики, heatmap по сетям и timeline аномалий.

## Next steps / roadmap

- Подменить `Web3DatasetSource` на реальный коннектор к RPC (batch queries + pagination).
- Расширить Kafka ingestion (Schema Registry, Avro/Protobuf).
- Подключить Slack/HTTP sink к реальным каналам и добавить SLO/metrics.
- Автоматизировать деплой Grafana dashboards через CI/CD.
