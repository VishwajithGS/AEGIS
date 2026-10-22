# AEGIS — technology stack guide

Why each tool is in AEGIS, what it does there, what else could have been used,
and what it costs. Versions are the ones pinned in `requirements/*.txt`,
`frontend/package.json`, `docker-compose.yml` and the Dockerfiles.

## 1. Decision matrix

| Technology | Where AEGIS uses it | Problem it solves | Why selected | Alternatives considered | Trade-offs | Status |
|---|---|---|---|---|---|---|
| **Python 3.11** | every backend component | one language for producers, Spark jobs, API, tests | PySpark, confluent-kafka, happybase, FastAPI all have first-class Python APIs | Java/Scala (native Spark), Go | Python UDFs are slower than JVM code; GIL | verified |
| **Apache Kafka 4.3.1** (KRaft) | 6 topics; producers → streaming → API | decouple fast producers from slower processing; replayable, ordered, partitioned log | durable log + offsets + consumer groups; first-class Spark connector; course requirement | RabbitMQ, Redis Streams, REST calls, writing straight to a DB | operational weight; single broker here (no replication) | verified (integration + e2e) |
| **confluent-kafka 2.16** | `producers/kafka_client.py`, admin, consumer check, API live feed | Python client | librdkafka: idempotent producer, delivery callbacks, fast | kafka-python (slower, fewer features) | native library dependency | verified |
| **Apache Spark 3.5.9 / PySpark** | all of `spark_jobs/` | parallel ETL, streaming, SQL, ML in one engine | one API for batch and streaming; Catalyst optimiser; MLlib; Kafka source | pandas, Flink, Dask, plain Python | JVM + Python memory; heavy for small data | verified |
| **Spark RDD API** | `rdd_demo.py` | low-level map/reduce over raw JSON | course requirement; shows lineage and lazy evaluation | — | no optimiser, more code | verified (RDD = DataFrame result) |
| **Spark SQL / DataFrames** | ETL, risk, spatial, features, 10 SQL queries | declarative, schema-aware transformations | optimised plans, same code in streaming | hand-written RDD code | less control than RDDs (not needed) | verified |
| **Structured Streaming** | `streaming_pipeline.py` (3 queries) | event-time windows, watermarks, dedup, fault-tolerant state | reuses the batch transforms; checkpoints; foreachBatch sinks | Kafka Streams (JVM only), Flink, a custom consumer loop | micro-batch latency (10 s trigger, 30–58 s batches measured on this machine) | verified |
| **Spark MLlib** (`pyspark.ml`) | `model_training.py`, `model_inference.py`, API model service | train/evaluate/persist a classifier next to the data | pipelines, persistence, same session as the ETL; course requirement | scikit-learn | needs a JVM to serve predictions in the API | verified |
| **Java 17 (OpenJDK)** | inside `aegis-spark` image | Spark runtime | Spark 3.5 supports 8/11/17 (host has Java 24, unsupported) | — | image size | verified |
| **Apache HBase 2.5.16** + Thrift | 5 tables (`aegis/storage/hbase_repository.py`) | low-latency keyed reads/writes of operational time series with versions and TTLs | row-key range scans, cell versions, sparse wide rows, horizontal scaling model; course requirement (wide-column store) | PostgreSQL, MongoDB, Cassandra, files only | JVM + ZooKeeper + Thrift gateway; no secondary indexes or joins; standalone here | verified (integration + e2e + outage test) |
| **happybase 1.3 / thriftpy2** | repository, Spark sink, API | Python ↔ HBase Thrift | simplest maintained Python HBase client | HBase REST gateway, Java client via Py4J | Thrift adds a hop; connection per partition | verified |
| **Apache Parquet** (+ snappy) | the whole lake | columnar historical storage | column pruning, compression, schemas, partitions; read by Spark and pyarrow | CSV/JSON, Delta Lake/Iceberg, a database | no ACID/time-travel (Delta/Iceberg would add it); many small files from streaming | verified |
| **pyarrow 18.1** | API `ParquetStore` | read Parquet without starting Spark | fast, no JVM in request path | Spark in the API (slow), pandas.read_parquet (uses pyarrow anyway) | per-file overhead on the Windows bind mount (mitigated: file listing + cached row counts) | verified |
| **pandas 2.2 / NumPy 1.26** | API services, vectorised Haversine, NetCDF reader | small in-memory shaping after Parquet reads | standard, fast for API-sized results | Polars | single machine (fine for API responses) | verified |
| **PyYAML** | `aegis/config.py` | one human-readable config with env overrides | simple, comments allowed | TOML, .env only | — | verified |
| **FastAPI 0.151 + Uvicorn 0.54** | `backend/app` | REST + Server-Sent Events, OpenAPI docs | type-driven validation, async SSE, automatic `/docs`, DI | Flask, Django REST, Node/Express | one process; no auth (local demo) | verified (unit + e2e) |
| **Pydantic 2.14** | `schemas.py`, query validation | reject invalid prediction requests | integrated with FastAPI; `extra="forbid"` | manual checks | — | verified (422 tests) |
| **React 18.3 + Vite 5.4** | `frontend/` | interactive 7-tab dashboard | component model, fast dev server/build | Angular, Vue, server-rendered templates, Streamlit | separate Node toolchain | verified (ui_check) |
| **Leaflet 1.9.4 + react-leaflet 4.2** | `RiskMap.jsx` | interactive map with zones, markers, popups, click search | light, open source, no API key, OpenStreetMap tiles | Google Maps (key, terms), Mapbox (key), OpenLayers (heavier) | map tiles need internet | verified |
| **Recharts 2.13** | analytics, zone history, real-data panels | charts from JSON | React-native, declarative | Chart.js, D3 (lower level), Plotly (heavier) | large bundle (> 500 kB warning) | verified |
| **Docker + Compose** | 8 services, 2 custom images | reproducible stack on Windows | one command per service; same Java/Python everywhere; health checks | native installs, Kubernetes | ~8 GB RAM for the full stack | verified |
| **pytest 9.1** | 138 tests | unit, Spark and integration tests | markers, fixtures, skip-with-reason | unittest | — | verified |
| **Playwright** (host only) | `scripts/ui_check.py` | real-browser check of every tab | drives installed Edge, no browser download | Selenium | host-only optional dependency | verified |
| **PowerShell scripts** | `scripts/*.ps1` | Windows-native one-command operation | the project machine is Windows | Makefile/bash | Windows-specific (raw docker commands documented as fallback) | verified |

## 2. Each technology in AEGIS terms

### Apache Kafka
*Simply:* a durable, append-only message log that many producers write to and
many consumers read from at their own pace.
*In AEGIS:* four source topics carry raw events; two derived topics carry
results (`zone-risk-updates`, `data-quality-events`) to the API. Producers use
`acks=all` + idempotence; messages are keyed by source id so each station's
events stay in order in one partition.
*Why not the alternatives:* **RabbitMQ** deletes messages once consumed — no
replay, and Spark's Kafka source with offset checkpoints would be lost.
**Redis Streams** keeps data in memory and has a weaker Spark integration.
**Direct REST calls** would couple producers to the processing speed (a slow
Spark batch would block sensors). **Writing straight to a database** would
make the DB the bottleneck and lose the ordered, replayable stream.
*Limitations here:* one broker, replication factor 1 — no broker fault
tolerance was demonstrated. *Mandatory?* For the live path yes; batch, real and
hybrid modes run without it. *Replacing it* would mean a new streaming source
in `streaming_pipeline.kafka_source()` and a new live-feed consumer.

### Apache Spark (RDD, DataFrame/SQL, Structured Streaming, MLlib)
*Simply:* an engine that splits data into partitions and processes them in
parallel, with one API for files and for streams.
*In AEGIS:* the same `transforms.canonicalize()` runs in the batch ETL and in
streaming, so validation can never drift between the layers; event-time
windows, watermarks and checkpoints come from the engine.
*Why not:* **pandas** — one machine, in memory, no streaming state; **Flink**
— excellent streaming but a second engine for batch/ML in Python is weaker;
**Dask** — no built-in Kafka streaming with watermarks; **plain Python** —
would require writing windowing, state and recovery by hand.
*Limitations:* local mode only (`local[8]` batch, `local[4]` streaming);
Python UDF for the risk formula costs speed; micro-batch latency.
*Replacing it* would touch every file in `spark_jobs/`.

### Apache HBase
*Simply:* a distributed sorted map `row key → column family → column →
versions`, built for fast reads and writes by key and key range.
*In AEGIS:* operational state the dashboard needs quickly: the latest live
risk per zone (`latest_with_prefix`, reverse scan), a zone's last 48 windows,
five versions of a risk cell, single incidents by id, processing metrics.
*Why not:* **PostgreSQL/MySQL** — would work at this size, but the course asks
for a wide-column model, and row-key time-series scans with cell versions and
TTLs are HBase's natural pattern; **MongoDB** — document store, not
wide-column; versions and TTL per column family would be manual;
**Cassandra** — also wide-column and a fair alternative (CQL, no single
master), but HBase is the Hadoop-ecosystem store the course names;
**files only** — no low-latency point lookups or versioned cells.
*Limitations:* standalone mode on the local filesystem; Thrift gateway hop; no
secondary index (lookups only by key prefix). *Optional:* every component
degrades gracefully without it (snapshot and Parquet fallbacks).

### Parquet data lake
*Simply:* column-oriented files with a schema, organised in folders.
*In AEGIS:* bronze/silver/gold/live/quarantine tables partitioned by date
(and topic/domain); the API reads gold tables directly with pyarrow.
*Why not:* **CSV/JSON** — no types, no compression, full scans; **Delta Lake /
Iceberg** — add ACID and time travel, but extra libraries and setup were not
needed for an overwrite-and-append workload; **a database for history** —
history is large, append-mostly and read in column subsets, which suits files.
*Limitations:* streaming appends create many small files (compaction job
provided); no transactional guarantees across tables.

### FastAPI + Pydantic + Uvicorn
*Simply:* a Python web framework where type hints become validation and docs.
*In AEGIS:* 16 endpoints, Server-Sent Events for live updates, request
validation (`PredictRequest`, query bounds), dependency-injected services,
automatic Swagger UI at `/docs`.
*Why not:* **Flask** — no built-in validation or async SSE; **Django REST** —
heavier, ORM not needed; **Node/Express** — a second backend language, and the
model needs PySpark in the same process.

### React + Vite + Leaflet + Recharts
*Simply:* a component UI library, its build tool, a map library and a chart library.
*In AEGIS:* 7 tabs; `useApi` polling (paused when the tab is hidden) +
`EventSource` for live pushes; the map draws 40 risk-coloured rectangles and
incident markers.
*Why not:* **Streamlit/Dash** — fast to build but limited control over live
updates and map interaction; **Google Maps/Mapbox** — API keys and terms;
**server-rendered pages** — no live updates without extra work.

### Docker Compose
*Simply:* a file that describes the containers of the system and how they connect.
*In AEGIS:* Kafka and HBase cannot be installed natively on Windows easily, and
Spark needs Java 17 (the host has Java 24); one image (`aegis-spark`) runs
every Python process with the right versions.
*Why not Kubernetes:* far more setup for a single machine.

## 3. Why not use one tool for everything?

Each layer has a different job, and each job has a different "shape":

| Job | Shape of the work | Tool built for it |
|---|---|---|
| receive events from many sources without losing or blocking them | append-only, high write rate, replay | **Kafka** |
| clean, join, window and aggregate those events | parallel computation with state | **Spark** |
| answer "latest risk of zone X" in milliseconds | key-range point reads, versions | **HBase** |
| keep months of history for analytics and ML | large, columnar, append-mostly | **Parquet** |
| expose results safely to clients | request/response + push | **FastAPI** |
| let a person explore it | interactive UI and map | **React + Leaflet** |

A single database could store events and serve queries, but it would have to
absorb the raw write rate, run the windowed stream processing and serve the
dashboard at the same time — the slowest of those tasks would slow down the
others. Separating them means a slow Spark batch never blocks a sensor (Kafka
buffers), a heavy analytics query never slows the dashboard (HBase/Parquet
are separate), and each part can fail and recover on its own (shown by the
HBase outage test).

## 4. Is this over-engineered for a demo?

Honestly, for 4 events per second and ~300,000 historical records, a single
PostgreSQL database plus a Python script could produce similar numbers.
AEGIS still uses the full stack, for reasons that can be defended:

1. **The learning objectives require it**: Kafka, Spark (RDD, SQL, Structured
   Streaming, MLlib), HBase and an end-to-end ETL are the subject of the course.
2. **The design scales without rewriting**: Kafka partitions, Spark
   partitions and HBase row keys are the same on a cluster; only the
   deployment changes (`local[8]` → a cluster master, more brokers/region
   servers). No distributed run was measured, so this is a design argument,
   not a performance claim.
3. **The problem is genuinely multi-source and continuous**: heterogeneous
   formats, late and duplicate events, and live windows are the hard parts of
   real disaster data, and the stack handles them explicitly (watermarks,
   dedup, quarantine, checkpoints).
4. **Each component is verified**, not just configured (unit, Spark,
   integration tests; 14-step end-to-end check).

The parts most open to the "over-engineering" question are HBase (a
relational table would serve 40 zones easily) and Spark for the small real
dataset. The answer: they are there to demonstrate the wide-column model and
distributed processing, and the documentation states the actual scale.
