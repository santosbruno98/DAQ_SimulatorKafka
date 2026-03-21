# DAQ-Simulator (Kafka)

A real-time **Distributed Acoustic Sensing (DAS) simulator** built on FastAPI and Apache Kafka. It ingests compressed fiber-optic sensor data, streams it through a multi-stage Kafka pipeline (correlation → conversion → storage), and exposes a RESTful API for management and cloud storage operations.

---

## Table of Contents

- [Introduction](#introduction)
  - [Purpose](#purpose)
  - [Features](#features)
  - [Architecture Overview](#architecture-overview)
- [Dependencies](#dependencies)
- [Installation](#installation)
- [Usage](#usage)
  - [Running with Docker Compose](#running-with-docker-compose)
  - [Running Services Individually](#running-services-individually)
  - [Accessing the Interfaces](#accessing-the-interfaces)
- [Configuration](#configuration)
- [Testing](#testing)
- [CI/CD](#cicd)
- [More Features](#more-features)

---

## Introduction

### Purpose

This project simulates the data acquisition pipeline of a fiber-optic DAS system. Sensor readings are read from compressed JSON files, serialized into NumPy arrays, and published to Kafka. Three downstream consumer services — **Correlation**, **Conversion**, and **Writer** — form a streaming pipeline that processes raw data into temperature/humidity measurements and persists results to MongoDB and AWS S3.

It is also a complete FastAPI backend with user management, JWT authentication, rate limiting, an admin UI, and background task workers.

### Features

- **DAQ Simulation**: Reads and decompresses base64-encoded sensor data and streams it continuously to Kafka.
- **Streaming Pipeline**:
  - **Correlation Service** — consumes raw electrical data, applies frequency-axis calculations and peak-finding algorithms, and publishes correlation results.
  - **Conversion Service** — joins correlation and raw data via Redis (by `sweeps_id`), computes temperature and humidity using Numba-JIT math functions, and publishes conversion results.
  - **Writer Service** — consumes all three topics and writes documents to MongoDB and uploads gzip-compressed files to AWS S3.
- **RESTful API** — user CRUD, authentication (JWT), posts, background tasks, and S3 bucket operations.
- **Admin Dashboard** — CRUD interface for all database tables.
- **Background Task Worker** — async job queue via `arq` + Redis.
- **Kafka Cluster** — 2-broker cluster with 50 partitions per topic and 50 MB message support.
- **Full Containerization** — all services orchestrated via Docker Compose.

### Architecture Overview

```
JSON Data Files (compressed sensor readings)
        │
        ▼
 Kafka Producer  ──────────────────────────────────────────────►  raw-electrical-data
                                                                           │
                                                              ┌────────────┴────────────┐
                                                              ▼                         ▼
                                                   Correlation Service          Writer Service
                                                   (frequency shift,            (MongoDB + S3)
                                                    peak finding)
                                                              │
                                                   correlation-data topic
                                                              │
                                                              ▼
                                                   Conversion Service
                                                   (Redis join, temp/humidity calc)
                                                              │
                                                   conversion-data topic
                                                              │
                                                              ▼
                                                   Writer Service
                                                   (MongoDB + S3)
```

**Kafka Topics:**

| Topic                  | Partitions | Replication | Max Message |
|------------------------|------------|-------------|-------------|
| `raw-electrical-data`  | 50         | 2           | 50 MB       |
| `correlation-data`     | 50         | 2           | 50 MB       |
| `conversion-data`      | 50         | 2           | 50 MB       |

---

## Dependencies

### Runtime Stack

| Category          | Technology                              |
|-------------------|-----------------------------------------|
| Framework         | FastAPI, Uvicorn, Uvloop                |
| Databases         | PostgreSQL 13, MongoDB, Redis           |
| Message Broker    | Apache Kafka (2-broker cluster)         |
| Cloud Storage     | AWS S3 (boto3)                          |
| Data Processing   | NumPy, Numba (JIT)                      |
| Task Queue        | arq (async Redis queue)                 |
| ORM               | SQLAlchemy (async), asyncpg             |
| Auth              | JWT (python-jose), bcrypt               |
| Validation        | Pydantic v2                             |
| Package Manager   | uv                                      |

### Service Graph (Docker Compose)

```
web ──────────────────┐
worker ───────────────┼──► db (PostgreSQL)
create_superuser ─────┘    redis
                           zookeeper
                           kafka1 (port 9092)
                           kafka2 (port 9093)
                           kafka-ui (port 8080)
```

---

## Installation

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/)
- AWS credentials with S3 access (if using cloud storage)
- A reachable MongoDB instance (URI configured via `.env`)

### 1. Clone the repository

```bash
git clone https://github.com/FiberSight/DAQ-simulator.git
cd DAQ-simulator
```

### 2. Configure environment variables

Create a `.env` file at `src/.env` (copy the example and fill in your values):

```dotenv
# PostgreSQL
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_SERVER=db
POSTGRES_PORT=5432
POSTGRES_DB=daq_db

# MongoDB
MONGODB_CONNECTION_STRING=mongodb://user:password@host:27017
DB_NAME=daq_simulator

# Redis
REDIS_CACHE_HOST=redis
REDIS_CACHE_PORT=6379
REDIS_QUEUE_HOST=redis
REDIS_QUEUE_PORT=6379
REDIS_RATE_LIMIT_HOST=redis
REDIS_RATE_LIMIT_PORT=6379

# AWS S3
AWS_BUCKET_NAME=your-bucket-name

# Security
SECRET_KEY=your_secret_key
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
REFRESH_TOKEN_EXPIRE_DAYS=7
ADMIN_EMAIL=admin@example.com
ADMIN_PASSWORD=admin_password

# App
APP_NAME=DAQ-Simulator
ENVIRONMENT=local
CRUD_ADMIN_ENABLED=true
```

### 3. (Optional) Local development without Docker

```bash
# Install uv
pip install uv

# Create virtual environment and install dependencies
uv venv
uv pip install -e ".[dev]"
```

---

## Usage

### Running with Docker Compose

```bash
docker-compose up -d
```

This starts all services automatically. The `create_superuser` service creates the initial admin account on first run.

To follow logs for the DAQ pipeline:

```bash
docker-compose logs -f web worker
```

### Running Services Individually

Each pipeline service runs as a standalone Python module (activate your venv first):

```bash
# DAQ data producer — reads JSON files and streams to Kafka
python -m app.core.worker.daq.kafka_producer

# Correlation service — consumes raw data, computes frequency shifts
python -m app.core.worker.services.correlation_service

# Conversion service — joins data via Redis, computes temperature/humidity
python -m app.core.worker.services.conversion_service

# Writer service — persists all data to MongoDB and S3
python -m app.core.worker.services.writer_service

# Background task worker (arq)
arq app.core.worker.settings.WorkerSettings
```

On Windows you can use the provided batch script to open all services in separate terminals:

```cmd
run_services.bat
```

### Accessing the Interfaces

| Interface        | URL                          |
|------------------|------------------------------|
| API (Swagger UI) | http://localhost:8000/docs   |
| API (ReDoc)      | http://localhost:8000/redoc  |
| Admin Dashboard  | http://localhost:8000/admin  |
| Kafka UI         | http://localhost:8080        |

### Key API Endpoints

| Method | Endpoint                          | Description              |
|--------|-----------------------------------|--------------------------|
| POST   | `/api/v1/login`                   | Authenticate, get JWT    |
| POST   | `/api/v1/users`                   | Create user              |
| GET    | `/api/v1/users/{user_id}`         | Get user                 |
| POST   | `/api/v1/tasks/task`              | Submit background task   |
| GET    | `/api/v1/tasks/task/{task_id}`    | Poll task status         |
| POST   | `/api/{bucket_name}`              | Upload file to S3        |
| GET    | `/api/{bucket_name}/size`         | Get S3 bucket size       |

---

## Configuration

### Kafka Message Format

Each Kafka message carries:
- **Value**: zlib-compressed pickle of a NumPy array
- **Headers**:
  - `type` — `"raw"` or `"elec"`
  - `sweeps_id` — BSON ObjectId string used for cross-service data joining via Redis

### Environment Modes

Set `ENVIRONMENT` in `.env` to one of:

| Value        | Behavior                                  |
|--------------|-------------------------------------------|
| `local`      | Debug logging, hot-reload, relaxed CORS   |
| `staging`    | Moderate logging, partial restrictions    |
| `production` | Minimal logging, strict security settings |

### Conversion Service — Redis TTL

The conversion service stores raw data in Redis for cross-service joining. Default TTL is **300 seconds** before a pending sweeps entry expires. Adjust in `conversion_service.py` if your pipeline latency is higher.

---

## Testing

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=src

# Run a specific test file
pytest tests/test_user.py

# Via uv
uv run pytest
```

---

## CI/CD

The repository ships three GitHub Actions workflows that run on every push and pull request:

| Workflow            | File                          | What it does                                      |
|---------------------|-------------------------------|---------------------------------------------------|
| **Tests**           | `.github/workflows/tests.yml` | Runs the full `pytest` suite on Python 3.11       |
| **Linting**         | `.github/workflows/linting.yml` | Checks code style with `ruff`                   |
| **Type Checking**   | `.github/workflows/type-checking.yml` | Validates static types with `mypy`        |

All three checks must pass before a pull request can be merged.

### Running checks locally

```bash
# Linting
ruff check src/

# Auto-fix lint issues
ruff check --fix src/

# Type checking
mypy src/

# Full test suite
uv run pytest
```

### Pre-commit hooks

A `.pre-commit-config.yaml` is included so the same checks run automatically before each commit:

```bash
# Install hooks (one-time)
pre-commit install

# Run manually against all files
pre-commit run --all-files
```

---

## More Features

### Admin Dashboard

Available at `/admin` when `CRUD_ADMIN_ENABLED=true`. Provides CRUD management for all PostgreSQL tables (users, posts, tiers, rate limits) with session-based authentication and optional Redis-backed distributed sessions.

### Rate Limiting

Per-user rate limits backed by Redis. Configure via `POST /api/v1/rate-limits`.

### Caching

- **Redis client-side cache** with configurable TTL (default 5 minutes)
- **HTTP cache headers** via middleware (`Cache-Control: max-age`)

### Database Migrations

Alembic migrations are applied automatically on application startup. To create a new migration:

```bash
alembic revision --autogenerate -m "description"
alembic upgrade head
```

### MongoDB Collections

| Collection                    | Contents                               |
|-------------------------------|----------------------------------------|
| `laser_metadata`              | Laser and sensor configuration         |
| `acquisition_characteristics` | Sensor acquisition parameters          |
| `electrical_data`             | Raw electrical readings                |
| `correlation_data`            | Frequency shift and correlation output |
| `conversion_data`             | Temperature and humidity results       |
| `trace_data`                  | Raw fiber trace data                   |

### Performance Notes

- **Numba JIT** compiles math-heavy functions (`moving_correlation_with_peak_finding`, `temperature_humidity_calculation`) on first call — expect a warm-up delay on the first batch.
- **ProcessPoolExecutor** handles CPU-bound computations off the async event loop.
- **ThreadPoolExecutor** handles S3 uploads without blocking Kafka consumers.
- **Uvloop** replaces the default asyncio event loop for higher throughput.
