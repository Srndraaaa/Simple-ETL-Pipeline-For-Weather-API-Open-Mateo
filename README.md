# Simple ETL Open-Meteo + Docker

Project ini mengambil data cuaca dari Open-Meteo, memprosesnya, lalu menyimpan ke PostgreSQL.

Mode jalan ETL:
- Otomatis setiap 2 jam (default)
- Manual satu kali saat dibutuhkan

## Visualisasi

### Arsitektur

![Schema](Schema.png)

### Sequence ETL

![Sequence diagram](Sequence-diagram.png)

## Struktur Inti

- `src/main.py` untuk orkestrasi dan mode trigger
- `src/api_client.py` untuk extract API (retry + timeout)
- `src/transform.py` untuk normalisasi data
- `src/db.py` untuk schema dan upsert idempotent
- `docker-compose.yml` untuk service `etl` dan `postgres`

## Quick Start

1. Buat file env lokal:

```bash
cp .env.example .env
```

2. Jalankan stack:

```bash
docker compose up --build
```

## Menjalankan ETL

Jalankan manual satu kali:

```bash
docker compose run --rm etl python -m src.main --run-once
```

Cek koneksi database dari service ETL:

```bash
docker compose run --rm etl python -m src.main --healthcheck
```

## Cek Data

Masuk ke PostgreSQL:

```bash
docker compose exec postgres psql -U ${POSTGRES_USER:-postgres} -d ${POSTGRES_DB:-etl_db}
```

Query cepat:

```sql
SELECT observed_at, temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m
FROM weather_observations
ORDER BY observed_at DESC
LIMIT 20;
```

## Test

```bash
pytest
```
