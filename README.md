# Customer Data Pipeline

A three-service Docker pipeline: **Flask mock server → FastAPI ingest → PostgreSQL**.

```
project-root/
├── docker-compose.yml
├── README.md
├── mock-server/
│   ├── app.py
│   ├── data/customers.json
│   ├── Dockerfile
│   └── requirements.txt
└── pipeline-service/
    ├── main.py
    ├── models/
    │   ├── __init__.py
    │   └── customer.py
    ├── services/
    │   ├── __init__.py
    │   └── ingestion.py
    ├── database.py
    ├── Dockerfile
    └── requirements.txt
```

---

## Quick Start

```bash
# Build and start all services
docker-compose up -d --build

# Watch logs
docker-compose logs -f
```

Wait ~20 seconds for all health checks to pass before testing.

---

## Service Reference

| Service | Port | Description |
|---|---|---|
| Flask Mock Server | 5000 | Serves 22 customers from JSON |
| FastAPI Pipeline | 8000 | Ingestion + query API |
| PostgreSQL | 5432 | Persistent storage |

---

## Testing

### Flask Mock Server

```bash
# Health check
curl http://localhost:5000/api/health

# Paginated customers
curl "http://localhost:5000/api/customers?page=1&limit=5"

# Single customer
curl http://localhost:5000/api/customers/CUST-001

# 404 example
curl http://localhost:5000/api/customers/CUST-999
```

### FastAPI Pipeline

```bash
# Ingest all data from Flask into PostgreSQL
curl -X POST http://localhost:8000/api/ingest

# Paginated customers (from DB)
curl "http://localhost:8000/api/customers?page=1&limit=5"

# Single customer (from DB)
curl http://localhost:8000/api/customers/CUST-001

# FastAPI interactive docs
open http://localhost:8000/docs
```

---

## Flow

```
Flask /api/customers (paginated JSON)
        │
        ▼  POST /api/ingest
FastAPI ingestion service
  └─ fetches all pages from Flask
  └─ upserts rows via SQLAlchemy (ON CONFLICT DO UPDATE)
        │
        ▼
PostgreSQL  customer_db.customers
        │
        ▼  GET /api/customers  |  GET /api/customers/{id}
FastAPI query endpoints return JSON to caller
```

---

## Environment Variables (pipeline-service)

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql://postgres:password@postgres:5432/customer_db` | PostgreSQL DSN |
| `FLASK_BASE_URL` | `http://mock-server:5000` | Flask service base URL |
| `FETCH_LIMIT` | `50` | Records per page when fetching from Flask |

---

## Teardown

```bash
# Stop all services
docker-compose down

# Remove volumes (deletes DB data)
docker-compose down -v
```
