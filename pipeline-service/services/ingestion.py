import os
import logging
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict, Any

import httpx
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models.customer import Customer

logger = logging.getLogger(__name__)

FLASK_BASE_URL = os.getenv("FLASK_BASE_URL", "http://mock-server:5000")
FETCH_LIMIT = int(os.getenv("FETCH_LIMIT", "50"))


def _parse_date(value: Any):
    if not value:
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _parse_timestamp(value: Any):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except (ValueError, TypeError):
            continue
    return None


def _parse_decimal(value: Any):
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def fetch_all_customers_from_flask() -> List[Dict[str, Any]]:
    all_customers = []
    page = 1

    with httpx.Client(timeout=30.0) as client:
        while True:
            url = f"{FLASK_BASE_URL}/api/customers"
            params = {"page": page, "limit": FETCH_LIMIT}

            logger.info(f"Fetching page {page} from Flask API...")
            response = client.get(url, params=params)
            response.raise_for_status()

            payload = response.json()
            records = payload.get("data", [])
            total = payload.get("total", 0)

            all_customers.extend(records)
            logger.info(f"Fetched {len(records)} records (total collected: {len(all_customers)}/{total})")

            if len(all_customers) >= total or not records:
                break

            page += 1

    logger.info(f"Finished fetching. Total records: {len(all_customers)}")
    return all_customers


def upsert_customers(db: Session, records: List[Dict[str, Any]]) -> int:
    if not records:
        return 0

    rows = []
    for rec in records:
        rows.append({
            "customer_id": rec.get("customer_id"),
            "first_name": rec.get("first_name"),
            "last_name": rec.get("last_name"),
            "email": rec.get("email"),
            "phone": rec.get("phone"),
            "address": rec.get("address"),
            "date_of_birth": _parse_date(rec.get("date_of_birth")),
            "account_balance": _parse_decimal(rec.get("account_balance")),
            "created_at": _parse_timestamp(rec.get("created_at")),
        })

    stmt = pg_insert(Customer).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["customer_id"],
        set_={
            "first_name": stmt.excluded.first_name,
            "last_name": stmt.excluded.last_name,
            "email": stmt.excluded.email,
            "phone": stmt.excluded.phone,
            "address": stmt.excluded.address,
            "date_of_birth": stmt.excluded.date_of_birth,
            "account_balance": stmt.excluded.account_balance,
            "created_at": stmt.excluded.created_at,
        },
    )

    db.execute(stmt)
    db.commit()
    logger.info(f"Upserted {len(rows)} records into PostgreSQL.")
    return len(rows)


def run_ingestion(db: Session) -> Dict[str, Any]:
    try:
        records = fetch_all_customers_from_flask()
        count = upsert_customers(db, records)
        return {"status": "success", "records_processed": count}
    except httpx.HTTPError as e:
        logger.error(f"HTTP error while fetching from Flask: {e}")
        raise
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        db.rollback()
        raise
