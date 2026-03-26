import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from database import init_db, get_db
from models.customer import Customer
from services.ingestion import run_ingestion

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up: initialising database tables...")
    init_db()
    logger.info("Database tables ready.")
    yield
    logger.info("Shutting down pipeline service.")


app = FastAPI(
    title="Data Pipeline Service",
    description="Ingests customer data from Flask mock server into PostgreSQL.",
    version="1.0.0",
    lifespan=lifespan,
)

@app.get("/api/health")
def health():
    return {"status": "healthy", "service": "pipeline-service"}


@app.post("/api/ingest")
def ingest(db: Session = Depends(get_db)):
    try:
        result = run_ingestion(db)
        return JSONResponse(status_code=200, content=result)
    except Exception as e:
        logger.error(f"Ingest endpoint error: {e}")
        raise HTTPException(status_code=502, detail=f"Ingestion failed: {str(e)}")


@app.get("/api/customers")
def list_customers(
    page: int = Query(default=1, ge=1, description="Page number (1-indexed)"),
    limit: int = Query(default=10, ge=1, le=100, description="Records per page"),
    db: Session = Depends(get_db),
):
    total = db.query(Customer).count()
    offset = (page - 1) * limit
    customers = (
        db.query(Customer)
        .order_by(Customer.customer_id)
        .offset(offset)
        .limit(limit)
        .all()
    )

    return {
        "data": [c.to_dict() for c in customers],
        "total": total,
        "page": page,
        "limit": limit,
    }


@app.get("/api/customers/{customer_id}")
def get_customer(customer_id: str, db: Session = Depends(get_db)):
    customer = db.query(Customer).filter(Customer.customer_id == customer_id).first()
    if customer is None:
        raise HTTPException(status_code=404, detail="Customer not found")
    return {"data": customer.to_dict()}
