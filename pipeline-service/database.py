import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.customer import Base

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@postgres:5432/customer_db")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=5, max_overflow=10)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
