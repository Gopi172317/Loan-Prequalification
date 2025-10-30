from sqlalchemy import Column, String, Numeric, Integer, DateTime
from sqlalchemy.sql import func
from .database import Base
import uuid


class Application(Base):
    __tablename__ = "applications"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    pan_number = Column(String(10), nullable=False)
    applicant_name = Column(String(255))
    monthly_income_inr = Column(Numeric(12, 2), nullable=False)
    loan_amount_inr = Column(Numeric(12, 2), nullable=False)
    loan_type = Column(String(20))
    status = Column(String(20), nullable=False, default="PENDING")
    cibil_score = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    