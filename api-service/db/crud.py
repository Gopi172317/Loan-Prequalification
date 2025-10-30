from sqlalchemy.orm import Session
from decimal import Decimal
import uuid

from .models import Application
from api.schemas.applications import ApplicationRequest


def db_create_application(db: Session, app_req: ApplicationRequest) -> Application:
    """
    Persist an application row and return the created model instance.
    Commits the transaction and refreshes the instance so created_at/updated_at are populated.
    """
    db_obj = Application(
        id=str(uuid.uuid4()),
        pan_number=app_req.pan_number,
        applicant_name=app_req.applicant_name,
        monthly_income_inr=Decimal(app_req.monthly_income_inr),
        loan_amount_inr=Decimal(app_req.loan_amount_inr),
        loan_type=app_req.loan_type.value if hasattr(app_req.loan_type, "value") else str(app_req.loan_type),
        status="PENDING",
    )
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)
    return db_obj

def get_application_by_id(db: Session, application_id: str) -> Application:
    """
    Retrieve an application by its ID.
    Returns None if not found.
    """
    return db.query(Application).filter(Application.id == application_id).first()
