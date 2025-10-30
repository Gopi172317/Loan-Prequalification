from typing import Optional
from pydantic import BaseModel, Field, ValidationError, model_validator


class CreditReportMessage(BaseModel):
    """
    Schema for messages consumed from credit_reports_generated topic.
    Fields monthly_income_inr and loan_amount_inr are optional because the
    decision service will fetch them from the DB if missing.
    """
    application_id: str = Field(..., min_length=1)
    pan_number: Optional[str] = Field(None, min_length=1)
    cibil_score: int = Field(..., ge=300, le=900)
    monthly_income_inr: Optional[int] = Field(None, gt=0)
    loan_amount_inr: Optional[int] = Field(None, gt=0)
    applicant_name: Optional[str] = Field(None, min_length=1)
    loan_type: Optional[str] = Field(None, min_length=1)

    @model_validator(mode="before")
    def ensure_required(cls, values):
        # quick pre-validation to provide clearer error when application_id or cibil_score missing
        if "application_id" not in values or values.get("application_id") in (None, ""):
            raise ValueError("application_id is required")
        if "cibil_score" not in values or values.get("cibil_score") is None:
            raise ValueError("cibil_score is required")
        return values


class DecisionRecord(BaseModel):
    """
    Payload used when updating DB / for internal use after applying decision rules.
    """
    application_id: str
    status: str
    cibil_score: int

    @model_validator(mode="before")
    def validate_fields(cls, values):
        if "application_id" not in values or not values["application_id"]:
            raise ValueError("application_id is required")
        if "status" not in values or not values["status"]:
            raise ValueError("status is required")
        if "cibil_score" not in values:
            raise ValueError("cibil_score is required")
        return values