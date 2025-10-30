from pydantic import BaseModel, Field, model_validator
from enum import Enum


class LoanType(str, Enum):
    PERSONAL = "PERSONAL"
    HOME = "HOME"
    AUTO = "AUTO"

class ApplicationRequest(BaseModel):
    pan_number: str = Field(..., min_length=1)
    applicant_name: str = Field(..., min_length=1)
    monthly_income_inr: int = Field(..., gt=0)
    loan_amount_inr: int = Field(..., gt=0)
    loan_type: LoanType

    @model_validator(mode="before")
    def check_required_fields(cls, values):
        """
        Ensure required keys are present and not empty.
        This provides a clearer single error message when required fields are missing.
        """
        required = ["pan_number", "applicant_name", "monthly_income_inr", "loan_amount_inr", "loan_type"]
        missing = []
        for key in required:
            if key not in values or values.get(key) is None:
                missing.append(key)
            elif isinstance(values.get(key), str) and values.get(key).strip() == "":
                missing.append(key)
        if missing:
            # raising ValueError produces a validation error surfaced as 422 by FastAPI
            raise ValueError(f"Missing or empty required field(s): {', '.join(missing)}")
        return values

class ApplicationResponse(BaseModel):
    application_id: str
    status: str
    