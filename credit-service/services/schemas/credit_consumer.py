from pydantic import BaseModel
from enum import Enum
import random

class LoanType(str, Enum):
    PERSONAL = "PERSONAL"
    HOME = "HOME"
    AUTO = "AUTO"

class LoanApplication(BaseModel):
    application_id: str
    pan_number: str
    applicant_name: str
    monthly_income_inr: int
    loan_amount_inr: int
    loan_type: LoanType

class CreditReport(BaseModel):
    application_id: str
    pan_number: str
    cibil_score: int

    @staticmethod
    def calculate_score(pan_number: str, monthly_income: int, loan_type: LoanType) -> int:
        # Configuration parameters
        scoring_config = {
            'base_score': 650,
            'loan_type_adjustments': {
                LoanType.PERSONAL: -10,  # Unsecured penalty
                LoanType.HOME: 10,       # Secured bonus
                LoanType.AUTO: 5         # Partially secured
            },
            'random_range': (-5, 5),
            'score_limits': (300, 900),
        }

        score = scoring_config['base_score']
        
        if monthly_income > 75000:
            score += 40
        elif monthly_income < 30000:
            score -= 20

        print("Score: ",score)
        
        # Loan type adjustments
        score += scoring_config['loan_type_adjustments'].get(loan_type, 0)
            
        # Add randomness
        score += random.randint(*scoring_config['random_range'])
        
        # Cap between min and max limits
        return max(scoring_config['score_limits'][0], 
                  min(scoring_config['score_limits'][1], score))
