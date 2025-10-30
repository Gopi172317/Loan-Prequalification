from fastapi import FastAPI, HTTPException
from fastapi import Depends
from sqlalchemy.orm import Session
from api.schemas.applications import ApplicationRequest, ApplicationResponse
from db.database import get_db
from db.crud import db_create_application, get_application_by_id


app = FastAPI()


@app.get("/health-check/")
async def applications_testing():
    return {"applications": "Health Check OK"}


@app.post("/applications", status_code=202, response_model=ApplicationResponse)
async def create_application(payload: ApplicationRequest, db: Session = Depends(get_db)):
    db_app = db_create_application(db, payload)
    print(f"Created application with ID: {db_app.id}")
    return ApplicationResponse(application_id=str(db_app.id), status="PENDING")


@app.get("/applications/{application_id}/status", response_model=ApplicationResponse)
async def get_application_status(application_id: str, db: Session = Depends(get_db)):
    db_app = get_application_by_id(db, application_id)
    if not db_app:
        raise HTTPException(status_code=404, detail="Application not found")
    return ApplicationResponse(application_id=str(db_app.id), status=db_app.status)
