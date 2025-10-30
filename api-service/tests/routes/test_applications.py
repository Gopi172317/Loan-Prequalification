from fastapi.testclient import TestClient
from api.routes.applications import app
from db.database import Base, engine, get_db
from unittest.mock import patch
import pytest
from types import SimpleNamespace
from api.routes import applications as applications_module
from api.routes.applications import app

client = TestClient(app)

@pytest.fixture
def db_session():
    # Create a new database session for tests
    db = next(get_db())
    try:
        yield db
    finally:
        db.close()

@pytest.fixture
def test_db():
    # Create test database tables
    Base.metadata.create_all(bind=engine)
    try:
        yield
    finally:
        # Drop test database tables
        Base.metadata.drop_all(bind=engine)


# Create Application Tests
def test_create_application_success():
    """Test successful creation of a loan application"""
    
    # Test payload
    test_payload = {
        "pan_number": "ABCDE1234F",
        "applicant_name": "John Doe",
        "monthly_income_inr": 50000,
        "loan_amount_inr": 200000,
        "loan_type": "PERSONAL"
    }

    # Make POST request to create application
    response = client.post("/applications", json=test_payload)

    # Assert response status code is 202
    assert response.status_code == 202
    
    # Assert response structure
    data = response.json()
    assert "application_id" in data
    assert "status" in data
    assert data["status"] == "PENDING"
    
    # Verify application_id is a valid string
    assert isinstance(data["application_id"], str)
    assert len(data["application_id"]) > 0


def test_create_application_invalid_data():
    """Test application creation with invalid data"""
    
    # Test payload with missing required fields
    invalid_payload = {
        "pan_number": "ABCDE1234F",
        # missing applicant_name
        "monthly_income_inr": 50000,
        "loan_type": "PERSONAL"
    }

    response = client.post("/applications", json=invalid_payload)
    assert response.status_code == 422  # Validation error


def test_create_application_invalid_loan_type():
    """Test application creation with invalid loan type"""
    
    test_payload = {
        "pan_number": "ABCDE1234F",
        "applicant_name": "John Doe",
        "monthly_income_inr": 50000,
        "loan_amount_inr": 200000,
        "loan_type": "INVALID_TYPE"  # Invalid loan type
    }

    response = client.post("/applications", json=test_payload)
    assert response.status_code == 422  # Validation error


# Get Application Status Tests
def _fake_app(app_id: str, status: str):
    return SimpleNamespace(id=app_id, status=status)


def test_get_application_status_pending(monkeypatch):
    """
    AC 2.1: GIVEN application app-123 has status PENDING
    WHEN GET /applications/app-123/status
    THEN response 200 with the pending status
    """
    app_id = "app-123"
    monkeypatch.setattr(
        applications_module,
        "get_application_by_id",
        lambda db, application_id: _fake_app(app_id, "PENDING") if application_id == app_id else None
    )

    resp = client.get(f"/applications/{app_id}/status")
    assert resp.status_code == 200
    assert resp.json() == {"application_id": app_id, "status": "PENDING"}


def test_get_application_status_pre_approved(monkeypatch):
    """
    AC 2.2: GIVEN application app-123 has status PRE_APPROVED
    WHEN GET /applications/app-123/status
    THEN response 200 with the pre-approved status
    """
    app_id = "app-123"
    monkeypatch.setattr(
        applications_module,
        "get_application_by_id",
        lambda db, application_id: _fake_app(app_id, "PRE_APPROVED") if application_id == app_id else None
    )

    resp = client.get(f"/applications/{app_id}/status")
    assert resp.status_code == 200
    assert resp.json() == {"application_id": app_id, "status": "PRE_APPROVED"}
    