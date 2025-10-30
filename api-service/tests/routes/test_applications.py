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

def _fake_get_by_id(app_id: str = "db3a988f-e07d-434f-b2b6-0961c7ba8c73", status: str = "PRE_APPROVED"):
    return SimpleNamespace(id=app_id, status=status)


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
def test_get_application_status_success(monkeypatch):
    app_id = "db3a988f-e07d-434f-b2b6-0961c7ba8c73"
    monkeypatch.setattr(
        applications_module,
        "get_application_by_id",
        lambda db, application_id: _fake_get_by_id(app_id, "PRE_APPROVED") if application_id == app_id else None
    )

    resp = client.get(f"/applications/{app_id}/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["application_id"] == app_id
    assert body["status"] == "PRE_APPROVED"


def test_get_application_status_not_found(monkeypatch):
    monkeypatch.setattr(
        applications_module,
        "get_application_by_id",
        lambda db, application_id: None
    )

    resp = client.get("/applications/non-existent-id/status")
    assert resp.status_code == 404
    body = resp.json()
    assert body.get("detail") == "Application not found"