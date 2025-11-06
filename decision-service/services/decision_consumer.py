import json
import logging
from typing import Optional, Tuple
from confluent_kafka import Consumer, KafkaError
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DecisionConsumer:
    def __init__(self, bootstrap_servers: str, input_topic: str, database_url: str):
        self.input_topic = input_topic
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": "decision_service_group",
                "auto.offset.reset": "earliest",
            }
        )

        # SQLAlchemy engine + ORM session factory
        self.engine = create_engine(database_url, echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False)

        # Reflect existing database and map the applications table to an ORM class
        Base = automap_base()
        # reflect existing tables
        Base.prepare(self.engine, reflect=True)
        # mapped class for applications table (guard against missing table)
        try:
            self.Application = getattr(Base.classes, "applications")
        except Exception:
            logger.warning(
                "Could not automap 'applications' table - ensure DB has the table. "
                "DecisionConsumer will still run but DB calls may fail."
            )
            self.Application = None

    def _decide_status(
        self, cibil_score: int, monthly_income: float, loan_amount: float
    ) -> str:
        """
        Decision rules:
        - If cibil_score < 650 -> REJECTED
        - If cibil_score >= 650 and monthly_income > loan_amount/48 -> PRE_APPROVED
        - Else -> MANUAL_REVIEW
        """
        if cibil_score < 650:
            return "REJECTED"
        if monthly_income > (loan_amount / 48.0):
            return "PRE_APPROVED"
        return "MANUAL_REVIEW"

    def _fetch_app_financials(
        self, application_id: str
    ) -> Optional[Tuple[float, float]]:
        """
        Fetch monthly_income_inr and loan_amount_inr from applications table via ORM.
        Returns tuple(monthly_income, loan_amount) or None if not found / mapping unavailable.
        """
        if self.Application is None:
            logger.error("ORM mapping for applications not available")
            return None

        try:
            with self.SessionLocal() as session:
                app = (
                    session.query(self.Application).filter_by(id=application_id).first()
                )
                if app is None:
                    return None
                monthly_income = (
                    float(app.monthly_income_inr)
                    if getattr(app, "monthly_income_inr", None) is not None
                    else 0.0
                )
                loan_amount = (
                    float(app.loan_amount_inr)
                    if getattr(app, "loan_amount_inr", None) is not None
                    else 0.0
                )
                return monthly_income, loan_amount
        except Exception as exc:
            logger.exception(
                "Error fetching financials for %s: %s", application_id, exc
            )
            return None

    def _update_application(self, application_id: str, status: str, cibil_score: int):
        """
        Update the applications table row with new status and cibil_score using ORM.
        """
        if self.Application is None:
            logger.error(
                "ORM mapping for applications not available - cannot update DB"
            )
            return

        try:
            with self.SessionLocal() as session:
                app = (
                    session.query(self.Application).filter_by(id=application_id).first()
                )
                if app is None:
                    logger.error(
                        "Attempted to update non-existent application %s",
                        application_id,
                    )
                    return
                setattr(app, "status", status)
                setattr(app, "cibil_score", int(cibil_score))
                try:
                    setattr(app, "updated_at", func.now())
                except Exception:
                    pass
                session.commit()
        except Exception as exc:
            logger.exception("Failed to update application %s: %s", application_id, exc)

    def run(self):
        """
        Main loop: consume credit reports, decide, and persist decision.
        Robust against missing fields and malformed messages.
        """
        try:
            self.consumer.subscribe([self.input_topic])
            logger.info("Subscribed to topic: %s", self.input_topic)

            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue

                err = msg.error()
                if err:
                    # ignore partition EOF
                    try:
                        if err.code() == KafkaError._PARTITION_EOF:
                            logger.debug("Reached end of partition")
                            continue
                    except Exception:
                        # err may not implement code()
                        pass
                    logger.error("Consumer error: %s", err)
                    continue

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except Exception as exc:
                    logger.exception("Failed to decode message value: %s", exc)
                    continue

                application_id = payload.get("application_id")
                if not application_id:
                    logger.error(
                        "Received credit report without application_id; skipping"
                    )
                    continue

                cibil_raw = payload.get("cibil_score")
                if cibil_raw is None:
                    logger.error(
                        "Received credit report without cibil_score for %s; skipping",
                        application_id,
                    )
                    continue
                try:
                    cibil_score = int(cibil_raw)
                except Exception:
                    logger.exception(
                        "Invalid cibil_score for %s: %s", application_id, cibil_raw
                    )
                    continue

                monthly_income = payload.get("monthly_income_inr")
                loan_amount = payload.get("loan_amount_inr")

                if monthly_income is None or loan_amount is None:
                    fin = self._fetch_app_financials(application_id)
                    if fin is None:
                        logger.error(
                            "Application %s not found or missing financials; skipping",
                            application_id,
                        )
                        continue
                    monthly_income, loan_amount = fin

                try:
                    monthly_income = float(monthly_income)
                    loan_amount = float(loan_amount)
                except Exception:
                    logger.exception(
                        "Invalid financial values for %s: income=%s loan=%s",
                        application_id,
                        monthly_income,
                        loan_amount,
                    )
                    continue

                status = self._decide_status(cibil_score, monthly_income, loan_amount)
                logger.info(
                    "Decision for %s: status=%s, cibil_score=%s",
                    application_id,
                    status,
                    cibil_score,
                )

                self._update_application(application_id, status, cibil_score)
                logger.info("Updated application %s in DB", application_id)

        except KeyboardInterrupt:
            logger.info("Shutting down decision consumer...")
        finally:
            try:
                self.consumer.close()
            except Exception:
                pass
