from confluent_kafka import Consumer, Producer, KafkaError
import json
from services.schemas.credit_consumer import LoanApplication, CreditReport
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CreditConsumer:
    def __init__(self, bootstrap_servers: str, input_topic: str):
        self.input_topic = input_topic
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'credit_service_group',
            'auto.offset.reset': 'earliest'
        })
        
    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()}')

    def process_loan_application(self, loan_app: LoanApplication) -> CreditReport:
        # Calculate credit score
        score = CreditReport.calculate_score(
            loan_app.pan_number,
            loan_app.monthly_income_inr,
            loan_app.loan_type
        )
        
        
        return CreditReport(
            application_id=loan_app.application_id,
            pan_number=loan_app.pan_number,
            cibil_score=score,
        )

    def publish_credit_report(self, report: CreditReport):
        try:
            # Serialize the credit report
            message = json.dumps({
                "application_id": report.application_id,
                "cibil_score": report.cibil_score,
            })
            
            # Publish to credit reports topic
            self.producer.produce(
                'credit_reports_generated',
                value=message.encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.flush()
            
        except Exception as e:
            logger.error(f"Error publishing credit report: {e}")

    def run(self):
        try:
            self.consumer.subscribe([self.input_topic])
            logger.info(f"Subscribed to topic: {self.input_topic}")

            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info('Reached end of partition')
                    else:
                        logger.error(f'Error: {msg.error()}')
                    continue

                try:
                    # Parse incoming message
                    value = json.loads(msg.value().decode('utf-8'))
                    loan_app = LoanApplication(**value)
                    
                    # Process application
                    logger.info(f"Processing application {loan_app.application_id}")
                    credit_report = self.process_loan_application(loan_app)
                    print(f"Generated credit report: {credit_report}")
                    
                    # Publish credit report
                    self.publish_credit_report(credit_report)
                    logger.info(f"Published credit report for {loan_app.application_id}")
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except KeyboardInterrupt:
            logger.info("Shutting down credit consumer...")
        finally:
            self.consumer.close()
