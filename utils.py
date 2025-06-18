import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import logging
import smtplib
from email.mime.text import MIMEText
import requests

load_dotenv()

def get_engine(env: str):
    key = f"DB_{env.upper()}_URL"
    
    try:
        db_url = os.getenv(key)
        if not db_url:
            raise ValueError(f"Environment variable '{key}' is not valid or not exist.")
        
        engine = create_engine(db_url)
        return engine
    
    except ValueError as ve:
        print(f"[ValueError] {ve}")
    
    except SQLAlchemyError as sae:
        print(f"[SQLAlchemyError] Failed to create engine: {sae}")
    
    except Exception as e:
        print(f"[Unexpected Error] {type(e).__name__}: {e}")
    
    return None


def send_alert(message: str):
    method = os.getenv("ALERT_METHOD", "log")

    if method == "log":
        logging.error(f"[ALERT] {message}")

    elif method == "email":
        try:
            sender = os.getenv("EMAIL_SENDER")
            recipient = os.getenv("EMAIL_RECIPIENT")
            smtp_server = os.getenv("SMTP_SERVER")
            smtp_port = int(os.getenv("SMTP_PORT", 587))
            smtp_user = os.getenv("SMTP_USER")
            smtp_password = os.getenv("SMTP_PASSWORD")

            msg = MIMEText(message)
            msg['Subject'] = 'ETL Pipeline Alert'
            msg['From'] = sender
            msg['To'] = recipient

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.sendmail(sender, [recipient], msg.as_string())
            logging.info("Email alert sent successfully.")
        except Exception as e:
            logging.exception("Failed to send email alert.")

    elif method == "slack":
        try:
            webhook_url = os.getenv("SLACK_WEBHOOK_URL")
            response = requests.post(webhook_url, json={"text": message})
            if response.status_code == 200:
                logging.info("Slack alert sent successfully.")
            else:
                logging.error(f"Slack alert failed with status {response.status_code}")
        except Exception as e:
            logging.exception("Failed to send Slack alert.")

