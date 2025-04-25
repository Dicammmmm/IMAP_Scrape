import os
import imaplib
import email
import dotenv
import psycopg2
import logging
from email.header import decode_header

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Load environment variables
dotenv.load_dotenv(override=True)

# DB & IMAP credentials
__DB_USER = os.getenv("DB_USER")
__DB_PASSWORD = os.getenv("DB_PASSWORD")
__DB_HOST = os.getenv("DB_HOST")
__EMAIL = os.getenv("EMAIL")
__PASSWORD = os.getenv("PASSWORD")
__IMAP_SERVER = os.getenv("IMAP_SERVER")

# ---------- Utils ----------

def decode_header_safe(header) -> str:
    """Decode email headers safely"""

    decoded = decode_header(header)
    result = ''

    for part, encoding in decoded:
        if isinstance(part, bytes):
            result += part.decode(encoding or 'utf-8', errors='ignore')

        else:
            result += part

    return result

# ---------- Connections ----------

def test_connection() -> bool:
    """Test database and IMAP connections"""

    db_ok = False
    imap_ok = False

    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=__DB_USER,
            password=__DB_PASSWORD,
            host=__DB_HOST
        )
        conn.close()
        db_ok = True
        log.info("Database connection successful")

    except Exception as e:
        raise Exception(f"Database connection failed: {e}")
    
    try:
        imap_conn = imaplib.IMAP4_SSL(__IMAP_SERVER)
        response, _ = imap_conn.login(__EMAIL, __PASSWORD)
        if response == 'OK':
            imap_conn.logout()
            imap_ok = True
            log.info("IMAP connection successful")

    except Exception as e:
        raise Exception(f"IMAP connection failed: {e}")

    return db_ok and imap_ok

# ---------- Email Processing ----------

def fetch_emails() -> list:
    """Fetch approved sender emails from the database"""

    try:
        conn = psycopg2.connect(
            dbname="inbox",
            user=__DB_USER,
            password=__DB_PASSWORD,
            host=__DB_HOST
        )

        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT email FROM emails")
        emails = [row[0].strip().lower() for row in cursor.fetchall()]

        cursor.close()
        conn.close()

        log.debug(f"Fetched approved sender list: {emails}")

        return emails
    
    except Exception as e:
        log.error(f"Error fetching emails from database: {e}")
        return []

def process_inbox():
    """Process the inbox and fetch emails from approved senders"""

    approved_senders = fetch_emails()
    if not approved_senders:
        log.info("No approved sender emails found")
        return []

    imap_conn = imaplib.IMAP4_SSL(__IMAP_SERVER)
    imap_conn.login(__EMAIL, __PASSWORD)
    imap_conn.select("inbox")

    status, messages = imap_conn.search(None, "ALL")
    if status != "OK":
        log.error("Failed to fetch emails")
        imap_conn.logout()
        return []

    results = []

    for num in messages[0].split():
        status, data = imap_conn.fetch(num, "(RFC822)")

        if status != "OK":
            log.error(f"Failed to fetch email {num}")
            continue

        raw_email = data[0][1]
        msg = email.message_from_bytes(raw_email)

        sender = email.utils.parseaddr(msg.get("From", ""))[1].strip().lower()
        log.debug(f"Parsed sender: {sender}")

        if sender not in approved_senders:
            log.debug(f"Sender {sender} not in approved list")
            continue

        msg_id = msg.get("Message-ID", "").strip()
        subject = decode_header_safe(msg.get("Subject", ""))
        attachments = []

        for part in msg.walk():
            if part.get_content_disposition() == "attachment":
                filename = part.get_filename()

                if filename:
                    filename = decode_header_safe(filename)
                    attachments.append(filename)

        if msg_id:
            results.append((msg_id, subject, attachments, sender))

    imap_conn.logout()
    return results

# ---------- Save to DB ----------

def save_to_db(msg_id, subject, attachments, sender):
    """Save email details to the database"""

    try:
        conn = psycopg2.connect(
            dbname="inbox",
            user=__DB_USER,
            password=__DB_PASSWORD,
            host=__DB_HOST
        )

        attachment_str = ', '.join(attachments) # Better format for the database

        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO inbox (message_id, sender, attachment, scraped, subject)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (msg_id, sender, attachment_str, False, subject)
        )

        conn.commit()
        cursor.close()
        conn.close()

        log.info(f"Saved email from {sender} to database")

    except Exception as e:
        log.error(f"Error saving email to database: {e}")

# ---------- Main ----------

def main():
    try:
        if test_connection():
            log.info("All connections are good")
            emails = process_inbox()

            if emails:
                for msg_id, subject, attachments, sender in emails:
                    save_to_db(msg_id, subject, attachments, sender)
                log.info(f"{len(emails)} emails processed and saved to database")

            else:
                log.info("No matching emails to process")

        else:
            log.error("Connection test failed")
            
    except Exception as e:
        log.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
