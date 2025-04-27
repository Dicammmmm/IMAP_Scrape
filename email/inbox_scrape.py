import os
import imaplib
import email
import dotenv
import logging
import pandas as pd
import io
import re
import hashlib
from sqlalchemy import create_engine, MetaData, Table, Column, inspect, text
from sqlalchemy.types import String, Integer, Float, DateTime, BigInteger, Text, Boolean
from email.header import decode_header
from email.utils import parseaddr
import numpy as np

# --- Basic Configuration & Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)
dotenv.load_dotenv(override=True)

# --- Environment Variables & Validation ---
__DB_USER = os.getenv("DB_USER")
__DB_PASSWORD = os.getenv("DB_PASSWORD")
__DB_HOST = os.getenv("DB_HOST")
__DB_NAME = os.getenv("DB_NAME")

__IMAP_SERVER = os.getenv("IMAP_SERVER")
__EMAIL = os.getenv("EMAIL")
__PASSWORD = os.getenv("PASSWORD")

if not all([__DB_USER, __DB_PASSWORD, __DB_HOST, __DB_NAME, __EMAIL, __PASSWORD, __IMAP_SERVER]):
    log.critical("Missing critical environment variables. Exiting.")
    exit(1)

# ---------- Utils ----------

def decode_header_safe(header) -> str:
    """Safely decodes email headers."""

    if header is None: return ""
    try:
        decoded = decode_header(header)
        result = ''
        for part, encoding in decoded:
            try:
                if isinstance(part, bytes): result += part.decode(encoding or 'utf-8', errors='ignore')
                else: result += part
            except (LookupError, UnicodeDecodeError): result += str(part) if isinstance(part, str) else part.decode('ascii', errors='ignore')
        return result
    
    except Exception: 
        return str(header) # Fallback

def get_sqlalchemy_type(dtype):
    """Maps pandas dtype to SQLAlchemy type."""

    if pd.api.types.is_integer_dtype(dtype): 
        return BigInteger if dtype.name == 'int64' else Integer
    elif pd.api.types.is_float_dtype(dtype): 
        return Float
    elif pd.api.types.is_datetime64_any_dtype(dtype): 
        return DateTime
    elif pd.api.types.is_bool_dtype(dtype): 
        return Boolean
    elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype): 
        return Text
    else: 
        return Text # Fallback

def calculate_df_hash(df: pd.DataFrame) -> str:
    """Calculates a SHA-256 hash of the DataFrame's content."""

    # Sort by columns and values for consistent hashing
    df_sorted = df.sort_index(axis=1).sort_values(by=df.columns.tolist()).reset_index(drop=True)

    # Convert to CSV string representation (or other canonical format like JSON)
    csv_string = df_sorted.to_csv(index=False, header=True, lineterminator='\n')

    return hashlib.sha256(csv_string.encode('utf-8')).hexdigest()

# ---------- Connections ----------

def get_sqlalchemy_engine():
    """Creates and tests SQLAlchemy engine."""
    try:
        connection_string = f"postgresql+psycopg2://{__DB_USER}:{__DB_PASSWORD}@{__DB_HOST}/{__DB_NAME}"
        engine = create_engine(connection_string, pool_pre_ping=True)

        with engine.connect() as connection: log.info("SQLAlchemy DB connection test successful.")

        return engine
    
    except Exception as e: 
        log.error(f"SQLAlchemy engine failed: {e}")
        raise

def test_imap_connection() -> bool:
    """Tests IMAP connection."""

    try:
        imap_conn = imaplib.IMAP4_SSL(__IMAP_SERVER)
        response, _ = imap_conn.login(__EMAIL, __PASSWORD)

        if response == 'OK': 
            imap_conn.logout() 
            return True

        else: 
            log.error(f"IMAP login failed: {response}") 
            return False

    except Exception as e: 
        log.error(f"IMAP connection error: {e}")
        return False

# ---------- Task Fetching Logic ----------

def fetch_brand_sender_pairs(engine) -> list:
    """Fetches distinct sender/brand pairs from 'emails' table."""

    pairs = []
    sql = text("SELECT DISTINCT email, brand FROM emails WHERE email IS NOT NULL AND brand IS NOT NULL;")

    try:
        with engine.connect() as connection:
            results = connection.execute(sql)
            for row in results:
                pairs.append({'sender': str(row[0]).strip().lower(), 'brand': str(row[1]).strip()})

        log.info(f"Fetched {len(pairs)} sender/brand pairs from 'emails' table.")

        return pairs
    
    except Exception as e: 
        log.error(f"Error fetching from 'emails' table: {e}")
        return []

# ---------- Email Processing Logic ----------

def process_inbox(expected_sender: str):
    """Connects to IMAP, finds emails FROM sender, extracts attachments."""

    if not expected_sender: 
        log.error("process_inbox: expected_sender missing.")
        return []
    processed_data = []

    log.info(f"IMAP: Processing inbox for sender: '{expected_sender}'.")
    imap_conn = None

    try:
        imap_conn = imaplib.IMAP4_SSL(__IMAP_SERVER)
        imap_conn.login(__EMAIL, __PASSWORD)
        imap_conn.select("inbox")

        search_criteria = f'(FROM "{expected_sender.replace("\"", "\\\"")}")' # Basic quoting
        status, messages = imap_conn.search(None, search_criteria)

        if status != 'OK' or not messages or not messages[0]:
            log.info(f"IMAP: No emails found for search: {search_criteria}")
            return []
        
        email_ids = messages[0].split()
        log.info(f"IMAP: Found {len(email_ids)} emails from '{expected_sender}'. Fetching...")

        for num in email_ids:
            msg_id, subject, combined_df = None, None, None # Reset for each email
            try:
                status, data = imap_conn.fetch(num, "(RFC822)")

                if status != 'OK' or not data or not data[0]: 
                    continue

                msg = email.message_from_bytes(data[0][1])
                subject = decode_header_safe(msg.get("Subject", "")).strip()
                parsed_sender = parseaddr(msg.get("From", ""))[1].strip().lower()

                if parsed_sender != expected_sender: 
                    continue # Double check sender

                msg_id = msg.get("Message-ID", "").strip()

                if not msg_id: # Simple fallback if needed
                    msg_id = f"missing_id_subj_{subject[:30]}_num_{num.decode()}"
                    log.warning(f"Generated unstable fallback Message-ID: {msg_id}")

                attachment_dfs = []
                for part in msg.walk():
                    if part.get_content_disposition() and part.get_content_disposition().startswith("attachment"):
                        filename = decode_header_safe(part.get_filename() or '').strip()
                        if not filename: 
                            continue

                        if filename.lower().endswith(('.csv', '.xlsx', '.xls')):
                            payload = part.get_payload(decode=True)
                            if payload:
                                try:
                                    df = None
                                    if filename.lower().endswith('.csv'):
                                        df = pd.read_csv(io.BytesIO(payload))

                                    elif filename.lower().endswith('.xlsx'): 
                                        df = pd.read_excel(io.BytesIO(payload), engine='openpyxl')

                                    elif filename.lower().endswith('.xls'): 
                                        df = pd.read_excel(io.BytesIO(payload), engine='xlrd')

                                    if df is not None and not df.empty: 
                                        attachment_dfs.append(df)

                                except Exception as read_err: 
                                    log.error(f"Read error: '{filename}' in email {msg_id}: {read_err}")

                if attachment_dfs:
                    combined_df = pd.concat(attachment_dfs, ignore_index=True)
                    processed_data.append((msg_id, subject, parsed_sender, combined_df))
                    log.info(f"Extracted {len(combined_df)} rows from {len(attachment_dfs)} attachment(s) in email {msg_id}.")

            except Exception as proc_err:
                 log.error(f"Error processing email num {num.decode()} or its attachments: {proc_err}")

        return processed_data
    
    except Exception as e: 
        log.exception(f"Error in process_inbox for {expected_sender}: {e}")
        return []
    
    finally:
        if imap_conn:
            try:
                imap_conn.logout()

            except Exception:
                pass # Ignore logout errors

# ---------- Save to DB Logic ----------

def save_dataframe_to_db(msg_id: str, sender: str, subject: str, df: pd.DataFrame, engine, target_table: str) -> str:
    """Saves DataFrame if content hash and message_id don't exist. Creates table if needed."""

    if df is None or df.empty:
        return 'failed'
    
    if not msg_id or msg_id.startswith('missing_id_'): 
        log.warning(f"Unreliable msg_id '{msg_id}', skipping save.")
        return 'failed'
    
    if not target_table or not engine:
        log.error("Missing target_table or engine.")
        return 'failed'

    try:
        log.debug(f"Processing msg {msg_id} for table '{target_table}'...")
        df_processed = df.copy()

        # --- Clean column names ---
        original_columns = df_processed.columns.tolist()
        cleaned_columns = []

        for i, col in enumerate(original_columns):
            col_str = str(col).strip(); col_str = re.sub(r'[\s./-]+', '_', col_str)
            col_str = re.sub(r'[^a-zA-Z0-9_]+', '', col_str); col_str = col_str.lower()

            if not col_str: 
                col_str = f"unnamed_col_{i}"

            cleaned_columns.append(col_str) # Basic cleaning, overwrite if needed later
        df_processed.columns = cleaned_columns # Overwrite directly, handle collisions if critical

        # --- Calculate Content Hash ---
        content_hash = calculate_df_hash(df_processed)
        log.debug(f"Calculated content hash for msg {msg_id}: {content_hash[:10]}...")

        # --- Add Metadata ---
        df_processed['message_id'] = msg_id
        df_processed['brand'] = target_table
        df_processed['content_hash'] = content_hash # Add the hash column
        df_processed['ingestion_timestamp_utc'] = pd.Timestamp.utcnow()

        # --- Check Duplicates (Hash first, then Message ID) ---
        inspector = inspect(engine)
        duplicate_found = False
        if inspector.has_table(target_table):
            table_columns = [col['name'] for col in inspector.get_columns(target_table)]
            # Check hash if column exists
            if 'content_hash' in table_columns:
                try:
                    check_sql = text(f'SELECT 1 FROM "{target_table}" WHERE content_hash = :hash LIMIT 1')
                    with engine.connect() as conn: 
                        result = conn.execute(check_sql, {'hash': content_hash}).scalar()

                    if result == 1: 
                        log.info(f"Skipping msg {msg_id}: Duplicate content hash {content_hash[:10]}... found.")
                        return 'skipped_hash'
                    
                except Exception as e: 
                    log.warning(f"Error checking content_hash in {target_table}: {e}")

            # Check message ID if column exists (secondary check)
            if 'message_id' in table_columns:
                try:
                    check_sql = text(f'SELECT 1 FROM "{target_table}" WHERE message_id = :msg_id LIMIT 1')
                    with engine.connect() as conn: 
                        result = conn.execute(check_sql, {'msg_id': msg_id}).scalar()

                    if result == 1: 
                        log.info(f"Skipping msg {msg_id}: Duplicate message_id found.")
                        return 'skipped_msg_id'
                    
                except Exception as e: 
                    log.warning(f"Error checking message_id in {target_table}: {e}")

        # --- Ensure Table Schema ---
        log.debug(f"Proceeding to ensure schema for {target_table}...")
        metadata = MetaData()
        sqlalchemy_columns = []

        for col_name in df_processed.columns:
            sql_type = get_sqlalchemy_type(df_processed[col_name].dtype)
            if col_name == 'message_id' or col_name == 'content_hash': 
                sql_type = Text # Ensure Text for IDs/Hashes

            sqlalchemy_columns.append(Column(col_name, sql_type))

        table = Table(target_table, metadata, *sqlalchemy_columns)
        try:
            table.create(engine, checkfirst=True)
            log.debug(f"Table '{target_table}' schema ready.")

        except Exception as e: 
            log.error(f"Table creation/check error for '{target_table}': {e}")
            return 'failed'

        # --- Insert Data ---
        log.info(f"Inserting {len(df_processed)} rows from msg {msg_id} into '{target_table}'...")
        df_processed.to_sql(name=target_table, con=engine, if_exists='append', index=False, method='multi')

        return 'inserted'

    except Exception as e:
        log.exception(f"Failed to save data for msg {msg_id} to table '{target_table}': {e}")
        return 'failed'

# ---------- Main Execution ----------

def main():
    log.info("--- Email Attachment Scraper Starting ---")
    total_inserted, total_skipped_hash, total_skipped_msgid, total_failed = 0, 0, 0, 0
    db_engine = None
    try:
        db_engine = get_sqlalchemy_engine()
        if not test_imap_connection(): 
            raise ConnectionError("IMAP connection failed.")
        log.info("Connections established.")

        tasks = fetch_brand_sender_pairs(db_engine)
        if not tasks: 
            log.info("No sender/brand tasks found in 'emails' table.")
            return
        
        log.info(f"Found {len(tasks)} tasks.")

        for task in tasks:
            sender, brand_table = task['sender'], task['brand']
            log.info(f"--- Task Start: Sender='{sender}', Table='{brand_table}' ---")
            emails_data = process_inbox(expected_sender=sender)
            task_stats = {'inserted': 0, 'skipped_hash': 0, 'skipped_msg_id': 0, 'failed': 0}
            if emails_data:
                for msg_id, subject, actual_sender, df in emails_data:
                    status = save_dataframe_to_db(msg_id, actual_sender, subject, df, db_engine, brand_table)
                    task_stats[status] = task_stats.get(status, 0) + 1 # Increment appropriate counter

            log.info(f"--- Task End: Sender='{sender}', Table='{brand_table}' | Inserted: {task_stats['inserted']}, Skipped(Hash): {task_stats['skipped_hash']}, Skipped(MsgID): {task_stats['skipped_msg_id']}, Failed: {task_stats['failed']} ---")
            total_inserted += task_stats['inserted']
            total_skipped_hash += task_stats['skipped_hash']
            total_skipped_msgid += task_stats['skipped_msg_id']
            total_failed += task_stats['failed']

        log.info("--- Script Finished ---")
        log.info(f"Overall Summary: Inserted={total_inserted}, Skipped(Hash)={total_skipped_hash}, Skipped(MsgID)={total_skipped_msgid}, Failed={total_failed}")

    except Exception as e: 
        log.exception(f"Critical error in main: {e}")

    finally:
        if db_engine: 
            db_engine.dispose()
            log.info("DB Engine disposed.")
            
        log.info("--- Script Exit ---")

if __name__ == "__main__":
    main()