# Email Attachment Scraper & Database Loader

## Overview

This Python script automates the process of scraping email attachments (CSV and Excel files) from specified senders in an IMAP inbox, processing the data, and loading it into a PostgreSQL database. It dynamically creates target tables based on the data structure and includes logic to prevent duplicate data insertion based on content hashing and email Message IDs.

## Features

* **IMAP Connection:** Connects securely to an IMAP server (tested with Gmail, Outlook/Office365).
* **Sender-Based Configuration:** Reads sender email addresses and their corresponding target database table names ('brands') from a PostgreSQL table named `emails`.
* **Targeted Email Search:** Efficiently searches the IMAP inbox specifically for emails `FROM` the configured senders.
* **Attachment Processing:** Extracts and processes attachments with `.csv`, `.xlsx`, or `.xls` extensions.
* **Data Aggregation:** Concatenates data from multiple valid attachments within a single email into one Pandas DataFrame.
* **Dynamic Table Creation:** If the target table (specified by the 'brand' in the `emails` table) doesn't exist, the script creates it automatically. The table schema is derived from the columns found in the processed attachments plus added metadata columns.
* **Duplicate Prevention:**
    * **Content Hashing:** Calculates a SHA-256 hash of the data content. Checks if this hash already exists in the target table's `content_hash` column before inserting, preventing insertion of identical report data even if sent via different emails.
    * **Message ID Check:** As a secondary check, verifies if the email's unique `Message-ID` already exists in the target table's `message_id` column, preventing reprocessing of the exact same email.
* **Metadata Addition:** Adds useful metadata to each record inserted into the database: `message_id`, `brand`, `content_hash`, and `ingestion_timestamp_utc`.
* **Column Cleaning:** Sanitizes column names read from attachments (lowercase, underscores for spaces, removes special characters) for database compatibility.
* **PostgreSQL Integration:** Uses SQLAlchemy for robust interaction with a PostgreSQL database.
* **Environment Variable Configuration:** Securely manages credentials and server details using a `.env` file.
* **Logging:** Provides informative logs about the script's progress, successes, skips, and errors.

## Requirements

* Python 3.7+
* Required Python libraries:
    * `pandas`
    * `openpyxl` (for `.xlsx` files)
    * `xlrd` (for `.xls` files)
    * `SQLAlchemy`
    * `psycopg2-binary` (PostgreSQL driver for SQLAlchemy)
    * `python-dotenv` (for reading `.env` file)
    * `numpy` (dependency for pandas)

## Installation

1.  **Clone or Download:** Get the script file (`your_script_name.py`).
2.  **Install Libraries:** Navigate to the script's directory in your terminal and install the required libraries:
    ```bash
    pip install pandas openpyxl xlrd SQLAlchemy psycopg2-binary python-dotenv numpy
    ```

## Configuration

1.  **`.env` File:**
    Create a file named `.env` in the same directory as the script. Add the following environment variables with your specific details:

    ```dotenv
    # Database Credentials
    DB_USER=your_postgres_user
    DB_PASSWORD=your_postgres_password
    DB_HOST=your_postgres_host_or_ip # e.g., localhost
    DB_NAME=your_database_name     # DB containing 'emails' table and target tables

    # IMAP Email Account Credentials
    EMAIL=your_email_address@example.com
    PASSWORD=your_email_password_or_app_password # Use App Password for Gmail/Outlook if 2FA enabled
    IMAP_SERVER=your_imap_server # e.g., imap.gmail.com or outlook.office365.com
    ```
    * **Important:** For Gmail or Outlook with Multi-Factor Authentication (MFA/2FA), you likely need to generate an "App Password" and use that in the `PASSWORD` field instead of your regular account password.

2.  **PostgreSQL Database Setup:**
    * Ensure you have a running PostgreSQL database accessible with the credentials provided in `.env`.
    * Connect to the database specified in `DB_NAME`.
    * **Create the `emails` table:** This table tells the script which senders to monitor and where to put their data. Execute the following SQL command (or use a GUI tool):
        ```sql
        CREATE TABLE emails (
            email VARCHAR(255) NOT NULL, -- Sender email address
            brand VARCHAR(100) NOT NULL, -- Target table name for this sender's data
            PRIMARY KEY (email)          -- Ensure each sender is listed once
        );
        ```
    * **Populate the `emails` table:** Insert rows for each sender you want to process.
        ```sql
        -- Example:
        INSERT INTO emails (email, brand) VALUES
        ('reports@company-a.com', 'company_a_reports'),
        ('daily_stats@company-b.com', 'company_b_daily'),
        ('company-c@compc.com', 'company_c_monthly');
        -- Add more rows as needed
        ```
        * The `brand` value **must** be a valid PostgreSQL table name. The script will use this name to create/access the target data table.

3.  **Target (`brand`) Tables:**
    * You do **not** need to create these tables manually (e.g., `company_a_reports`, `leovegas`).
    * The script will automatically create a target table the first time it processes data for a specific `brand` if the table doesn't already exist.
    * The schema (columns and data types) of the created table will be based on the columns found in the first processed attachment file for that brand, plus the metadata columns (`message_id`, `brand`, `content_hash`, `ingestion_timestamp_utc`).

4.  **Database Permissions:**
    The database user specified (`DB_USER`) needs sufficient permissions in the target database (`DB_NAME`) to:
    * Connect to the database.
    * Read from the `emails` table (`SELECT`).
    * Check if tables exist (`SELECT` on information schema or similar).
    * Create new tables (`CREATE`).
    * Read from target tables (`SELECT` for duplicate checks).
    * Insert data into target tables (`INSERT`).

## How it Works

1.  **Initialization:** Loads environment variables, sets up logging.
2.  **Connections:** Establishes and tests connections to the PostgreSQL database (via SQLAlchemy) and the IMAP server. Exits if connections fail.
3.  **Fetch Tasks:** Queries the `emails` table to get the list of configured sender/brand pairs.
4.  **Loop Through Tasks:** Iterates through each sender/brand pair.
5.  **IMAP Search:** For the current sender, performs an IMAP search specifically for emails `FROM` that sender's address.
6.  **Email Processing:**
    * Iterates through the emails found via search.
    * Fetches the full email content (RFC822).
    * Parses the email using Python's `email` library.
    * Extracts the `Message-ID` (generating a fallback if missing, though this can impact deduplication).
    * Identifies attachments (`.csv`, `.xlsx`, `.xls`).
7.  **Attachment Handling:**
    * Reads valid attachments into Pandas DataFrames.
    * If multiple valid attachments exist in one email, concatenates them into a single DataFrame.
8.  **Data Saving (`save_dataframe_to_db`):**
    * Takes the combined DataFrame, email metadata, engine, and target table name (`brand`).
    * **Cleans Columns:** Sanitizes DataFrame column names for DB compatibility.
    * **Calculates Hash:** Computes a SHA-256 hash of the DataFrame's content.
    * **Adds Metadata:** Adds `message_id`, `brand`, `content_hash`, and `ingestion_timestamp_utc` columns.
    * **Duplicate Check (Content):** Queries the target table to see if the calculated `content_hash` already exists. If yes, skips the insertion and returns `'skipped_hash'`.
    * **Duplicate Check (Message ID):** If the content hash is unique, queries the target table to see if the `message_id` already exists. If yes, skips the insertion and returns `'skipped_msg_id'`.
    * **Table Creation/Schema Check:** Ensures the target table exists using `table.create(engine, checkfirst=True)`. If the table is created, the schema is based on the final processed DataFrame.
    * **Data Insertion:** If no duplicates are found, appends the DataFrame's data to the target table using `df.to_sql()`. Returns `'inserted'`.
    * Returns `'failed'` if any error occurs during saving.
9.  **Logging & Summary:** Logs progress for each task and provides an overall summary of inserted, skipped, and failed records upon completion.
10. **Cleanup:** Disposes of the SQLAlchemy engine connection pool.

## Usage

1.  Ensure all setup and configuration steps are complete.
2.  Open your terminal or command prompt.
3.  Navigate to the directory containing the script and the `.env` file.
4.  Run the script using Python:
    ```bash
    python your_script_name.py
    ```
    (Replace `your_script_name.py` with the actual filename).

The script will then connect, fetch tasks, process emails, and save data according to the configuration, logging its actions to the console.

## Troubleshooting

* **Connection Errors:** Double-check credentials, hostnames, ports, and firewall rules for both the database and IMAP server in your `.env` file. Ensure the database server is running. For IMAP, check if less secure app access or App Passwords are required.
* **Table 'emails' Not Found:** Ensure you have created the `emails` table in the correct database (`DB_NAME`) with the specified columns (`email`, `brand`).
* **Permission Denied:** Verify the database user (`DB_USER`) has the necessary permissions (SELECT, INSERT, CREATE TABLE, etc.).
* **File Reading Errors:** Check the logs for errors related to reading specific CSV/Excel files. The file might be corrupted, password-protected, or in an unexpected format. Ensure `openpyxl` and `xlrd` are installed.
* **Duplicate Data Still Appears:**
    * Verify the `REPORT_UNIQUE_KEY_COLUMNS` list (if using that method) accurately reflects unique identifiers in your files.
    * If using hashing, check if minor, insignificant variations in the files are causing different hashes.
    * Ensure the `message_id` column exists in the target tables if relying on that check. Check logs for warnings about unreliable Message IDs.
* **Check Logs:** The script outputs logs to the console. Review these logs carefully for specific error messages or warnings. Increase logging level (e.g., to `DEBUG`) in the script for more detailed information if needed.
