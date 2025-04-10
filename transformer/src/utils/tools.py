import MySQLdb
import json
import os
from typing import List, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def connect_database(config: Dict[str, Any]):
    """Connect to the database using mysqlclient with retries."""
    try:
        conn = MySQLdb.connect(
            host=config['host'],
            user=config['user'],
            passwd=config['password'],
            port=config['port'],
            db=config['database']
        )
        logging.info(f"Successfully connected to database: {config['database']} on {config['host']}")
        return conn
    except MySQLdb.Error as e:
        logging.error(f"Database connection error: {e}")
        raise

# Other utility functions (store_json, load_json, etc.) remain unchanged...

def create_main_table(cursor):
    """Create the main table if it doesn't exist."""
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS kpi_summary (
                Id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                Date DATETIME NOT NULL,
                Node VARCHAR(50) NOT NULL
            );
        """)
        logging.info("✅ Main table 'kpi_summary' created or already exists.")
    except MySQLdb.Error as e:
        logging.error(f"Error creating main table: {e}")
        raise

def create_kpi_tables(cursor, KPI_FORMULAS):
    """Create KPI-specific tables based on KPI_FORMULAS config, ensuring no duplicate columns."""
    try:
        for table_name, config in KPI_FORMULAS.items():
            # Use a set to ensure unique column definitions
            columns_set = {"kpi_id INT NOT NULL"}
            
            if config.get('Suffix', False):
                columns_set.add("suffix VARCHAR(50)")

            # Collect all unique column names from numerator, denominator, and additional fields
            all_fields = (
                config.get('numerator', []) +
                config.get('denominator', []) +
                config.get('additional', [])
            )
            for col in all_fields:
                columns_set.add(f"{col} FLOAT")

            # Always include value column
            columns_set.add("value FLOAT")

            # Convert set to list and join for SQL query
            columns_str = ",\n    ".join(sorted(columns_set))  # Sort for consistency

            create_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}_details (
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                {columns_str},
                FOREIGN KEY (kpi_id) REFERENCES kpi_summary(Id)
            );
            """
            cursor.execute(create_query)
            logging.info(f"✅ Table '{table_name}_details' created or already exists.")
    except MySQLdb.Error as e:
        logging.error(f"Error creating KPI tables: {e}")
        raise

def create_tables(cursor, KPI_FORMULAS):
    """Create all necessary tables in the database."""
    try:
        create_main_table(cursor)
        create_kpi_tables(cursor, KPI_FORMULAS)
        logging.info("✅ All tables created successfully.")
    except MySQLdb.Error as e:
        logging.error(f"Error creating tables: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error creating tables: {e}")
        raise

def extract_noeud(pattern, texts):
    """Extracts prefixes from the provided list of texts using the given regex pattern."""
    matches = []
    for text in texts:
        match = pattern.match(text)
        if match:
            prefix = match.group(1).upper()
            matches.append((text, prefix))
    return matches

def extract_indicateur_suffixe(indicateur):
    """Extract the suffix from the KPI name."""
    if not isinstance(indicateur, str):
        raise ValueError("Indicateur must be a string")
    
    parts = indicateur.split('.')
    
    if len(parts) == 2:
        return parts[0], parts[1]
    
    return parts[0], None