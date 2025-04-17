import MySQLdb
import json
import os
from typing import List, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from config import KPI_FORMULAS_5MIN, KPI_FAMILIES

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

def create_kpi_tables(cursor, KPI_FORMULAS, KPI_FAMILIES):
    """Create KPI-specific tables based on KPI_FORMULAS and KPI_FAMILIES config."""
    try:
        # Create family-based tables
        for family, kpis in KPI_FAMILIES.items():
            columns_set = {"kpi_id INT NOT NULL", "suffix VARCHAR(50)", "operator VARCHAR(50)", "kpi VARCHAR(50) NOT NULL", "type VARCHAR(10)"}
            # Collect all unique counters for the family
            all_fields = set()
            for kpi in kpis:
                config = KPI_FORMULAS[kpi]
                all_fields.update(config.get('numerator', []) + config.get('denominator', []) + config.get('additional', []))
            for col in all_fields:
                columns_set.add(f"{col} FLOAT")
            columns_set.add("value FLOAT")
            columns_str = ",\n    ".join(sorted(columns_set))
            create_query = f"""
            CREATE TABLE IF NOT EXISTS {family}_details (
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                {columns_str},
                FOREIGN KEY (kpi_id) REFERENCES kpi_summary(Id)
            );
            """
            cursor.execute(create_query)
            logging.info(f"✅ Table '{family}_details' created or already exists.")

        # Create individual tables for KPIs not in a family
        for kpi, config in KPI_FORMULAS.items():
            if config.get('family') in KPI_FAMILIES:
                continue  # Skip KPIs already handled in family tables
            columns_set = {"kpi_id INT NOT NULL"}
            if config.get('Suffix', False):
                columns_set.add("suffix VARCHAR(50)")
            columns_set.add("operator VARCHAR(50)")
            all_fields = (
                config.get('numerator', []) +
                config.get('denominator', []) +
                config.get('additional', [])
            )
            for col in all_fields:
                columns_set.add(f"{col} FLOAT")
            columns_set.add("value FLOAT")
            columns_str = ",\n    ".join(sorted(columns_set))
            create_query = f"""
            CREATE TABLE IF NOT EXISTS {kpi}_details (
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                {columns_str},
                FOREIGN KEY (kpi_id) REFERENCES kpi_summary(Id)
            );
            """
            cursor.execute(create_query)
            logging.info(f"✅ Table '{kpi}_details' created or already exists.")
    except MySQLdb.Error as e:
        logging.error(f"Error creating KPI tables: {e}")
        raise

def create_tables(cursor, KPI_FORMULAS, KPI_FAMILIES):
    """Create all necessary tables in the database."""
    try:
        create_main_table(cursor)
        create_kpi_tables(cursor, KPI_FORMULAS, KPI_FAMILIES)
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