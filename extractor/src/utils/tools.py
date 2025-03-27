import MySQLdb
import pandas as pd
import re
import sys
import json
import os
from typing import List, Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
from config import files_paths as output_paths
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def connect_database(config: Dict[str, Any]):
    """Connect to the database using mysqlclient with retries.
    
    Args:
        config: Dictionary with host, user, password, port, and database details.
    
    Returns:
        MySQLdb connection object.
    
    Raises:
        MySQLdb.Error: If connection fails after retries.
    """
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

def store_json(data: Any, filename: str):
    """Store data in a JSON file.
    
    Args:
        data: Data to store.
        filename: Path to the JSON file.
    """
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        logging.info(f"Stored data to JSON file: {filename}")
    except Exception as e:
        logging.error(f"Error storing JSON to {filename}: {e}")
        raise

def load_json(filename: str) -> Any:
    """Load data from a JSON file.
    
    Args:
        filename: Path to the JSON file.
    
    Returns:
        Loaded data from the file.
    """
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
        logging.info(f"Loaded data from JSON file: {filename}")
        return data
    except FileNotFoundError:
        logging.warning(f"JSON file not found: {filename}, returning None")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in {filename}: {e}")
        raise

def store_csv(data: List[List[str]], filename: str):
    """Store data in a CSV file.
    
    Args:
        data: List of rows to store.
        filename: Path to the CSV file.
    """
    try:
        with open(filename, 'w') as f:
            for row in data:
                f.write(','.join(map(str, row)) + '\n')
        logging.info(f"Stored data to CSV file: {filename}")
    except Exception as e:
        logging.error(f"Error storing CSV to {filename}: {e}")
        raise

def load_csv(filename: str) -> List[List[str]]:
    """Load data from a CSV file.
    
    Args:
        filename: Path to the CSV file.
    
    Returns:
        List of rows from the file.
    """
    try:
        data = []
        with open(filename, 'r') as f:
            for line in f:
                data.append(line.strip().split(','))
        logging.info(f"Loaded {len(data)} rows from CSV file: {filename}")
        return data
    except FileNotFoundError:
        logging.warning(f"CSV file not found: {filename}, returning empty list")
        return []
    except Exception as e:
        logging.error(f"Error loading CSV from {filename}: {e}")
        raise

def store_txt(data: List[str], filename: str):
    """Store data in a text file.
    
    Args:
        data: List of strings to store.
        filename: Path to the text file.
    """
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as f:
            f.write('\n'.join(data))
        logging.info(f"Stored {len(data)} lines to text file: {filename}")
    except Exception as e:
        logging.error(f"Error storing text to {filename}: {e}")
        raise

def load_txt(filename: str) -> List[str]:
    """Load data from a text file.
    
    Args:
        filename: Path to the text file.
    
    Returns:
        List of lines from the file.
    """
    try:
        with open(filename, 'r') as f:
            data = f.read().splitlines()
        logging.info(f"Loaded {len(data)} lines from text file: {filename}")
        return data
    except FileNotFoundError:
        logging.warning(f"Text file not found: {filename}, returning empty list")
        return []
    except Exception as e:
        logging.error(f"Error loading text from {filename}: {e}")
        raise

def filter_tables(table_names: List[str], pattern: re.Pattern) -> List[str]:
    """Filter table names based on a regex pattern.
    
    Args:
        table_names: List of table names to filter.
        pattern: Compiled regex pattern to match against.
    
    Returns:
        Filtered list of table names.
    """
    filtered = [table for table in table_names if re.match(pattern, table)]
    logging.info(f"Filtered {len(filtered)} tables matching pattern {pattern.pattern}")
    return filtered

def filter_by_year(tables: List[str], start_year: int) -> List[str]:
    """Filter tables by year (e.g., 2024 or greater).
    
    Args:
        tables: List of table names to filter.
        start_year: Minimum year to include.
    
    Returns:
        Filtered list of table names.
    """
    filtered_tables = []
    for table in tables:
        match = re.search(r'_A(\d{4})$', table, re.IGNORECASE)
        if match:
            year = int(match.group(1))
            if year >= start_year:
                filtered_tables.append(table)
                logging.info(f"Matched table '{table}' with year {year} >= {start_year}")
            else:
                logging.info(f"Skipped table '{table}' - year {year} < {start_year}")
        else:
            logging.warning(f"Skipped table '{table}' - no year found in format '_AXXXX'")
    return filtered_tables

def sort_by_year_and_week(tables: List[str]) -> List[str]:
    """Sort tables by year and week.
    
    Args:
        tables: List of table names to sort.
    
    Returns:
        Sorted list of table names.
    """
    try:
        sorted_tables = sorted(tables, key=lambda x: (
            int(re.search(r'_A(\d{4})$', x, re.IGNORECASE).group(1)),
            int(re.search(r'_S(\d+)_', x, re.IGNORECASE).group(1))
        ))
        logging.info(f"Sorted {len(sorted_tables)} tables by year and week")
        return sorted_tables
    except Exception as e:
        logging.error(f"Error sorting tables: {e}")
        raise

def process_tables_names(table_names: List[str], patterns: Dict[str, re.Pattern], start_year: int) -> List[str]:
    """Process table names by filtering and sorting them.
    
    Args:
        table_names: List of all table names from the database.
        patterns: Dictionary of regex patterns for filtering.
        start_year: Minimum year to include.
    
    Returns:
        Unified sorted list of filtered table names.
    """
    filtered_5min = filter_tables(table_names, patterns['5min'])
    filtered_15min = filter_tables(table_names, patterns['15min'])
    filtered_mgw = filter_tables(table_names, patterns['mgw'])
    
    logging.info(f"Found {len(filtered_5min)} 5-minute tables: {filtered_5min}")
    logging.info(f"Found {len(filtered_15min)} 15-minute tables: {filtered_15min}")
    logging.info(f"Found {len(filtered_mgw)} MGW tables: {filtered_mgw}")

    filtered_5min_by_year = filter_by_year(filtered_5min, start_year)
    filtered_15min_by_year = filter_by_year(filtered_15min, start_year)
    filtered_mgw_by_year = filter_by_year(filtered_mgw, start_year)

    sorted_5min = sort_by_year_and_week(filtered_5min_by_year)
    sorted_15min = sort_by_year_and_week(filtered_15min_by_year)
    sorted_mgw = sort_by_year_and_week(filtered_mgw_by_year)

    store_txt(sorted_5min, output_paths['5min'])
    store_txt(sorted_15min, output_paths['15min'])
    store_txt(sorted_mgw, output_paths['mgw'])

    logging.info(f"Filtered 5-minute results saved to {output_paths['5min']}")
    logging.info(f"Filtered 15-minute results saved to {output_paths['15min']}")
    logging.info(f"Filtered MGW results saved to {output_paths['mgw']}")

    total_tables = len(sorted_5min) + len(sorted_15min) + len(sorted_mgw)
    logging.info(f"Total tables found: {total_tables}")
    
    unified_sorted_tables = sorted_5min + sorted_15min + sorted_mgw
    unified_sorted_tables = sort_by_year_and_week(unified_sorted_tables)
    logging.info(f"Returning unified sorted list: {unified_sorted_tables}")
    return unified_sorted_tables

def load_indicator_csv(table: str) -> Dict[int, str]:
    """Load indicator data from CSV with headers into a dictionary.
    
    Args:
        table: Table name to derive the CSV filename from.
    
    Returns:
        Dictionary mapping ID_indicateur to indicateur.
    """
    base_table_name = re.sub(r'_s\d+_a\d{4}$', '', table, flags=re.IGNORECASE)
    csv_path = f"./data/indicators/indicateur_{base_table_name}.csv"
    if not os.path.exists(csv_path):
        logging.warning(f"Indicator CSV not found: {csv_path}, returning empty dict")
        return {}
    
    try:
        df = pd.read_csv(csv_path, dtype={'ID_indicateur': int, 'indicateur': str, 'type': str})
        indicator_map = dict(zip(df['ID_indicateur'], df['indicateur']))
        logging.info(f"Loaded indicator map from {csv_path} with {len(indicator_map)} entries")
        return indicator_map
    except Exception as e:
        logging.error(f"Error loading CSV {csv_path}: {e}")
        return {}

def load_last_extracted(filename: str = output_paths['last_extracted']) -> Dict[str, Any]:
    """Load the last extracted data for each table from a JSON file.
    
    Args:
        filename: Path to the JSON file (default from config).
    
    Returns:
        Dictionary with last extracted info or empty dict if not found/invalid.
    """
    try:
        with open(filename, 'r') as f:
            content = f.read().strip()
            if not content:
                logging.warning(f"{filename} is empty, returning empty dict")
                return {}
            data = json.loads(content)
            logging.info(f"Loaded last extracted info from {filename}")
            return data
    except FileNotFoundError:
        logging.info(f"{filename} not found, returning empty dict")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in {filename}: {e}, returning empty dict")
        return {}

def save_last_extracted(last_extracted: Dict[str, Any], filename: str = output_paths['last_extracted']):
    """Save the last extracted data for each table to a JSON file.
    
    Args:
        last_extracted: Dictionary with extraction info.
        filename: Path to the JSON file (default from config).
    """
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(last_extracted, f, indent=4)
        logging.info(f"Saved last extracted info to {filename}")
    except Exception as e:
        logging.error(f"Error saving last extracted to {filename}: {e}")
        raise

def extract_table_data(table: str, cursor, offset: int, batch_size: int = 5000) -> Optional[List[tuple]]:
    """Extract raw data from table in batches based on offset.
    
    Args:
        table: Name of the table to extract from.
        cursor: Database cursor to execute queries.
        offset: Starting row offset for the batch.
        batch_size: Number of rows to fetch per batch (default: 5000).
    
    Returns:
        List of tuples (date_heure, indicateur, valeur) or None if no data.
    """
    query = f"""
        SELECT date_heure, ID_indicateur, valeur
        FROM {table}
        ORDER BY date_heure
        LIMIT {batch_size} OFFSET {offset}
    """
    try:
        cursor.execute(query)
        raw_data = cursor.fetchall()
        logging.info(f"Executed query for {table} at offset {offset}, fetched {len(raw_data)} rows")
    except MySQLdb.Error as e:
        logging.error(f"SQL error for table {table}: {e}")
        return None
    
    if not raw_data:
        logging.info(f"No data fetched for table {table} at offset {offset}")
        return None
    
    indicator_map = load_indicator_csv(table)
    if not indicator_map:
        logging.error(f"Cannot proceed without indicator mapping for {table}")
        return None
    
    result = []
    for date_heure, id_indicateur, valeur in raw_data:
        indicateur = indicator_map.get(id_indicateur, "Unknown")
        result.append((date_heure, indicateur, valeur))
    
    logging.info(f"Processed {len(result)} rows for {table} with indicator mapping")
    return result

def load_batch_into_database(batch: List[tuple], target_db, target_table: str):
    """Load a batch of data into the target database.
    
    Args:
        batch: List of tuples (date_heure, indicateur, valeur) to load.
        target_db: Target database connection.
        target_table: Name of the table to load into.
    """
    cursor = target_db.cursor()
    try:
        cursor.execute(f"SHOW TABLES LIKE '{target_table}'")
        if not cursor.fetchone():
            create_query = f"""
                CREATE TABLE {target_table} (
                    Date DATETIME,
                    indicateur VARCHAR(255),
                    valeur FLOAT
                )
            """
            cursor.execute(create_query)
            target_db.commit()
            logging.info(f"Created table {target_table}")

        columns = ['Date', 'indicateur', 'valeur']
        placeholders = ', '.join(['%s'] * len(batch[0]))
        insert_query = f"INSERT INTO {target_table} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.executemany(insert_query, batch)
        target_db.commit()
        logging.info(f"Successfully loaded {len(batch)} rows into {target_table}")
    except MySQLdb.Error as e:
        logging.error(f"Error loading batch into {target_table}: {e}")
        target_db.rollback()
        raise
    finally:
        cursor.close()