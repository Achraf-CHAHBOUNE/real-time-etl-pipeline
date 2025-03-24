import MySQLdb
import pandas as pd
import re
import sys
import json
import os
from typing import List, Dict, Any, Optional
from config import files_paths as output_paths

def connect_database(config: Dict[str, Any]):
    """Connect to the database using mysqlclient."""
    try:
        conn = MySQLdb.connect(
            host=config['host'],
            user=config['user'],
            passwd=config['password'],
            port=config['port'],
            db=config['database']
        )
        return conn
    except MySQLdb.Error as e:
        print(f"❌ Connection error: {e}")
        sys.exit(1)

def store_json(data: Any, filename: str):
    """Store data in a JSON file."""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

def load_json(filename: str) -> Any:
    """Load data from a JSON file."""
    with open(filename, 'r') as f:
        return json.load(f)

def store_csv(data: List[List[str]], filename: str):
    """Store data in a CSV file."""
    with open(filename, 'w') as f:
        for row in data:
            f.write(','.join(map(str, row)) + '\n')

def load_csv(filename: str) -> List[List[str]]:
    """Load data from a CSV file."""
    data = []
    with open(filename, 'r') as f:
        for line in f:
            data.append(line.strip().split(','))
    return data

def store_txt(data: List[str], filename: str):
    """Store data in a text file."""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as f:
        f.write('\n'.join(data))

def load_txt(filename: str) -> List[str]:
    """Load data from a text file."""
    with open(filename, 'r') as f:
        return f.read().splitlines()

def filter_tables(table_names: List[str], pattern: re.Pattern) -> List[str]:
    """Filter table names based on a regex pattern."""
    return [table for table in table_names if re.match(pattern, table)]

def filter_by_year(tables: List[str], start_year: int) -> List[str]:
    """Filter tables by year (e.g., 2024 or greater)."""
    filtered_tables = []
    for table in tables:
        match = re.search(r'_A(\d{4})$', table, re.IGNORECASE)
        if match:
            year = int(match.group(1))
            print(f"✅ Matched table '{table}' with year {year}")
            if year >= start_year:
                filtered_tables.append(table)
            else:
                print(f"ℹ️ Skipping table '{table}' - year {year} < {start_year}")
        else:
            print(f"⚠️ Skipping table '{table}' - no year found in format '_AXXXX'")
    return filtered_tables

def sort_by_year_and_week(tables: List[str]) -> List[str]:
    """Sort tables by year and week."""
    return sorted(tables, key=lambda x: (
        int(re.search(r'_A(\d{4})$', x, re.IGNORECASE).group(1)),
        int(re.search(r'_S(\d+)_', x, re.IGNORECASE).group(1))
    ))

def process_tables_names(table_names: List[str], patterns: Dict[str, re.Pattern], start_year: int) -> List[str]:
    """Process table names by filtering and sorting them."""
    filtered_5min = filter_tables(table_names, patterns['5min'])
    filtered_15min = filter_tables(table_names, patterns['15min'])
    filtered_mgw = filter_tables(table_names, patterns['mgw'])
    
    print(f"✅ Found {len(filtered_5min)} 5-minute tables: {filtered_5min}")
    print(f"✅ Found {len(filtered_15min)} 15-minute tables: {filtered_15min}")
    print(f"✅ Found {len(filtered_mgw)} MGW tables: {filtered_mgw}")

    filtered_5min_by_year = filter_by_year(filtered_5min, start_year)
    filtered_15min_by_year = filter_by_year(filtered_15min, start_year)
    filtered_mgw_by_year = filter_by_year(filtered_mgw, start_year)

    sorted_5min = sort_by_year_and_week(filtered_5min_by_year)
    sorted_15min = sort_by_year_and_week(filtered_15min_by_year)
    sorted_mgw = sort_by_year_and_week(filtered_mgw_by_year)

    store_txt(sorted_5min, output_paths['5min'])
    store_txt(sorted_15min, output_paths['15min'])
    store_txt(sorted_mgw, output_paths['mgw'])

    print(f"✅ Filtered 5-minute results saved to {output_paths['5min']}")
    print(f"✅ Filtered 15-minute results saved to {output_paths['15min']}")
    print(f"✅ Filtered MGW results saved to {output_paths['mgw']}")

    total_tables = len(sorted_5min) + len(sorted_15min) + len(sorted_mgw)
    print(f"✅ Total tables found: {total_tables}")
    
    unified_sorted_tables = sorted_5min + sorted_15min + sorted_mgw
    unified_sorted_tables = sort_by_year_and_week(unified_sorted_tables)
    print(f"ℹ️ Returning unified sorted list: {unified_sorted_tables}")
    return unified_sorted_tables

def load_indicator_csv(table: str) -> Dict[int, str]:
    """Load indicator data from CSV with headers into a dictionary."""
    base_table_name = re.sub(r'_s\d+_a\d{4}$', '', table, flags=re.IGNORECASE)
    csv_path = f"./data/indicators/indicateur_{base_table_name}.csv"
    if not os.path.exists(csv_path):
        print(f"❌ Indicator CSV not found: {csv_path}")
        return {}
    
    try:
        # Read CSV with headers
        df = pd.read_csv(csv_path, dtype={'ID_indicateur': int, 'indicateur': str, 'type': str})
        return dict(zip(df['ID_indicateur'], df['indicateur']))
    except Exception as e:
        print(f"❌ Error loading CSV {csv_path}: {e}")
        return {}

def load_last_extracted(filename: str = output_paths['last_extracted']) -> Dict[str, str]:
    """Load the last extracted date_heure for each table from a JSON file."""
    try:
        with open(filename, 'r') as f:
            content = f.read().strip()
            if not content:
                print(f"⚠️ {filename} is empty, returning empty dict")
                return {}
            return json.loads(content)
    except FileNotFoundError:
        print(f"ℹ️ {filename} not found, returning empty dict")
        return {}
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in {filename}: {e}, returning empty dict")
        return {}

def save_last_extracted(last_extracted: Dict[str, str], filename: str = output_paths['last_extracted']):
    """Save the last extracted date_heure for each table to a JSON file."""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as f:
        json.dump(last_extracted, f, indent=4)

def extract_table_data(table: str, cursor, offset: int, batch_size: int = 500) -> Optional[List[tuple]]:
    """Extract raw data from table and join locally with indicator CSV."""
    last_extracted = load_last_extracted()
    last_date = last_extracted.get(table, None)
    
    query = f"""
        SELECT date_heure, ID_indicateur, valeur
        FROM {table}
    """
    if last_date:
        query += f" WHERE date_heure > '{last_date}'"
    query += f" LIMIT {batch_size} OFFSET {offset}"
    
    print(f"🔍 Executing query: {query}")
    
    try:
        cursor.execute(query)
        raw_data = cursor.fetchall()
    except MySQLdb.Error as e:
        print(f"❌ SQL error for table {table}: {e}")
        return None
    
    if not raw_data:
        print(f"ℹ️ No data fetched for table {table}")
        return None
    
    # Load indicator mapping from CSV
    indicator_map = load_indicator_csv(table)
    if not indicator_map:
        print(f"❌ Cannot proceed without indicator mapping for {table}")
        return None
    
    # Perform local join
    result = []
    for date_heure, id_indicateur, valeur in raw_data:
        indicateur = indicator_map.get(id_indicateur, "Unknown")
        result.append((date_heure, indicateur, valeur))
    
    # Sort locally by date_heure
    result = sorted(result, key=lambda x: x[0])
    
    if result:
        max_date = result[-1][0]
        last_extracted[table] = str(max_date)
        save_last_extracted(last_extracted)
        print(f"✅ Updated last_extracted for {table} with {max_date} after batch")
    
    return result

def load_batch_into_database(batch: List[tuple], target_db, target_table: str):
    """Load a batch of data into the target database."""
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
            print(f"✅ Created table {target_table}")

        columns = ['Date', 'indicateur', 'valeur']
        placeholders = ', '.join(['%s'] * len(batch[0]))
        insert_query = f"INSERT INTO {target_table} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.executemany(insert_query, batch)
        target_db.commit()
        print(f"✅ Successfully loaded {len(batch)} rows into {target_table}")
    except MySQLdb.Error as e:
        print(f"❌ Error loading batch into {target_table}: {e}")
        target_db.rollback()
    finally:
        cursor.close()