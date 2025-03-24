import MySQLdb  
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
            passwd=config['password'],  # 'passwd' is used instead of 'password'
            port=config['port'],
            db=config['database']      # 'db' is used instead of 'database'
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
            f.write(','.join(row) + '\n')

def load_csv(filename: str) -> List[List[str]]:
    """Load data from a CSV file."""
    data = []
    with open(filename, 'r') as f:
        for line in f:
            data.append(line.strip().split(','))
    return data

def store_txt(data: List[str], filename: str):
    """Store data in a text file."""
    os.makedirs(os.path.dirname(filename), exist_ok=True)  # Ensure directory exists
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
    """Filter tables by year (2024 or greater)."""
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
        int(re.search(r'_A(\d{4})$', x, re.IGNORECASE).group(1)),  # Extract year
        int(re.search(r'_S(\d+)_', x, re.IGNORECASE).group(1))    # Extract week number
    ))

def process_tables_names(table_names: List[str], patterns: Dict[str, re.Pattern], start_year: int):
    """Process table names by filtering and sorting them, excluding indicator tables."""
    # Filter tables by pattern
    filtered_5min = filter_tables(table_names, patterns['5min'])
    filtered_15min = filter_tables(table_names, patterns['15min'])
    filtered_mgw = filter_tables(table_names, patterns['mgw'])
    
    print(f"✅ Found {len(filtered_5min)} 5-minute tables: {filtered_5min}")
    print(f"✅ Found {len(filtered_15min)} 15-minute tables: {filtered_15min}")
    print(f"✅ Found {len(filtered_mgw)} MGW tables: {filtered_mgw}")

    # Filter by year
    filtered_5min_by_year = filter_by_year(filtered_5min, start_year)
    filtered_15min_by_year = filter_by_year(filtered_15min, start_year)
    filtered_mgw_by_year = filter_by_year(filtered_mgw, start_year)

    # Sort by year and week
    sorted_5min = sort_by_year_and_week(filtered_5min_by_year)
    sorted_15min = sort_by_year_and_week(filtered_15min_by_year)
    sorted_mgw = sort_by_year_and_week(filtered_mgw_by_year)

    # Store only the filtered and sorted tables
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

def join_table(table: str) -> str:
    """Derive the join table name by removing the _SXX_AXXXX suffix."""
    base_table_name = re.sub(r'_s\d+_a\d{4}$', '', table, flags=re.IGNORECASE)
    return f"indicateur_{base_table_name}"

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

def extract_table_data(table: str, cursor, offset: int, batch_size: int = 5000) -> Optional[List[tuple]]:
    """Extract a single batch of data from the specified table, incrementally, sorted by date_heure."""
    last_extracted = load_last_extracted()
    last_date = last_extracted.get(table, None)
    
    query = f"""
        SELECT t1.date_heure as Date, t2.indicateur, t1.valeur
        FROM {table} t1 
        JOIN {join_table(table)} t2 
        ON t1.ID_indicateur = t2.ID_indicateur 
    """
    if last_date:
        query += f" WHERE t1.date_heure > '{last_date}'"
    query += f" ORDER BY t1.date_heure ASC LIMIT {batch_size} OFFSET {offset}"
    
    print(f"🔍 Executing query: {query}")
    
    try:
        cursor.execute(query)
        batch = cursor.fetchall()
    except MySQLdb.Error as e:
        print(f"❌ SQL error for table {table}: {e}")
        return None
    
    if batch:
        max_date = max(row[0] for row in batch)
        last_extracted[table] = str(max_date)
        save_last_extracted(last_extracted)
        print(f"✅ Updated last_extracted for {table} with {max_date} after batch")
    
    return batch if batch else None

def load_batch_into_database(batch: List[tuple], target_db, target_table: str):
    """Load a batch of data into the target database, creating the table if it doesn’t exist."""
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