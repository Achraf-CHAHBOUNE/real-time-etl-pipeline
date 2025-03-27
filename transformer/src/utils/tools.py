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