import logging
from tools import connect_database, load_batch_into_database

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Loader:
    def __init__(self, config):
        self.config = config
        self.db = None
        self.cursor = None
        self.connect()

    def connect(self):
        """Connect to the database."""
        self.db = connect_database(self.config)  # Retries handled in tools.py
        self.cursor = self.db.cursor()

    def load_batch_into_database(self, table_name, data):
        """Load a batch of data into the database."""
        try:
            load_batch_into_database(data, self.db, table_name)
        except Exception as e:
            logging.error(f"Error loading batch into table {table_name}: {e}")
            raise