from tools import connect_database, process_tables_names, store_txt, extract_table_data
from config import patterns, start_year

class Extractor:
    def __init__(self, config):
        self.config = config
        self.db = None
        self.cursor = None
        self.tables = None
        self.connect()

    def connect(self):
        """Connect to the database."""
        try:
            self.db = connect_database(self.config)
            self.cursor = self.db.cursor()
        except Exception as e:
            print(f"❌ Failed to connect to the database: {e}")
            raise

    def extract_tables_names(self):
        """Extract all table names from the database and store them in a file."""
        try:
            self.cursor.execute("SHOW TABLES")
            tables = [table[0] for table in self.cursor.fetchall()]  # Extract table names from tuples
            tables_file_path = "./data/our_tables/tables.txt"
            store_txt(tables, tables_file_path)
            self.tables = tables
        except Exception as e:
            print(f"❌ Error extracting table names: {e}")
            raise

    def process_tables_names(self):
        """Process table names by filtering and sorting them."""
        try:
            self.extract_tables_names()
            tables_names = process_tables_names(self.tables, patterns, start_year)
            print(f"the files are : {tables_names}")
            return tables_names
        except Exception as e:
            print(f"❌ Error processing table names: {e}")
            raise

    def extract_table_data(self, table_name, offset, batch_size=5000):
        """Extract data from a specific table in batches."""
        try:
            data = extract_table_data(table_name, self.cursor, offset, batch_size)
            return data
        except Exception as e:
            print(f"❌ Error extracting data from table {table_name}: {e}")
            raise