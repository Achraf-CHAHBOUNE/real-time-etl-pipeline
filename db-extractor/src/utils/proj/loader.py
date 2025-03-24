from tools import connect_database, load_batch_into_database

class Loader:
    def __init__(self, config):
        self.config = config
        self.db = None
        self.cursor = None
        self.connect()

    def connect(self):
        """Connect to the database."""
        try:
            self.db = connect_database(self.config)
            self.cursor = self.db.cursor()
        except Exception as e:
            print(f"❌ Failed to connect to the database: {e}")
            raise

    def load_batch_into_database(self, table_name, data):
        """Load a batch of data into the database."""
        try:
            load_batch_into_database(data, self.db, table_name)
        except Exception as e:
            print(f"❌ Error loading batch into table {table_name}: {e}")
            raise