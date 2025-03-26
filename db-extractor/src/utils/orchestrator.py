import logging
from extractor import Extractor
from loader import Loader
from config import SOURCE_CONFIG, DESTINATION_CONFIG
from tools import load_last_extracted, save_last_extracted, connect_database

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Orchestrator:
    def __init__(self):
        self.extractor = Extractor(SOURCE_CONFIG)
        self.loader = Loader(DESTINATION_CONFIG)
        self.batch_size = 5000

    def get_total_rows(self, table, db_connection):
        """Get the total number of rows in the source table."""
        cursor = db_connection.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total_rows = cursor.fetchone()[0]
            logging.info(f"Total rows in table '{table}': {total_rows}")
            return total_rows
        except Exception as e:
            logging.error(f"Error fetching row count for table {table}: {e}")
            raise
        finally:
            cursor.close()

    def process_table_completely(self, table):
        """Process a single table completely before moving to the next."""
        offset = 0
        total_extracted = 0
        last_extracted_info = load_last_extracted()
        
        if table in last_extracted_info and "offset" in last_extracted_info[table]:
            offset = last_extracted_info[table]["offset"]
            total_extracted = offset
            logging.info(f"Resuming extraction for '{table}' from offset {offset}")

        source_db = connect_database(SOURCE_CONFIG)
        total_rows = self.get_total_rows(table, source_db)

        while True:
            data = self.extractor.extract_table_data(table, offset, self.batch_size)
            logging.info(f"Processing table '{table}' at offset {offset}")
            
            if not data:
                logging.info(f"No more data to process for table '{table}'")
                break

            self.loader.load_batch_into_database(table, data)
            offset += len(data)
            total_extracted += len(data)

            percentage = (total_extracted / total_rows) * 100 if total_rows > 0 else 0
            last_extracted_info[table] = {
                "offset": offset,
                "total_extracted": total_extracted,
                "total_rows": total_rows,
                "percentage": round(percentage, 2)
            }
            save_last_extracted(last_extracted_info)
            logging.info(f"Progress: Extracted {total_extracted}/{total_rows} rows ({percentage:.2f}%) from '{table}'")

            if total_extracted >= total_rows:
                logging.info(f"Table '{table}' fully extracted ({total_extracted}/{total_rows} rows)")
                break

        last_extracted_info[table]["completed"] = True
        save_last_extracted(last_extracted_info)
        source_db.close()

    def process_orchestration(self):
        """Orchestrate the extraction and loading process."""
        try:
            tables = self.extractor.process_tables_names()
            last_extracted_info = load_last_extracted()

            for table in tables:
                if table in last_extracted_info and last_extracted_info[table].get("completed", False):
                    logging.info(f"Skipping table '{table}' - already fully processed")
                    continue
                
                logging.info(f"Starting full extraction for table '{table}'")
                self.process_table_completely(table)
        
        except Exception as e:
            logging.error(f"Error during orchestration: {e}")
            raise

if __name__ == "__main__":
    orchestrator = Orchestrator()
    orchestrator.process_orchestration()