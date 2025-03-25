from extractor import Extractor
from loader import Loader
from config import SOURCE_CONFIG, DESTINATION_CONFIG
from tools import load_last_extracted, save_last_extracted, connect_database

class Orchestrator:
    def __init__(self):
        self.extractor = Extractor(SOURCE_CONFIG)
        self.loader = Loader(DESTINATION_CONFIG)
        self.batch_size = 5000  # Define batch size here or in config

    def get_total_rows(self, table, db_connection):
        """Get the total number of rows in the source table."""
        cursor = db_connection.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total_rows = cursor.fetchone()[0]
            print(f"ℹ️ Total rows in table '{table}': {total_rows}")
            return total_rows
        except Exception as e:
            print(f"❌ Error fetching row count for table {table}: {e}")
            raise
        finally:
            cursor.close()

    def process_table_completely(self, table):
        """Process a single table completely before moving to the next."""
        offset = 0
        total_extracted = 0
        last_extracted_info = load_last_extracted()
        
        # Check if this table was partially processed before
        if table in last_extracted_info and "offset" in last_extracted_info[table]:
            offset = last_extracted_info[table]["offset"]
            total_extracted = offset
            print(f"ℹ️ Resuming extraction for '{table}' from offset {offset}")

        # Get total rows to ensure completion
        source_db = connect_database(SOURCE_CONFIG)
        total_rows = self.get_total_rows(table, source_db)

        while True:
            data = self.extractor.extract_table_data(table, offset, self.batch_size)
            print(f"🔄 Processing table '{table}' at offset {offset}")
            
            if not data:
                print(f"✅ No more data to process for table '{table}'")
                break

            self.loader.load_batch_into_database(table, data)
            offset += len(data)
            total_extracted += len(data)

            # Update the last extracted offset in the tracking file
            last_extracted_info[table] = {
                "offset": offset,
                "total_extracted": total_extracted,
                "total_rows": total_rows
            }
            save_last_extracted(last_extracted_info)
            print(f"ℹ️ Progress: Extracted {total_extracted}/{total_rows} rows from '{table}'")

            # Check if we've extracted everything
            if total_extracted >= total_rows:
                print(f"✅ Table '{table}' fully extracted ({total_extracted}/{total_rows} rows)")
                break

        # Once complete, mark the table as fully processed
        last_extracted_info[table]["completed"] = True
        save_last_extracted(last_extracted_info)
        source_db.close()

    def process_orchestration(self):
        """Orchestrate the extraction and loading process, ensuring complete table extraction."""
        try:
            # Process table names only once (initial run) or skip if already done
            tables = self.extractor.process_tables_names()
            last_extracted_info = load_last_extracted()

            for table in tables:
                # Skip if table is already fully processed
                if table in last_extracted_info and last_extracted_info[table].get("completed", False):
                    print(f"ℹ️ Skipping table '{table}' - already fully processed")
                    continue
                
                print(f"🔄 Starting full extraction for table '{table}'")
                self.process_table_completely(table)
        
        except Exception as e:
            print(f"❌ Error during orchestration: {e}")
            raise

if __name__ == "__main__":
    orchestrator = Orchestrator()
    orchestrator.process_orchestration()