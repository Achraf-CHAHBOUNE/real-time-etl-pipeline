import time
import os
from utils.db_utils import get_table_names, fetch_new_data, bulk_insert_into_destination, load_last_dates, save_last_dates
from utils.kafka_utils import send_to_kafka
from utils.config import FIRST_MYSQL_DB, SECOND_MYSQL_DB

# Initialize last extracted timestamps
last_dates = load_last_dates()

def extract_and_load():
    databases = [FIRST_MYSQL_DB, SECOND_MYSQL_DB]

    for db_name in databases:
        tables = get_table_names(db_name)

        for table in tables:
            last_date = last_dates.get(table, "2000-01-01")  # Default to old date

            while True:
                data, last_date = fetch_new_data(db_name, table, last_date)
                if not data:
                    break  # No more data, exit loop

                bulk_insert_into_destination(table, data)

                # Send to Kafka
                for record in data:
                    send_to_kafka(db_name, table, record)

                last_dates[table] = last_date  # Update last processed date
                save_last_dates(last_dates)  # Save progress

def main():
    while True:
        extract_and_load()
        time.sleep(30)  # Poll every 30 seconds

if __name__ == "__main__":
    main()
