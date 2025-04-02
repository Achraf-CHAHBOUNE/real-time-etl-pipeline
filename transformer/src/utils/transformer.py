import logging
import re
from collections import defaultdict
from tools import connect_database, load_txt
from config import DB_CONFIG, KPI_FORMULAS_5MIN, NOEUD_PATTERN_5_and_15, files_paths

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Transformer:
    def __init__(self):
        # Fix DB_CONFIG to use DB_NAME instead of DB_PORT
        self.db = connect_database(DB_CONFIG)
        self.cursor = self.db.cursor()
        self.target_table = "transformed_5min"
        self.create_transformed_table()

    def load_tables(self):
        """Load 5-minute tables from the Extractor's result_5min.txt."""
        # Adjust path to point to Extractor's output
        table_list_file = files_paths['5min']
        tables = load_txt(table_list_file)
        if not tables:
            logging.warning(f"No tables found in {table_list_file}")
        else:
            logging.info(f"Loaded {len(tables)} 5-minute tables: {tables}")
        return tables

    def extract_noeud(self, table):
        match = NOEUD_PATTERN_5_and_15.match(table)
        return match.group(1).upper() if match else "UNKNOWN"

    def parse_indicateur(self, indicateur):
        match = re.match(r'^(.*?)\.(.+)$', indicateur)
        if match:
            return match.group(1), match.group(2)
        return indicateur, None

    def group_by_timestamp_and_suffix(self, table):
        grouped = defaultdict(lambda: defaultdict(dict))
        query = f"SELECT Date, indicateur, valeur FROM {table}"
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            for date_heure, indicateur, valeur in rows:
                timestamp = str(date_heure)
                base_indicateur, suffix = self.parse_indicateur(indicateur)
                grouped[(timestamp, suffix)][base_indicateur] = valeur
            logging.info(f"Grouped {len(rows)} rows from {table}")
        except Exception as e:
            logging.error(f"Error querying {table}: {e}")
        return grouped

    def calculate_kpis(self, grouped_data, noeud):
        transformed = []
        for (timestamp, suffix), indicators in grouped_data.items():
            kpi_results = {"date_heure": timestamp, "Noeud": noeud, "suffix": suffix}
            # Store original indicator values
            for indicateur, valeur in indicators.items():
                kpi_results[indicateur] = valeur
            # Calculate KPIs
            for kpi_name, kpi_config in KPI_FORMULAS_5MIN.items():
                if "numerator" in kpi_config and "denominator" in kpi_config:
                    numerator_values = [indicators.get(ind, 0) for ind in kpi_config["numerator"]]
                    denominator_values = [indicators.get(ind, 0) for ind in kpi_config["denominator"]]
                    if not all(v is not None for v in numerator_values + denominator_values):
                        kpi_results[kpi_name] = None
                        continue
                    if "additional" in kpi_config:
                        additional_values = [indicators.get(ind, 0) for ind in kpi_config["additional"]]
                        kpi_value = kpi_config["formula"](numerator_values, denominator_values, additional_values)
                    else:
                        kpi_value = kpi_config["formula"](numerator_values, denominator_values)
                elif "numerator" in kpi_config:
                    numerator_values = [indicators.get(ind, 0) for ind in kpi_config["numerator"]]
                    if not all(v is not None for v in numerator_values):
                        kpi_results[kpi_name] = None
                        continue
                    kpi_value = kpi_config["formula"](numerator_values)
                else:
                    kpi_results[kpi_name] = None
                    continue
                kpi_results[kpi_name] = round(kpi_value, 2) if kpi_value is not None else None
            transformed.append(kpi_results)
        return transformed

    def create_transformed_table(self):
        """Create the transformed_5min table if it doesn't exist."""
        self.cursor.execute(f"SHOW TABLES LIKE '{self.target_table}'")
        if not self.cursor.fetchone():
            columns = [
                "Date DATETIME",
                "Noeud VARCHAR(255)",
                "suffix VARCHAR(255)"
            ]
            # Add all original indicators from KPI_FORMULAS_5MIN
            all_indicators = set()
            for kpi_config in KPI_FORMULAS_5MIN.values():
                all_indicators.update(kpi_config.get("numerator", []))
                all_indicators.update(kpi_config.get("denominator", []))
                all_indicators.update(kpi_config.get("additional", []))
            for ind in sorted(all_indicators):
                columns.append(f"`{ind}` FLOAT")
            # Add all KPI columns
            for kpi_name in KPI_FORMULAS_5MIN.keys():
                columns.append(f"`{kpi_name}` FLOAT")
            create_query = f"CREATE TABLE {self.target_table} ({', '.join(columns)})"
            self.cursor.execute(create_query)
            self.db.commit()
            logging.info(f"Created table {self.target_table}")

    def save_transformed_data(self, transformed_data):
        """Insert transformed data into transformed_5min."""
        all_indicators = set()
        for kpi_config in KPI_FORMULAS_5MIN.values():
            all_indicators.update(kpi_config.get("numerator", []))
            all_indicators.update(kpi_config.get("denominator", []))
            all_indicators.update(kpi_config.get("additional", []))
        columns = ["Date", "Noeud", "suffix"] + sorted(all_indicators) + list(KPI_FORMULAS_5MIN.keys())
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {self.target_table} ({', '.join(columns)}) VALUES ({placeholders})"
        data_to_insert = [
            (
                row["date_heure"], row["Noeud"], row["suffix"],
                *[row.get(ind, None) for ind in sorted(all_indicators)],
                *[row.get(kpi, None) for kpi in KPI_FORMULAS_5MIN.keys()]
            )
            for row in transformed_data
        ]
        self.cursor.executemany(insert_query, data_to_insert)
        self.db.commit()
        logging.info(f"Inserted {len(transformed_data)} rows into {self.target_table}")

    def transform(self, table):
        noeud = self.extract_noeud(table)
        grouped_data = self.group_by_timestamp_and_suffix(table)
        transformed_data = self.calculate_kpis(grouped_data, noeud)
        self.save_transformed_data(transformed_data)

    def run(self):
        tables = self.load_tables()
        for table in tables:
            logging.info(f"Transforming table: {table}")
            self.transform(table)
        logging.info("Transformation complete.")

if __name__ == "__main__":
    transformer = Transformer()
    transformer.run()