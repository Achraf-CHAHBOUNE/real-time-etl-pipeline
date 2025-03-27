import logging
import re
from collections import defaultdict
from tools import connect_database, load_txt
from config import DESTINATION_CONFIG, KPI_FORMULAS_5MIN, NOEUD_PATTERN

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Transformer:
    def __init__(self, table_list_file):
        self.db = connect_database(DESTINATION_CONFIG)
        self.cursor = self.db.cursor()
        self.table_list_file = table_list_file

    def load_tables(self):
        return load_txt(self.table_list_file)

    def extract_noeud(self, table):
        match = NOEUD_PATTERN.match(table)
        return match.group(1).upper() if match else "UNKNOWN"

    def parse_indicateur(self, indicateur):
        match = re.match(r'^(.*?)\.(.+)$', indicateur)
        if match:
            return match.group(1), match.group(2)
        return indicateur, None

    def group_by_timestamp_and_suffix(self, table):
        grouped = defaultdict(lambda: defaultdict(dict))
        query = f"SELECT Date, indicateur, valeur FROM {table}"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for date_heure, indicateur, valeur in rows:
            timestamp = str(date_heure)
            base_indicateur, suffix = self.parse_indicateur(indicateur)
            grouped[(timestamp, suffix)][base_indicateur] = valeur
        return grouped

    def calculate_kpis(self, grouped_data, noeud):
        transformed = []
        for (timestamp, suffix), indicators in grouped_data.items():
            kpi_results = {"date_heure": timestamp, "Noeud": noeud, "suffix": suffix}
            # Store original KPI values
            for indicateur, valeur in indicators.items():
                kpi_results[indicateur] = valeur
            # Calculate new KPIs
            for kpi_name, kpi_config in KPI_FORMULAS_5MIN.items():
                # Handle different formula structures
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

    def save_transformed_data(self, table, transformed_data):
        target_table = f"transformed_{table}"
        self.cursor.execute(f"SHOW TABLES LIKE '{target_table}'")
        if not self.cursor.fetchone():
            columns = ["Date DATETIME", "Noeud VARCHAR(255)", "suffix VARCHAR(255)"]
            # Add columns for original KPIs (indicateurs)
            query = f"SELECT DISTINCT indicateur FROM extracted_{table}"
            self.cursor.execute(query)
            indicateurs = [row[0] for row in self.cursor.fetchall()]
            base_indicateurs = set(self.parse_indicateur(ind)[0] for ind in indicateurs)
            for ind in base_indicateurs:
                columns.append(f"`{ind}` FLOAT")
            # Add columns for new KPIs
            for kpi_name in KPI_FORMULAS_5MIN.keys():
                columns.append(f"`{kpi_name}` FLOAT")
            create_query = f"CREATE TABLE {target_table} ({', '.join(columns)})"
            self.cursor.execute(create_query)
            self.db.commit()
            logging.info(f"Created table {target_table}")

        columns = ["Date", "Noeud", "suffix"] + list(base_indicateurs) + list(KPI_FORMULAS_5MIN.keys())
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {target_table} ({', '.join(columns)}) VALUES ({placeholders})"
        data_to_insert = [
            (row["date_heure"], row["Noeud"], row["suffix"], *[row.get(ind, None) for ind in base_indicateurs], *[row.get(kpi, None) for kpi in KPI_FORMULAS_5MIN.keys()])
            for row in transformed_data
        ]
        self.cursor.executemany(insert_query, data_to_insert)
        self.db.commit()
        logging.info(f"Inserted {len(transformed_data)} rows into {target_table}")

    def transform(self, table):
        noeud = self.extract_noeud(table)
        grouped_data = self.group_by_timestamp_and_suffix(table)
        transformed_data = self.calculate_kpis(grouped_data, noeud)
        self.save_transformed_data(table, transformed_data)

    def run(self):
        tables = self.load_tables()
        for table in tables:
            self.transform(table)