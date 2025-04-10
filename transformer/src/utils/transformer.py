import pandas as pd
import logging
from typing import Dict, List, Any
from config import SOURCE_DB_CONFIG, DEST_DB_CONFIG, KPI_FORMULAS_5MIN, NOEUD_PATTERN_5_15, files_paths
from tools import connect_database, create_tables, extract_noeud, extract_indicateur_suffixe

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Transformer:
    
    def __init__(self):
        self.source_conn = connect_database(SOURCE_DB_CONFIG)
        self.source_cursor = self.source_conn.cursor()
        self.dest_conn = connect_database(DEST_DB_CONFIG)
        self.dest_cursor = self.dest_conn.cursor()
        self.kpi_formulas = KPI_FORMULAS_5MIN
        self.noeud_pattern = NOEUD_PATTERN_5_15
        self.tables = self.load_tables()

    def load_tables(self) -> List[str]:
        """Load table names from result_5min.txt."""
        try:
            with open(files_paths['5min'], 'r') as f:
                tables = [line.strip() for line in f if line.strip()]
            logging.info(f"Loaded {len(tables)} tables from {files_paths['5min']}: {tables}")
            return tables
        except Exception as e:
            logging.error(f"Error loading tables from file: {e}")
            raise

    def create_tables(self):
        """Create tables in the destination database."""
        try:
            create_tables(self.dest_cursor, self.kpi_formulas)
            self.dest_conn.commit()
            logging.info("Tables created successfully in destination database.")
        except Exception as e:
            logging.error(f"Error creating tables in destination database: {e}")
            self.dest_conn.rollback()
            raise

    def get_distinct_dates(self, table: str) -> List[str]:
        """Retrieve distinct Date values from a table in the source database."""
        try:
            query = f"SELECT DISTINCT Date FROM {table}"
            self.source_cursor.execute(query)
            dates = [str(row[0]) for row in self.source_cursor.fetchall()]
            logging.info(f"Extracted {len(dates)} distinct dates from {table}: {dates}")
            return dates
        except Exception as e:
            logging.error(f"Error getting distinct dates from {table}: {e}")
            raise

    def extract_node(self, table: str) -> str:
        """Extract Node from table name."""
        matches = extract_noeud(self.noeud_pattern, [table])
        if matches:
            node = matches[0][1]
            logging.info(f"Extracted node '{node}' from table '{table}'")
            return node
        logging.warning(f"No node found in table name: {table}")
        return None

    def filter_indicateur_values(self, table: str, date: str, kpi: str) -> pd.DataFrame:
        """Filter indicateur values for a specific KPI and date from the source database."""
        kpi_config = self.kpi_formulas[kpi]
        prefixes = kpi_config.get('numerator', []) + kpi_config.get('denominator', []) + kpi_config.get('additional', [])
        
        try:
            query = f"""
                SELECT indicateur, valeur
                FROM {table}
                WHERE Date = %s AND ({' OR '.join(['indicateur LIKE %s' for _ in prefixes])})
            """
            params = [date] + [f"{prefix}%" for prefix in prefixes]
            self.source_cursor.execute(query, params)
            data = self.source_cursor.fetchall()
            
            df = pd.DataFrame(data, columns=['indicateur', 'valeur'])
            logging.info(f"Filtered {len(df)} indicateur values for {kpi} on {date} from {table}")
            return df
        except Exception as e:
            logging.error(f"Error filtering indicateur values for {kpi} from {table}: {e}")
            raise

    def group_by_suffix(self, df: pd.DataFrame, kpi: str) -> Dict[str, Dict[str, List[float]]]:
        """Group filtered data by suffix if applicable."""
        kpi_config = self.kpi_formulas[kpi]
        if not kpi_config.get('Suffix', False):
            return {'': self.calculate_group_values(df, kpi_config)}

        grouped = {}
        for _, row in df.iterrows():
            prefix, suffix = extract_indicateur_suffixe(row['indicateur'])
            if suffix:
                if suffix not in grouped:
                    grouped[suffix] = {'numerator': [], 'denominator': [], 'additional': []}
                if prefix in kpi_config.get('numerator', []):
                    grouped[suffix]['numerator'].append(float(row['valeur']))
                elif prefix in kpi_config.get('denominator', []):
                    grouped[suffix]['denominator'].append(float(row['valeur']))
                elif prefix in kpi_config.get('additional', []):
                    grouped[suffix]['additional'].append(float(row['valeur']))
        
        logging.info(f"Grouped data by suffix for {kpi}: {grouped}")
        return grouped

    def calculate_group_values(self, df: pd.DataFrame, kpi_config: Dict) -> Dict[str, List[float]]:
        """Calculate values for numerator, denominator, and additional fields."""
        result = {
            'numerator': [],
            'denominator': [],
            'additional': []
        }
        for _, row in df.iterrows():
            prefix, _ = extract_indicateur_suffixe(row['indicateur'])
            if prefix in kpi_config.get('numerator', []):
                result['numerator'].append(float(row['valeur']))
            elif prefix in kpi_config.get('denominator', []):
                result['denominator'].append(float(row['valeur']))
            elif prefix in kpi_config.get('additional', []):
                result['additional'].append(float(row['valeur']))
        return result

    def calculate_kpi(self, kpi: str, group_values: Dict[str, List[float]]) -> float:
        """Calculate KPI value using the formula."""
        kpi_config = self.kpi_formulas[kpi]
        formula = kpi_config['formula']
        
        try:
            if 'additional' in kpi_config:
                result = formula(group_values['numerator'], group_values['denominator'], group_values['additional'])
            elif 'denominator' in kpi_config:
                result = formula(group_values['numerator'], group_values['denominator'])
            else:
                result = formula(group_values['numerator'])
            logging.info(f"Calculated {kpi} value: {result}")
            return result
        except Exception as e:
            logging.error(f"Error calculating {kpi}: {e}")
            return None

    def insert_kpi_summary(self, date: str, node: str) -> int:
        """Insert into kpi_summary in the destination database and return the generated ID."""
        try:
            query = "INSERT INTO kpi_summary (Date, Node) VALUES (%s, %s)"
            self.dest_cursor.execute(query, (date, node))
            self.dest_conn.commit()
            self.dest_cursor.execute("SELECT LAST_INSERT_ID()")
            kpi_id = self.dest_cursor.fetchone()[0]
            logging.info(f"Inserted into kpi_summary: Date={date}, Node={node}, ID={kpi_id}")
            return kpi_id
        except Exception as e:
            logging.error(f"Error inserting into kpi_summary: {e}")
            self.dest_conn.rollback()
            raise

    def insert_kpi_details(self, kpi: str, kpi_id: int, suffix: str, group_values: Dict[str, List[float]], kpi_value: float):
        """Insert into KPI details table in the destination database, avoiding duplicate columns."""
        kpi_config = self.kpi_formulas[kpi]
        table_name = f"{kpi.lower()}_details"
        
        # Use a dictionary to store column-value pairs, ensuring uniqueness
        column_value_map = {"kpi_id": kpi_id}
        
        if kpi_config.get('Suffix', False) and suffix:
            column_value_map["suffix"] = suffix

        # Aggregate values for each unique column
        for field in ['numerator', 'denominator', 'additional']:
            if field in kpi_config:
                for i, prefix in enumerate(kpi_config[field]):
                    value = sum(group_values[field][i:i+1]) if i < len(group_values[field]) else 0
                    if prefix in column_value_map:
                        column_value_map[prefix] += value
                    else:
                        column_value_map[prefix] = value

        column_value_map["value"] = kpi_value

        # Convert to lists for SQL insertion
        columns = list(column_value_map.keys())
        values = list(column_value_map.values())
        params = ["%s"] * len(columns)

        try:
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(params)})"
            self.dest_cursor.execute(query, values)
            self.dest_conn.commit()
            logging.info(f"Inserted into {table_name}: kpi_id={kpi_id}, suffix={suffix}, columns={columns}, values={values}")
        except Exception as e:
            logging.error(f"Error inserting into {table_name}: {e}")
            self.dest_conn.rollback()
            raise

    def process(self):
        """Main process to handle all tables."""
        self.create_tables()
        
        for table in self.tables:
            node = self.extract_node(table)
            if not node:
                continue
                
            dates = self.get_distinct_dates(table)
            for date in dates:
                kpi_summary_id = self.insert_kpi_summary(date, node)
                
                for kpi in self.kpi_formulas.keys():
                    df = self.filter_indicateur_values(table, date, kpi)
                    grouped_data = self.group_by_suffix(df, kpi)
                    
                    for suffix, group_values in grouped_data.items():
                        kpi_value = self.calculate_kpi(kpi, group_values)
                        # Skip insertion if kpi_value is None
                        if kpi_value is not None:
                            self.insert_kpi_details(kpi, kpi_summary_id, suffix, group_values, kpi_value)
                        else:
                            logging.warning(f"Skipped insertion for {kpi} (suffix: {suffix}) due to None value")

    def __del__(self):
        """Cleanup database connections."""
        self.source_cursor.close()
        self.source_conn.close()
        self.dest_cursor.close()
        self.dest_conn.close()
        logging.info("Database connections closed.")

if __name__ == "__main__":
    transformer = Transformer()
    transformer.process()