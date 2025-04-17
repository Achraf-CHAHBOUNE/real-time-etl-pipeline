import pandas as pd
import logging
from typing import Dict, List, Any
from config import SOURCE_DB_CONFIG, DEST_DB_CONFIG, KPI_FORMULAS_5MIN, NOEUD_PATTERN_5_15, files_paths, SUFFIX_OPERATOR_MAPPING, KPI_FAMILIES
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
        self.kpi_families = KPI_FAMILIES
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
            create_tables(self.dest_cursor, self.kpi_formulas, self.kpi_families)
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
            logging.info(f"Extracted {len(dates)} distinct dates from {table}")
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

    def filter_indicateur_values(self, table: str, date: str, kpi: str = None, family: str = None) -> pd.DataFrame:
        """Filter indicateur values for a specific KPI or family and date from the source database."""
        if family:
            # Fetch all counters for the family
            kpis = self.kpi_families[family]
            prefixes = set()
            for k in kpis:
                config = self.kpi_formulas[k]
                prefixes.update(config.get('numerator', []) + config.get('denominator', []) + config.get('additional', []))
            prefixes = list(prefixes)
        else:
            # Fetch counters for a single KPI
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
            if df.empty:
                logging.warning(f"No data found for {kpi or family} on {date} in {table}")
            else:
                logging.info(f"Filtered {len(df)} indicateur values for {kpi or family} on {date} from {table}")
            return df
        except Exception as e:
            logging.error(f"Error filtering indicateur values for {kpi or family} from {table}: {e}")
            raise

    def group_by_suffix(self, df: pd.DataFrame, kpi: str) -> List[Dict[str, Any]]:
        """Group filtered data by suffix if applicable, with type logic for families."""
        kpi_config = self.kpi_formulas[kpi]
        is_family_kpi = kpi_config.get('family') in self.kpi_families
        if not kpi_config.get('Suffix', False):
            return [{'suffix': '', 'type': None, 'values': self.calculate_group_values(df, kpi_config)}]

        grouped = {}
        for _, row in df.iterrows():
            prefix, suffix = extract_indicateur_suffixe(row['indicateur'])
            if not suffix:
                logging.warning(f"No suffix found for indicateur: {row['indicateur']}")
                continue

            # For family KPIs, determine if counters end with I or O
            if is_family_kpi:
                counters = kpi_config.get('numerator', []) + kpi_config.get('denominator', []) + kpi_config.get('additional', [])
                has_input = any(c.endswith('I') for c in counters if c == prefix)
                has_output = any(c.endswith('O') for c in counters if c == prefix)
                
                # Determine type and adjust suffix based on KPI
                if kpi == 'TRAF_Erlang_S':
                    # TRAF_Erlang_S uses TrunkrouteNTRALACCO (ends with 'O')
                    kpi_type = 'S'
                    # Use second part of suffix if hyphenated, else keep suffix
                    if '-' in suffix:
                        parts = suffix.split('-')
                        if len(parts) == 2:
                            new_suffix = parts[1]
                        else:
                            logging.warning(f"Invalid hyphenated suffix for {kpi}: {suffix}")
                            continue
                    else:
                        new_suffix = suffix
                elif kpi == 'TRAF_Erlang_E':
                    # TRAF_Erlang_E uses TrunkrouteNTRALACCI (ends with 'I')
                    kpi_type = 'E'
                    # Use first part of suffix if hyphenated, else keep suffix
                    if '-' in suffix:
                        parts = suffix.split('-')
                        if len(parts) == 2:
                            new_suffix = parts[0]
                        else:
                            logging.warning(f"Invalid hyphenated suffix for {kpi}: {suffix}")
                            continue
                    else:
                        new_suffix = suffix
                else:
                    # Other family KPIs (e.g., TRAF_RDT) may use both I and O
                    if has_input and not has_output:
                        kpi_type = 'E'
                        # Use first part of suffix
                        if '-' in suffix:
                            parts = suffix.split('-')
                            if len(parts) == 2:
                                new_suffix = parts[0]
                            else:
                                logging.warning(f"Invalid hyphenated suffix for {kpi}: {suffix}")
                                continue
                        else:
                            new_suffix = suffix
                    elif has_output and not has_input:
                        kpi_type = 'S'
                        # Use second part of suffix
                        if '-' in suffix:
                            parts = suffix.split('-')
                            if len(parts) == 2:
                                new_suffix = parts[1]
                            else:
                                logging.warning(f"Invalid hyphenated suffix for {kpi}: {suffix}")
                                continue
                        else:
                            new_suffix = suffix
                    else:
                        # Both I and O or neither (use full suffix)
                        kpi_type = 'E-S'
                        new_suffix = suffix
            else:
                kpi_type = None
                new_suffix = suffix

            # Validate suffix
            if not new_suffix or new_suffix == 'M':  # Handle invalid suffixes like 'M'
                logging.warning(f"Skipping invalid suffix for {kpi}: {suffix} (new_suffix: {new_suffix})")
                continue

            if new_suffix not in grouped:
                grouped[new_suffix] = {
                    'type': kpi_type,
                    'numerator': [],
                    'denominator': [],
                    'additional': []
                }
            
            if prefix in kpi_config.get('numerator', []):
                grouped[new_suffix]['numerator'].append(float(row['valeur']))
            elif prefix in kpi_config.get('denominator', []):
                grouped[new_suffix]['denominator'].append(float(row['valeur']))
            elif prefix in kpi_config.get('additional', []):
                grouped[new_suffix]['additional'].append(float(row['valeur']))
        
        # Convert to list of dictionaries
        result = [
            {'suffix': suffix, 'type': data['type'], 'values': {
                'numerator': data['numerator'],
                'denominator': data['denominator'],
                'additional': data['additional']
            }} for suffix, data in grouped.items()
        ]
        
        logging.info(f"Grouped data by suffix for {kpi}: {[item['suffix'] for item in result]}")
        return result

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
            logging.info(f"Calculated {kpi} value: {result}, numerator={group_values['numerator']}, denominator={group_values.get('denominator', [])}, additional={group_values.get('additional', [])}")
            return result
        except ZeroDivisionError:
            logging.warning(f"ZeroDivisionError calculating {kpi}: denominator={group_values.get('denominator', [])}")
            return None
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

    def insert_kpi_details(self, kpi: str, kpi_id: int, suffix: str, group_values: Dict[str, List[float]], kpi_value: float, kpi_type: str = None):
        """Insert into KPI details table in the destination database, including operator and type."""
        kpi_config = self.kpi_formulas[kpi]
        table_name = f"{kpi_config.get('family', kpi).lower()}_details"
        
        # Use a dictionary to store column-value pairs
        column_value_map = {"kpi_id": kpi_id}
        if kpi_config.get('family'):
            column_value_map["kpi"] = kpi
        
        if kpi_config.get('Suffix', False) and suffix:
            column_value_map["suffix"] = suffix
            # Parse suffix to determine operator
            normalized_suffix = suffix.lower()
            operator = "Unknown"
            # Special case: NW and IE/IS means Inwi International
            if 'nw' in normalized_suffix and ('ie' in normalized_suffix or 'is' in normalized_suffix):
                operator = "Inwi International"
            else:
                # Fallback: check for other operator codes
                for op_suffix in SUFFIX_OPERATOR_MAPPING.keys():
                    if op_suffix in normalized_suffix:
                        operator = SUFFIX_OPERATOR_MAPPING[op_suffix]
                        break
            column_value_map["operator"] = operator
            if operator == "Unknown":
                logging.warning(f"No known operator found in suffix: {suffix} (normalized: {normalized_suffix})")
        else:
            column_value_map["operator"] = None

        # Add type for family-based tables
        if kpi_config.get('family'):
            column_value_map["type"] = kpi_type if kpi_type else 'E-S'  # Default to 'E-S' if type not specified

        # Aggregate values for each unique column
        for field in ['numerator', 'denominator', 'additional']:
            if field in kpi_config:
                for i, prefix in enumerate(kpi_config[field]):
                    value = sum(group_values[field][i:i+1]) if i < len(group_values[field]) else 0
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
            operator_log = column_value_map.get('operator', 'None')
            type_log = column_value_map.get('type', 'None')
            logging.info(f"Inserted into {table_name}: kpi_id={kpi_id}, suffix={suffix}, operator={operator_log}, type={type_log}, columns={columns}")
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
                
                # Process family-based KPIs (Traffic)
                for family, kpis in self.kpi_families.items():
                    df = self.filter_indicateur_values(table, date, family=family)
                    for kpi in kpis:
                        # Filter df for this KPI's counters
                        kpi_config = self.kpi_formulas[kpi]
                        prefixes = kpi_config.get('numerator', []) + kpi_config.get('denominator', []) + kpi_config.get('additional', [])
                        kpi_df = df[df['indicateur'].str.startswith(tuple(prefixes))]
                        grouped_data = self.group_by_suffix(kpi_df, kpi)
                        
                        for group in grouped_data:
                            suffix = group['suffix']
                            group_values = group['values']
                            kpi_type = group.get('type')
                            kpi_value = self.calculate_kpi(kpi, group_values)
                            self.insert_kpi_details(kpi, kpi_summary_id, suffix, group_values, kpi_value, kpi_type)

                # Process non-family KPIs
                for kpi in self.kpi_formulas.keys():
                    if self.kpi_formulas[kpi].get('family') in self.kpi_families:
                        continue  # Skip KPIs already processed in family
                    df = self.filter_indicateur_values(table, date, kpi=kpi)
                    grouped_data = self.group_by_suffix(df, kpi)
                    
                    for group in grouped_data:
                        suffix = group['suffix']
                        group_values = group['values']
                        kpi_type = group.get('type')
                        kpi_value = self.calculate_kpi(kpi, group_values)
                        self.insert_kpi_details(kpi, kpi_summary_id, suffix, group_values, kpi_value, kpi_type)

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