
"""
Data ingestion module for loading raw CSV data into staging table
"""

import json
import ast
from typing import Optional
import pandas as pd
from tqdm import tqdm
from loguru import logger

from database import DatabaseConfig # pylint: disable=import-error


class DataIngestion:
    """Handle data ingestion from CSV to database staging table"""

    def __init__(self, db_config:DatabaseConfig):
        """Initialize data ingestion"""
        self.db_config = db_config


    def read_csv(self, filepath:str) -> pd.DataFrame:
        """Read CSV file with proper handling of complex columns"""
        logger.info(f"Reading CSV file: {filepath}")

        # Define converters for semi-structured columns
        converters = {
            'job_skills': self._parse_list_column,
            'job_type_skills': self._parse_dict_column
        }

        df = pd.read_csv(filepath, converters=converters)

        logger.info(f"Loaded {len(df)} rows from CSV")
        logger.info(f"Columns: {list(df.columns)}")

        return df


    @staticmethod
    def _parse_list_column(value) -> Optional[list]:
        """Parse string representation of list to actual list"""
        if pd.isna(value) or value == '' or value == '[]':
            return None
        try:
            parsed = ast.literal_eval(value)
            return parsed if isinstance(parsed, list) else None
        except (ValueError, SyntaxError):
            return None


    @staticmethod
    def _parse_dict_column(value) -> Optional[dict]:
        """Parse string representation of dict to actual dict"""
        if pd.isna(value) or value == '' or value == '{}':
            return None
        try:
            parsed = ast.literal_eval(value)
            return parsed if isinstance(parsed, dict) else None
        except (ValueError, SyntaxError):
            return None


    def load_to_staging(self, df:pd.DataFrame, batch_size:int=10000):
        """Load DataFrame to staging table"""
        logger.info(f"Loading {len(df)} rows to staging table")

        # Convert Python objects to JSON strings for JSONB columns
        def to_json_or_none(x): # pragma: no cover
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            return json.dumps(x)
        # Convert Python objects to ARRAYS for TEXT[] columns
        def normalize_skills(x): # pragma: no cover
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            if isinstance(x, list):
                return x
            if isinstance(x, str):
                try:
                    return json.loads(x)  # por si viene como string JSON
                except Exception: # pylint: disable=broad-exception-caught
                    return None
            return None

        df_copy = df.copy()
        df_copy["job_skills"] = df_copy["job_skills"].apply(normalize_skills)
        df_copy["job_type_skills"] = df_copy["job_type_skills"].apply(to_json_or_none)

        # Convert boolean strings to actual booleans
        bool_columns = ['job_work_from_home', 'job_no_degree_mention', 'job_health_insurance']
        for col in bool_columns:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].apply(
                    lambda x: True if str(x).lower() == 'true' \
                        else False if str(x).lower() == 'false' else None
                )

        if 'job_posted_date' in df_copy.columns:
            df_copy['job_posted_date'] = pd.to_datetime(df_copy['job_posted_date'], errors='coerce')

        # Load to database
        engine = self.db_config.get_engine()
        with tqdm(total=len(df_copy), desc="Progress") as pbar:
            for i in range(0, len(df_copy), batch_size):
                chunk = df_copy.iloc[i : i + batch_size]
                chunk.to_sql(
                    'staging_jobs', 
                    engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                pbar.update(len(chunk))

        logger.info(f"Successfully loaded {len(df)} rows to staging table")


    def run_ingestion(self, csv_filepath:str): # pragma: no cover
        """Run complete ingestion process"""
        logger.info("Starting data ingestion process")

        df = self.read_csv(csv_filepath)

        self.load_to_staging(df)

        logger.info("Data ingestion completed successfully")
