
"""
Data validation module using Pandera for data quality checks
"""

from typing import Optional
from loguru import logger
import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema




NON_HASHABLE_COLS = ['job_skills', 'job_type_skills']


class DataValidator:
    """Validate data quality using Pandera schemas"""

    raw_data_schema = DataFrameSchema(
        {
            "job_title_short": Column(str, nullable=True),
            "job_title": Column(str, nullable=True),
            "job_location": Column(str, nullable=True),
            "job_via": Column(str, nullable=True),
            "job_schedule_type": Column(str, nullable=True),
            "job_work_from_home": Column(object, nullable=True),
            "search_location": Column(str, nullable=True),
            "job_posted_date": Column(object, nullable=True),
            "job_no_degree_mention": Column(object, nullable=True),
            "job_health_insurance": Column(object, nullable=True),
            "job_country": Column(str, nullable=True),
            "salary_rate": Column(str, nullable=True,
                                checks=Check.isin(['hour', 'day', 'week', 'month', 'year', None])),
            "salary_year_avg": Column(float, nullable=True,
                                    checks=Check.greater_than_or_equal_to(0)),
            "salary_hour_avg": Column(float, nullable=True,
                                    checks=Check.greater_than_or_equal_to(0)),
            "company_name": Column(str, nullable=True),
            "job_skills": Column(object, nullable=True),
            "job_type_skills": Column(object, nullable=True),
        },
        strict=False,
        coerce=True
    )


    @classmethod
    def validate_raw_data(cls, df:pd.DataFrame) -> tuple[bool, Optional[str]]:
        """Validate raw CSV data"""
        try:
            logger.info("Validating raw data")
            _ = cls.raw_data_schema.validate(df, lazy=True)
            logger.info("Raw data validation passed")
            return True, None
        except pa.errors.SchemaErrors as e:
            error_msg = f"Schema validation failed:\n{e}"
            return False, error_msg
        except Exception as e: # pylint: disable=broad-exception-caught
            error_msg = f"Validation error: {e}"
            return False, error_msg


    @staticmethod
    def check_data_quality(df:pd.DataFrame) -> dict:
        """Perform data quality checks and return statistics"""
        logger.info("Performing data quality checks")

        check_cols = df.columns.difference(NON_HASHABLE_COLS).tolist()

        quality_metrics = {
            'total_rows': len(df),
            'duplicate_rows': int(df.duplicated(subset=check_cols).sum()),
            'missing_values': {},
            'unique_values': {},
            'data_types': {}
        }

        # Missing values per column
        for col in check_cols:
            missing_count = df[col].isna().sum()
            missing_pct = (missing_count / len(df)) * 100
            quality_metrics['missing_values'][col] = {
                'count': int(missing_count),
                'percentage': round(missing_pct, 2)
            }

        # Unique values for key columns
        key_columns = ['job_title_short', 'company_name', 'job_country', 'job_schedule_type']
        for col in key_columns:
            if col in check_cols:
                quality_metrics['unique_values'][col] = int(df[col].nunique())

        # Data types
        for col in check_cols:
            quality_metrics['data_types'][col] = str(df[col].dtype)

        logger.info(f"Data quality metrics:\n{quality_metrics}")


    @staticmethod
    def validate_skills_structure(df:pd.DataFrame) -> dict:
        """Validate structure of job_skills and job_type_skills columns"""
        logger.info("Validating skills structure")

        results = {
            'job_skills': {
                'total_non_null': 0,
                'valid_lists': 0,
                'invalid': 0,
                'sample_values': []
            },
            'job_type_skills': {
                'total_non_null': 0,
                'valid_dicts': 0,
                'invalid': 0,
                'sample_categories': set()
            }
        }

        # Validate job_skills (should be lists)
        if 'job_skills' in df.columns:
            non_null = df['job_skills'].notna()
            results['job_skills']['total_non_null'] = int(non_null.sum())

            for skill_list in df[non_null]['job_skills'].head(10):
                if isinstance(skill_list, list):
                    results['job_skills']['valid_lists'] += 1
                    if len(results['job_skills']['sample_values']) < 3:
                        results['job_skills']['sample_values'].append(skill_list[:3])
                else:
                    results['job_skills']['invalid'] += 1

        # Validate job_type_skills (should be dicts)
        if 'job_type_skills' in df.columns:
            non_null = df['job_type_skills'].notna()
            results['job_type_skills']['total_non_null'] = int(non_null.sum())

            for skill_dict in df[non_null]['job_type_skills'].head(100):
                if isinstance(skill_dict, dict):
                    results['job_type_skills']['valid_dicts'] += 1
                    results['job_type_skills']['sample_categories'].update(skill_dict.keys())
                else:
                    results['job_type_skills']['invalid'] += 1

        results['job_type_skills']['sample_categories'] = list(
            results['job_type_skills']['sample_categories']
        )

        logger.info(f"Skills validation:\n{results}")


    @staticmethod
    def generate_data_profile(df:pd.DataFrame) -> str:
        """Generate a text profile of the dataset"""
        profile = []
        profile.append("=" * 100)
        profile.append("DATA PROFILE")
        profile.append("=" * 100)

        # Basic info
        profile.append(f"\nDataset Shape: {df.shape[0]} rows × {df.shape[1]} columns")
        profile.append(f"Memory Usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

        # Column info
        profile.append("\nColumn Information:")
        profile.append("-" * 60)
        for col in df.columns.difference(NON_HASHABLE_COLS).tolist():
            dtype = df[col].dtype
            null_count = df[col].isna().sum()
            null_pct = (null_count / len(df)) * 100
            unique_count = df[col].nunique()

            profile.append(
                f"{col:25s} | {str(dtype):10s} | "
                f"Nulls: {null_count:6d} ({null_pct:5.1f}%) | "
                f"Unique: {unique_count:6d}"
            )

        profile.append("\n" + "=" * 100)
        profile_str = "\n".join(profile)

        logger.info(f"Data profile:\n{profile_str}")
