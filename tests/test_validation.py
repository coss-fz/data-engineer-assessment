
"""
Unit tests for ingestion module
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from src.validation import DataValidator # pylint: disable=wrong-import-position




class TestDataValidator:
    """Tests for DataValidator class"""

    @pytest.fixture
    def fake_df(self):
        """Fixture to simulate a valid raw DataFrame"""
        return pd.DataFrame({
            "job_title_short": ["Data Engineer", "Analyst"],
            "job_title": ["Senior Data Engineer", "Junior Analyst"],
            "job_location": ["Remote", "New York, NY"],
            "job_via": ["via LinkedIn", "via Indeed"],
            "job_schedule_type": ["Full-time", "Part-time"],
            "job_work_from_home": [True, False],
            "search_location": ["Worldwide", "USA"],
            "job_posted_date": ["2023-01-01", "2023-01-02"],
            "job_no_degree_mention": [True, True],
            "job_health_insurance": [False, True],
            "job_country": ["United States", "UK"],
            "salary_rate": ["year", "hour"],
            "salary_year_avg": [120000.0, 100000.0],
            "salary_hour_avg": [60.0, 45.0],
            "company_name": ["Tech Corp", "Data Inc"],
            "job_skills": [["python", "sql"], ["r"]],
            "job_type_skills": [
                {"programming": ['python','r']}, {'databases': ['mongodb', 'sql server']}
            ]
        })


    def test_validate_raw_data_success(self, fake_df):
        """Test for a successfull DF validation"""
        success, error = DataValidator.validate_raw_data(fake_df)
        assert success is True
        assert error is None


    def test_validate_raw_data_invalid_salary_rate(self, fake_df):
        """Test failure for invalid salary rate"""
        fake_df.loc[0, "salary_rate"] = "century"
        success, error = DataValidator.validate_raw_data(fake_df)
        assert success is False
        assert "salary_rate" in error
    

    def test_validate_raw_data_negative_salary(self, fake_df):
        """Test failure for negative salary"""
        fake_df.loc[0, "salary_year_avg"] = -24000.0
        success, error = DataValidator.validate_raw_data(fake_df)
        assert success is False
        assert "salary_year_avg" in error
    

    def test_check_data_quality(self, fake_df):
        """Test NULL and duplicates calculations"""
        df_with_issues = pd.concat([fake_df, fake_df.iloc[[0]]], ignore_index=True)
        df_with_issues.loc[1, "company_name"] = np.nan

        metrics = DataValidator.check_data_quality(df_with_issues)

        assert metrics['total_rows'] == 3
        assert metrics['duplicate_rows'] == 1
        assert metrics['missing_values']['company_name']['count'] == 1
        assert metrics['unique_values']['job_title_short'] == 2


    def test_validate_skills_structure_mixed_data(self):
        """Test valid/invalida detection for lists/dictionaries"""
        df = pd.DataFrame({
            'job_skills': [['python'], 'not_a_list', np.nan],
            'job_type_skills': [{'cloud': ['aws', 'azure']}, 'not_a_dict', None]
        })
        results = DataValidator.validate_skills_structure(df)

        assert results['job_skills']['total_non_null'] == 2
        assert results['job_skills']['valid_lists'] == 1
        assert results['job_skills']['invalid'] == 1

        assert results['job_type_skills']['total_non_null'] == 2
        assert results['job_type_skills']['valid_dicts'] == 1
        assert 'cloud' in results['job_type_skills']['sample_categories']
