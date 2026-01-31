
"""
Unit tests for ingestion module
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from src.ingestion import DataIngestion # pylint: disable=wrong-import-position




class TestDataIngestion:
    """Tests for DataIngestion class"""

    @pytest.fixture
    def ingestion(self):
        """Fixture to initialize class with a database mock config"""
        mock_db_config = MagicMock()
        return DataIngestion(mock_db_config)


    @patch('pandas.read_csv')
    def test_read_csv(self, mock_read_csv, ingestion):
        """Test CSV read with correct converters"""
        mock_df = pd.DataFrame({'job_skills': [None], 'job_type_skills': [None]})
        mock_read_csv.return_value = mock_df
        result = ingestion.read_csv("path.csv")

        _, kwargs = mock_read_csv.call_args
        assert isinstance(result, pd.DataFrame)
        assert 'converters' in kwargs
        assert 'job_skills' in kwargs['converters']
        assert 'job_type_skills' in kwargs['converters']


    @pytest.mark.parametrize("value, expected", [
        ("['Python', 'SQL']", ['Python', 'SQL']),
        ("[]", None),
        ("", None),
        (float('nan'), None),
        ("Invalid List", None),
    ])
    def test_parse_list_column(self, ingestion, value, expected):
        """Test conversion to list"""
        assert ingestion._parse_list_column(value) == expected # pylint: disable=protected-access


    @pytest.mark.parametrize("value, expected", [
        ("{'cloud': ['aws'], 'programming': ['r', 'python']}",
            {'cloud': ['aws'], 'programming': ['r', 'python']}),
        ("{}", None),
        (None, None),
        ("not a dict", None),
    ])
    def test_parse_dict_column(self, ingestion, value, expected):
        """Test conversion to dict"""
        assert ingestion._parse_dict_column(value) == expected # pylint: disable=protected-access


    @patch('pandas.DataFrame.to_sql')
    def test_load_to_staging(self, mock_to_sql, ingestion):
        """Test data cleaning and staging load"""
        data = {
            'job_skills': [['r', 'python']],
            'job_type_skills': [{'cloud': ['aws'], 'programming': ['r', 'python']}],
            'job_work_from_home': 'true',
            'job_no_degree_mention': 'FALSE',
            'job_posted_date': '2026-01-01'
        }

        mock_engine = MagicMock()
        ingestion.db_config.get_engine.return_value = mock_engine

        # Test data types transformation
        df = pd.DataFrame(data)
        captured_chunks = []

        def mock_to_sql_capture(self, *args, **kwargs): # pylint: disable=unused-argument
            captured_chunks.append(self.copy())

        with patch.object(pd.DataFrame, 'to_sql', mock_to_sql_capture):
            ingestion.load_to_staging(df, batch_size=1)
        chunk = captured_chunks[0]

        assert len(captured_chunks) == 1
        assert chunk['job_work_from_home'].iloc[0] == True # pylint: disable=singleton-comparison
        assert chunk['job_no_degree_mention'].iloc[0] == False # pylint: disable=singleton-comparison
        assert isinstance(chunk['job_posted_date'].iloc[0], pd.Timestamp)
        assert isinstance(chunk['job_type_skills'].iloc[0], str)
        assert chunk['job_skills'].iloc[0] == ['r', 'python']

        # Test data ingestion
        ingestion.load_to_staging(df, batch_size=1)
        name = mock_to_sql.call_args[0][0]

        assert name == 'staging_jobs'
        mock_to_sql.assert_called_once()
