
"""
Unit tests for tranformation module
"""

import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from src.transformation import DataTransformation # pylint: disable=wrong-import-position




class TestDataTransformation:
    """Tests for DataTransformation class"""

    @pytest.fixture
    def mock_db_config(self):
        """Fixture to mock the database connection"""
        db_config = MagicMock()
        mock_engine = MagicMock()
        db_config.get_engine.return_value = mock_engine
        return db_config, mock_engine


    def test_extract_location_components_full(self):
        """Test location parsing with full address"""
        db_config = Mock()
        transformer = DataTransformation(db_config)

        # Case 1
        location = "Russia"
        country = "Russia"
        result = transformer.extract_location_components(location, country)
        assert result['city'] == "Russia"
        assert result['state_province'] is None
        assert result['country'] == "Russia"

        # Case 2
        location = "San Francisco, CA"
        country = "United States"
        result = transformer.extract_location_components(location, country)
        assert result['city'] == "San Francisco"
        assert result['state_province'] == "CA"
        assert result['country'] == "United States"

        # Case 3
        location = "Rio de Janeiro, State of Rio de Janeiro, Brazil"
        country = "Brazil"
        result = transformer.extract_location_components(location, country)
        assert result['city'] == "Rio de Janeiro"
        assert result['state_province'] == "State of Rio de Janeiro"
        assert result['country'] == "Brazil"

        # Case 4
        result = transformer.extract_location_components("", "Colombia")
        assert result['city'] is None
        assert result['state_province'] is None
        assert result['country'] == "Colombia"

        # Case 5
        result = transformer.extract_location_components(None, "Germany")
        assert result['city'] is None
        assert result['state_province'] is None
        assert result['country'] == "Germany"
    

    def test_delete_previous_info(self, mock_db_config):
        """Test objects deletion"""
        db_config, mock_engine = mock_db_config
        transformer = DataTransformation(db_config)
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        transformer.delete_previous_info()

        calls = [call[0][0].text for call in mock_conn.execute.call_args_list]
        assert any("DELETE FROM job_skills" in s for s in calls)
        assert any("DELETE FROM schedule_types" in s for s in calls)
        assert not any("DELETE FROM jobs_staging" in s for s in calls)
        mock_conn.commit.assert_called_once()
    

    def test_populate_companies(self, mock_db_config):
        """Test companies insertion"""
        db_config, mock_engine = mock_db_config
        transformer = DataTransformation(db_config)
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        transformer.populate_companies()

        args, _ = mock_conn.execute.call_args
        sql_sent = args[0].text
        assert "INSERT INTO companies" in sql_sent
        assert "ON CONFLICT (company_name) DO NOTHING" in sql_sent
        mock_conn.commit.assert_called_once()

    
    def test_populate_locations(self, mock_db_config):
        """Test locations selection and insertion"""
        db_config, mock_engine = mock_db_config
        transformer = DataTransformation(db_config)
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.fetchall.return_value = [
            ("New York, NY", "United States"),
        ]
        transformer.populate_locations()

        assert mock_conn.execute.call_count >= 2
        mock_conn.commit.assert_called()


    def test_populate_platforms(self, mock_db_config):
        """Test platform insertion and regex expression'"""
        db_config, mock_engine = mock_db_config
        transformer = DataTransformation(db_config)
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        transformer.populate_platforms()

        args, _ = mock_conn.execute.call_args
        sql_sent = args[0].text
        assert "REGEXP_REPLACE(job_via, '^(via|melalui)\\s+', '', 'i')" in sql_sent
        assert "INSERT INTO platforms" in sql_sent
        assert "ON CONFLICT (platform_name) DO NOTHING" in sql_sent
        mock_conn.commit.assert_called_once()
        
    
    def test_populate_schedule_types(self, mock_db_config):
        """Test schedule_type insertion"""
        db_config, mock_engine = mock_db_config
        transformer = DataTransformation(db_config)
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        transformer.populate_schedule_types()

        args, _ = mock_conn.execute.call_args
        sql_sent = args[0].text
        assert "INSERT INTO schedule_types" in sql_sent
        assert "job_schedule_type IS NOT NULL" in sql_sent
        mock_conn.commit.assert_called_once()
        
        
    def test_populate_skill_categories_and_skills(self, mock_db_config):
        """Test JSONB parsing and insertion"""
        db_config, mock_engine = mock_db_config
        transformer = DataTransformation(db_config)
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.fetchall.side_effect = [
            [({"programming": ["python", "sql"]},)],
            [(1, "programming")],
            [("python",), ("sql",)]
        ]
        transformer.populate_skill_categories_and_skills()

        assert mock_conn.execute.call_count >= 6
        mock_conn.commit.assert_called()


    @patch('src.transformation.tqdm')
    def test_populate_jobs(self, _, mock_db_config):
        """Test jobs insertion (batch)"""
        db_config, mock_engine = mock_db_config
        transformer = DataTransformation(db_config)
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.scalar.return_value = 25000
        transformer.populate_jobs()

        assert mock_conn.execute.call_count >= 3
        mock_conn.commit.assert_called()


    @patch('src.transformation.tqdm')
    def test_populate_job_skills(self, _, mock_db_config):
        """Test job skills insertion (batch)"""
        db_config, mock_engine = mock_db_config
        transformer = DataTransformation(db_config)
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.scalar.return_value = 1000000
        transformer.populate_job_skills()

        assert mock_conn.execute.call_count >= 3
        mock_conn.commit.assert_called()


