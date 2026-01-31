
"""
Unit tests for database module
"""

import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from src.database import DatabaseConfig # pylint: disable=wrong-import-position





class TestDatabaseConfig:
    """Tests for DatabaseConfig class"""

    def test_init_with_url(self):
        """Test initialization with explicit URL"""
        test_url = "postgresql://user:pwd@host:5432/db"
        db_config = DatabaseConfig(database_url=test_url)
        assert db_config.database_url == test_url


    @patch.dict('os.environ', {
        'POSTGRES_USER': 'user',
        'POSTGRES_PASSWORD': 'pwd',
        'POSTGRES_HOST': 'host',
        'POSTGRES_PORT': '5433',
        'POSTGRES_DB': 'db'
    })
    def test_build_database_url_from_env(self):
        """Test building database URL from environment variables"""
        db_config = DatabaseConfig()
        expected = "postgresql://user:pwd@host:5433/db"
        assert db_config.database_url == expected


    @patch('src.database.create_engine')
    def test_get_engine(self, mock_create_engine):
        """Test getting database engine"""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        db_config = DatabaseConfig("postgresql://test:test@localhost:5432/test")

        engine_1 = db_config.get_engine()
        engine_2 = db_config.get_engine()

        assert engine_1 == mock_engine
        mock_create_engine.assert_called_once()
        assert engine_2 == mock_engine
        assert mock_create_engine.call_count == 1


    @patch('src.database.create_engine')
    @patch('src.database.sessionmaker')
    def test_get_session(self, mock_sessionmaker, mock_create_engine):
        """Test getting database session"""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        mock_session_class = Mock()
        mock_sessionmaker.return_value = mock_session_class
        mock_session_instance = Mock()
        mock_session_class.return_value = mock_session_instance

        db_config = DatabaseConfig("postgresql://test:test@localhost:5432/test")
        session = db_config.get_session()

        assert session == mock_session_instance
        mock_sessionmaker.assert_called_once_with(bind=mock_engine)
        mock_session_class.assert_called_once()


    @patch('src.database.create_engine')
    def test_test_connection_success(self, mock_create_engine):
        """Test successful connection"""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine

        db_config = DatabaseConfig("postgresql://test:test@localhost:5432/test")
        result = db_config.test_connection()

        assert result is True
        mock_conn.execute.assert_called_once()


    @patch('src.database.create_engine')
    @patch('builtins.open', create=True)
    def test_execute_sql_file(self, mock_open, mock_create_engine):
        """Test executing SQL file"""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine

        mock_open.return_value.__enter__.return_value.read.return_value = """
            CREATE TABLE test1 (id INT);
            CREATE TABLE test2 (id INT);
        """

        db_config = DatabaseConfig("postgresql://test:test@localhost:5432/test")
        result = db_config.execute_sql_file("test.sql")

        assert result is True
        assert mock_conn.execute.call_count >= 2
        mock_conn.commit.assert_called_once()


    @patch('src.database.create_engine')
    def test_close(self, mock_create_engine):
        """Test closing database connections"""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        db_config = DatabaseConfig("postgresql://test:test@localhost:5432/test")

        db_config.close()
        mock_engine.dispose.assert_not_called()

        db_config.get_engine()
        db_config.close()
        mock_engine.dispose.assert_called_once()
