
"""
Database configuration and connection management
"""

import os
from typing import Optional
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine




load_dotenv()


class DatabaseConfig:
    """Database configuration and connection manager"""

    def __init__(self, database_url: Optional[str] = None):
        """Initialize database configuration"""
        self.database_url = database_url or self._build_database_url()
        self.engine: Optional[Engine] = None
        self.db_session = None


    @staticmethod
    def _build_database_url() -> str:
        """Build database URL from environment variables"""

        user = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')
        host = os.getenv('POSTGRES_HOST')
        port = os.getenv('POSTGRES_PORT')
        database = os.getenv('POSTGRES_DB')

        return f"postgresql://{user}:{password}@{host}:{port}/{database}"


    def get_engine(self) -> Engine:
        """Get or create database engine"""
        if self.engine is None:
            logger.info(f"Creating database engine for {self.database_url.split('@')[1]}")
            self.engine = create_engine(self.database_url)
        return self.engine


    def get_session(self):
        """Get database session."""
        if self.db_session is None:
            engine = self.get_engine()
            self.db_session = sessionmaker(bind=engine)
        return self.db_session()


    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            return True
        except Exception as e: # pylint: disable=broad-exception-caught
            logger.error(f"Database connection failed: {e}")
            return False


    def execute_sql_file(self, filepath: str) -> bool:
        """Execute SQL commands from a file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                sql_commands = f.read()

            engine = self.get_engine()
            with engine.connect() as conn:
                for statement in sql_commands.split(';'):
                    statement = statement.strip()
                    if statement:
                        conn.execute(text(statement))
                conn.commit()

            logger.info(f"Successfully executed SQL file: {filepath}")
            return True
        except Exception as e: # pylint: disable=broad-exception-caught
            logger.error(f"Failed to execute SQL file {filepath}: {e}")
            return False


    def close(self):
        """Close database connections"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")
