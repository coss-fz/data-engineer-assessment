
"""
Main pipeline orchestrator that runs the complete ETL process
"""

import sys
from pathlib import Path
from loguru import logger

from transformation import DataTransformation # pylint: disable=import-error
from ingestion import DataIngestion # pylint: disable=import-error
from validation import DataValidator # pylint: disable=import-error
from database import DatabaseConfig # pylint: disable=import-error




class JobPipeline:
    """Main pipeline orchestrator for the ETL process"""

    def __init__(self, csv_filepath:str, only_transformation:bool=False):
        """Initialize pipeline"""
        self.csv_filepath = csv_filepath
        self.only_transformation = only_transformation
        self.db_config = DatabaseConfig()

        # Setup logging
        logger.add(
            "logs/pipeline_{time}.log", 
            rotation="10 MB",
            retention="10 days",
            level="INFO"
        )

    def setup_database(self) -> bool:
        """Setup database schema"""
        logger.info("Setting up database schema")

        try:
            if not self.db_config.test_connection():
                return False

            # Execute schema file
            schema_path = Path(__file__).parent.parent / "sql" / "schema.sql"
            if not schema_path.exists():
                logger.error(f"Schema file not found: {schema_path}")
                return False

            if not self.db_config.execute_sql_file(str(schema_path)):
                return False

            logger.info("Database schema setup completed")
            return True

        except Exception as e: # pylint: disable=broad-exception-caught
            logger.error(f"Error setting up database: {e}")
            return False

    def validate_data(self, df) -> bool:
        """Validate input data"""
        logger.info("Validating input data")

        validator = DataValidator()

        is_valid, error_msg = validator.validate_raw_data(df)
        if not is_valid:
            logger.error(f"Schema validation failed: {error_msg}")
            return False

        validator.check_data_quality(df)

        validator.validate_skills_structure(df)

        validator.generate_data_profile(df)

        logger.info("Data validation completed successfully")
        return True

    def run_ingestion(self) -> bool:
        """Run data ingestion phase"""
        try:
            logger.info("=" * 100)
            logger.info("PHASE 1: DATA INGESTION")
            logger.info("=" * 100)

            ingestion = DataIngestion(self.db_config)

            df = ingestion.read_csv(self.csv_filepath)

            if not self.validate_data(df):
                logger.error("Data validation failed, stopping pipeline")
                return False

            ingestion.load_to_staging(df)

            logger.info("Phase 1 completed successfully")
            return True

        except Exception as e: # pylint: disable=broad-exception-caught
            logger.error(f"Error during ingestion: {e}")
            return False

    def run_transformation(self) -> bool:
        """Run data transformation phase"""
        try:
            logger.info("=" * 100)
            logger.info("PHASE 2: DATA TRANSFORMATION (3NF)")
            logger.info("=" * 100)

            transformation = DataTransformation(self.db_config)
            transformation.run_transformation()

            logger.info("Phase 2 completed successfully")
            return True

        except Exception as e: # pylint: disable=broad-exception-caught
            logger.error(f"Error during transformation: {e}")
            return False

    def run(self) -> bool:
        """Run complete pipeline"""
        logger.info("=" * 100)
        logger.info("STARTING JOB DATA PIPELINE")
        logger.info("=" * 100)

        if self.only_transformation:
            try:
                logger.warning("Executing only Phase 2 (Internal Postgres Transformation)")
                if not self.run_transformation():
                    logger.error("Transformation phase failed")
                    return False
            except Exception as e: # pylint: disable=broad-exception-caught
                logger.error(f"Pipeline failed with error: {e}")
                return False
            finally:
                self.db_config.close()

            logger.info("=" * 100)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 100)
            return True

        try:
            if not self.setup_database():
                logger.error("Database setup failed")
                return False

            # Phase 1
            if not self.run_ingestion():
                logger.error("Ingestion phase failed")
                return False

            # Phase 2
            if not self.run_transformation():
                logger.error("Transformation phase failed")
                return False

            logger.info("=" * 100)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 100)
            return True
        except Exception as e: # pylint: disable=broad-exception-caught
            logger.error(f"Pipeline failed with error: {e}")
            return False
        finally:
            self.db_config.close()


def main():
    """Main entry point for the pipeline"""
    import argparse # pylint: disable=import-outside-toplevel

    parser = argparse.ArgumentParser(description="Job Data ETL Pipeline")
    parser.add_argument(
        "--csv",
        type=str,
        default="data/data_jobs.csv",
        help="Path to input CSV file"
    )
    parser.add_argument(
        "--only-transformation",
        action="store_true",
        help="Only runs the Phase 2"
    )

    args = parser.parse_args()

    pipeline = JobPipeline(args.csv, only_transformation=args.only_transformation)

    success = pipeline.run()

    if success:
        sys.exit(0)
    else:
        logger.error("Pipeline execution failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
