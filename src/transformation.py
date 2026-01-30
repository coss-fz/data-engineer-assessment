
"""
Data transformation module for normalizing staging data into 3NF schema
"""

from typing import Dict, Optional
import pandas as pd
from tqdm import tqdm
from sqlalchemy import text
from loguru import logger

from database import DatabaseConfig # pylint: disable=import-error




BATCH_SIZE = 10000


class DataTransformation:
    """Transform staging data into normalized 3NF schema"""

    def __init__(self, db_config:DatabaseConfig):
        """Initialize data transformation"""
        self.db_config = db_config
        self.engine = db_config.get_engine()


    def delete_previous_info(self):
        """Delete all information in all tables (except staging)"""
        logger.info("Deleting data in all tables")

        with self.engine.connect() as conn:
            conn.execute(text("DELETE FROM job_skills"))
            conn.execute(text("DELETE FROM jobs"))
            conn.execute(text("DELETE FROM skills"))
            conn.execute(text("DELETE FROM skill_categories"))
            conn.execute(text("DELETE FROM companies"))
            conn.execute(text("DELETE FROM locations"))
            conn.execute(text("DELETE FROM platforms"))
            conn.execute(text("DELETE FROM schedule_types"))
            conn.commit()

        logger.info("All data was successfully deleted")


    def extract_location_components(
            self, location_string:str, country:str) -> Dict[str, Optional[str]]:
        """Extract city, state/province from location string"""
        if not location_string or pd.isna(location_string):
            return {'city': None, 'state_province': None, 'country': country}

        # Clean the location string
        location_string = str(location_string).strip()

        # Split by comma
        parts = [p.strip() for p in location_string.split(',')]

        if len(parts) >= 3:
            # Format: "City, State/Province, Country"
            return {
                'city': parts[0] if parts[0] else None,
                'state_province': parts[1] if parts[1] else None,
                'country': country
            }
        elif len(parts) == 2:
            # Format: "City, Country" or "City, State"
            return {
                'city': parts[0] if parts[0] else None,
                'state_province': parts[1] if parts[1] else None,
                'country': country
            }
        elif len(parts) == 1:
            # Only city or country
            return {
                'city': parts[0] if parts[0] else None,
                'state_province': None,
                'country': country
            }
        else:
            return {'city': None, 'state_province': None, 'country': country}


    def populate_companies(self):
        """Populate companies dimension table from staging"""
        logger.info("Populating companies table")

        sql = """
        INSERT INTO companies (company_name)
        SELECT DISTINCT TRIM(COALESCE(company_name, 'Unknown'))
        FROM staging_jobs
        WHERE TRIM(company_name) != ''
        ON CONFLICT (company_name) DO NOTHING;
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
            conn.commit()
            logger.info(f"Inserted {result.rowcount} companies")


    def populate_locations(self):
        """Populate locations dimension table from staging"""
        logger.info("Populating locations table")

        # First, get all unique location/country combinations from staging
        sql_select = """
        SELECT DISTINCT 
            job_location,
            job_country
        FROM staging_jobs
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(sql_select))
            locations = result.fetchall()

            logger.info(f"Processing {len(locations)} unique locations")

            # Process each location
            for loc_row in locations:
                location_string = loc_row[0]
                country = loc_row[1]

                # Parse location
                components = self.extract_location_components(location_string, country)

                # Insert location
                insert_sql = """
                INSERT INTO locations (main_city, main_state_province, country, full_location)
                VALUES (
                    :main_city, 
                    :main_state_province, 
                    COALESCE(:country, 'Unknown'), 
                    COALESCE(:full_location, 'Unknown')
                )
                ON CONFLICT (main_city, main_state_province, country, full_location) DO NOTHING
                """

                conn.execute(text(insert_sql), {
                    'main_city': components['city'],
                    'main_state_province': components['state_province'],
                    'country': components['country'],
                    'full_location': location_string
                })

            conn.commit()
            logger.info("Locations populated successfully")


    def populate_platforms(self):
        """Populate platforms dimension table from staging"""
        logger.info("Populating platforms table")

        sql = r"""
        INSERT INTO platforms (platform_name)
        SELECT DISTINCT
            TRIM(REGEXP_REPLACE(job_via, '^(via|melalui)\s+', '', 'i')) AS job_via_clean
        FROM staging_jobs
        WHERE job_via IS NOT NULL
        AND TRIM(job_via) <> ''
        ON CONFLICT (platform_name) DO NOTHING;
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
            conn.commit()
            logger.info(f"Inserted {result.rowcount} platforms")

    def populate_schedule_types(self):
        """Populate schedule_types dimension table from staging"""
        logger.info("Populating schedule_types table")

        sql = """
        INSERT INTO schedule_types (schedule_type_name)
        SELECT DISTINCT TRIM(job_schedule_type)
        FROM staging_jobs
        WHERE job_schedule_type IS NOT NULL 
            AND TRIM(job_schedule_type) != ''
        ON CONFLICT (schedule_type_name) DO NOTHING;
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
            conn.commit()
            logger.info(f"Inserted {result.rowcount} schedule types")


    def populate_skill_categories_and_skills(self):
        """Populate skill categories and skills tables from staging"""
        logger.info("Populating skill categories and skills tables")

        # Get all job_type_skills (which contains skill categories)
        sql_select = """
        SELECT DISTINCT job_type_skills
        FROM staging_jobs
        WHERE job_type_skills IS NOT NULL
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(sql_select))
            skill_type_rows = result.fetchall()

            all_categories = set()
            skills_by_category = {}

            # Extract all categories and skills
            for row in skill_type_rows:
                skill_dict = row[0]  # JSONB is returned as dict

                if skill_dict and isinstance(skill_dict, dict):
                    for category, skills in skill_dict.items():
                        all_categories.add(category)

                        if category not in skills_by_category:
                            skills_by_category[category] = set()

                        if isinstance(skills, list):
                            for skill in skills:
                                if skill:
                                    skills_by_category[category].add(skill.lower().strip())

            # Insert categories
            logger.info(f"Inserting {len(all_categories)} skill categories")
            for category in all_categories:
                conn.execute(text(
                    """INSERT INTO skill_categories (category_name)
                    VALUES (:name) ON CONFLICT DO NOTHING"""
                ), {'name': category})

            conn.commit()

            # Get category IDs
            result = conn.execute(text("SELECT category_id, category_name FROM skill_categories"))
            category_map = {row[1]: row[0] for row in result.fetchall()}

            # Insert skills with their categories
            skill_count = 0
            for category, skills in skills_by_category.items():
                category_id = category_map.get(category)
                for skill in skills:
                    conn.execute(text(
                        """INSERT INTO skills (skill_name, category_id) 
                           VALUES (:name, :cat_id) ON CONFLICT (skill_name) DO NOTHING"""
                    ), {'name': skill, 'cat_id': category_id})
                    skill_count += 1

            # Insert skills that don't have category info (from job_skills column)
            sql_skills = """
            SELECT DISTINCT unnest(job_skills) AS skill
            FROM staging_jobs
            WHERE job_skills IS NOT NULL
            """
            result = conn.execute(text(sql_skills))
            uncategorized_skills = result.fetchall()

            for row in uncategorized_skills:
                skill = row[0].lower().strip()
                conn.execute(text(
                    """INSERT INTO skills (skill_name, category_id) 
                       VALUES (:name, NULL) 
                       ON CONFLICT (skill_name) DO NOTHING"""
                ), {'name': skill})

            conn.commit()
            logger.info("Inserted skills successfully")


    def populate_jobs(self):
        """Populate jobs fact table from staging"""
        logger.info("Populating jobs table")

        sql = r"""
        INSERT INTO jobs (
            job_title,
            job_title_short,
            company_id,
            location_id,
            platform_id,
            schedule_type_id,
            job_work_from_home,
            job_posted_date,
            job_no_degree_mention,
            job_health_insurance,
            salary_rate,
            salary_year_avg,
            salary_hour_avg,
            search_location
        )
        SELECT 
            COALESCE(s.job_title, s.job_title_short) as job_title,
            s.job_title_short,
            c.company_id,
            l.location_id,
            p.platform_id,
            st.schedule_type_id,
            s.job_work_from_home,
            s.job_posted_date,
            s.job_no_degree_mention,
            s.job_health_insurance,
            s.salary_rate,
            s.salary_year_avg,
            s.salary_hour_avg,
            s.search_location
        FROM staging_jobs s
        LEFT JOIN companies c ON TRIM(COALESCE(s.company_name, 'Unknown')) = c.company_name
        LEFT JOIN locations l ON (
            COALESCE(s.job_country, 'Unknown') = l.country AND COALESCE(s.job_location, 'Unknown') = l.full_location
        )
        LEFT JOIN platforms p ON TRIM(REGEXP_REPLACE(s.job_via, '^(via|melalui)\s+', '', 'i')) = p.platform_name
        LEFT JOIN schedule_types st ON TRIM(s.job_schedule_type) = st.schedule_type_name
        WHERE NOT (s.job_title IS NULL AND s.job_title_short IS NULL)
        ORDER BY s.id
        """

        with self.engine.connect() as conn:
            total_rows = conn.execute(text(
                """SELECT COUNT(*) FROM staging_jobs
                WHERE NOT (job_title IS NULL AND job_title_short IS NULL)"""
            )).scalar()

        offset = 0
        with tqdm(total=total_rows, desc="Progress") as pbar:
            while offset < total_rows:
                batch_sql = f"""
                {sql}
                LIMIT {BATCH_SIZE} OFFSET {offset}
                """
                with self.engine.connect() as conn:
                    result = conn.execute(text(batch_sql))
                    conn.commit()

                    rows_inserted = result.rowcount
                    pbar.update(rows_inserted)
                    offset += BATCH_SIZE


    def populate_job_skills(self):
        """Populate job_skills table"""
        logger.info("Populating job_skills table")

        sql = """
        INSERT INTO job_skills (job_id, skill_id)
        SELECT j.job_id, s_map.skill_id
        FROM jobs j
        JOIN staging_jobs stg ON j.job_title = stg.job_title 
            AND j.job_posted_date = stg.job_posted_date
        CROSS JOIN LATERAL UNNEST(stg.job_skills) AS s_name
        JOIN skills s_map ON LOWER(TRIM(s_name)) = LOWER(s_map.skill_name)
        ORDER BY j.job_id, s_map.skill_id
        """

        with self.engine.connect() as conn:
            total_rows = conn.execute(text(
                """
                SELECT count(*)
                FROM jobs j
                JOIN staging_jobs stg ON j.job_title = stg.job_title 
                    AND j.job_posted_date = stg.job_posted_date
                CROSS JOIN LATERAL UNNEST(stg.job_skills) AS s_name
                JOIN skills s_map ON LOWER(TRIM(s_name)) = LOWER(s_map.skill_name)
                """
            )).scalar()

        offset = 0
        with tqdm(total=total_rows, desc="Progress") as pbar:
            while offset < total_rows:
                batch_sql = f"""
                {sql}
                LIMIT {BATCH_SIZE*10} OFFSET {offset}
                ON CONFLICT DO NOTHING
                """
                with self.engine.connect() as conn:
                    result = conn.execute(text(batch_sql))
                    conn.commit()

                    rows_inserted = result.rowcount
                    pbar.update(rows_inserted)
                    offset += BATCH_SIZE*10


    def run_transformation(self):
        """Run complete transformation process"""
        logger.info("Starting data transformation to 3NF")

        self.delete_previous_info()

        self.populate_companies()
        self.populate_locations()
        self.populate_platforms()
        self.populate_schedule_types()
        self.populate_skill_categories_and_skills()

        self.populate_jobs()

        self.populate_job_skills()

        logger.info("Data transformation completed successfully")
