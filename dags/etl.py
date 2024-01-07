from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

TABLES_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (job_id) REFERENCES job(id)
);
"""

@task()
def extract():
    """Extract data from jobs.csv."""
    import os
    import pandas as pd
    
    SOURCE_FILE = "C:/Users/Pc/Desktop/Data-Internship-Home-Assignment/source/jobs.csv"
    EXTRACTED_DIR = "C:/Users/Pc/Desktop/Data-Internship-Home-Assignment/staging/extracted"
    
    df = pd.read_csv(SOURCE_FILE)
    
    context_data = df["context"]
    
    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    for i, item in enumerate(context_data):
        file_path = os.path.join(EXTRACTED_DIR, f"extracted_{i}.txt")
        with open(file_path, "w") as file:
            file.write(str(item))

@task()
def transform():
    """Clean and convert extracted elements to JSON."""
    EXTRACTED_DIR = "C:/Users/Pc/Desktop/Data-Internship-Home-Assignment/staging/extracted"
    TRANSFORMED_DIR = "C:/Users/Pc/Desktop/Data-Internship-Home-Assignment/staging/transformed"

    schema = {
        "job": {
            "title": "job_title",
            "industry": "job_industry",
            "description": "job_description",
            "employment_type": "job_employment_type",
            "date_posted": "job_date_posted",
        },
        "company": {
            "name": "company_name",
            "link": "company_linkedin_link",
        },
        "education": {
            "required_credential": "job_required_credential",
        },
        "experience": {
            "months_of_experience": "job_months_of_experience",
            "seniority_level": "seniority_level",
        },
        "salary": {
            "currency": "salary_currency",
            "min_value": "salary_min_value",
            "max_value": "salary_max_value",
            "unit": "salary_unit",
        },
        "location": {
            "country": "country",
            "locality": "locality",
            "region": "region",
            "postal_code": "postal_code",
            "street_address": "street_address",
            "latitude": "latitude",
            "longitude": "longitude",
        },
    }

    os.makedirs(TRANSFORMED_DIR, exist_ok=True)

    for file_name in os.listdir(EXTRACTED_DIR):
        if file_name.endswith(".txt"):
            extracted_file_path = os.path.join(EXTRACTED_DIR, file_name)
            transformed_file_path = os.path.join(TRANSFORMED_DIR, f"transformed_{file_name}.json")

            with open(extracted_file_path, "r") as extracted_file:
                extracted_data = extracted_file.read()

                cleaned_data = clean_and_transform(extracted_data)

                transformed_data = {}
                for category, attributes in schema.items():
                    transformed_data[category] = {}
                    for attribute_key, attribute_value in attributes.items():
                        transformed_data[category][attribute_value] = cleaned_data.get(attribute_key)

                with open(transformed_file_path, "w") as transformed_file:
                    json.dump(transformed_data, transformed_file)
@task()
def load():
    '''Load data into the SQLite database.'''
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    connection = sqlite_hook.get_conn()

    with connection:
        cursor = connection.cursor()
        cursor.executescript(TABLES_CREATION_QUERY)
        connection.commit()

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    '''ETL pipeline'''

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    extract_task = extract()
    transform_task = transform()
    load_task = load()

    create_tables >> extract_task >> transform_task >> load_task

etl_dag = etl_dag()
