import requests
import pytest
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

@pytest.fixture(scope="function")
def real_postgres_connection():
    """
    Pytest Fixture: Initializes a live connection to the local 
    PostgreSQL database using Airflow's internal connection hooks.
    """
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    
    yield conn  
    
    conn.close() 


def test_real_postgres_connection(real_postgres_connection):
    """
    Integration Test: Verifies that your script code can communicate 
    with the live database container efficiently.
    """
    cursor = None
    try:
        cursor = real_postgres_connection.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()

        assert result[0] == 1

    except psycopg2.Error as e:
        pytest.fail(f"Database query failed: {e}")

    finally:
        if cursor is not None:
            cursor.close()

