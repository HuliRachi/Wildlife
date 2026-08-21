from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()

def create_schema(schema):
    conn, cur = get_conn_cursor()
    try:
        conn.autocommit = True 
        schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        cur.execute(schema_sql)
    finally:
        close_conn_cursor(conn, cur)

def create_table(schema):

    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Name" TEXT NOT NULL,
                "class" TEXT NOT NULL,
                "diet" VARCHAR(50) NOT NULL,
                "family" VARCHAR(50) NOT NULL,
                "group" VARCHAR(50) NOT NULL,
                "color" TEXT NOT NULL,
                "lifespan" TEXT NOT NULL
            );
         """
    else:
        table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table} (
                        "Name" TEXT NOT NULL,
                        "animal_class" TEXT NOT NULL,
                        "animal_diet" VARCHAR(50) NOT NULL,
                        "animal_family" VARCHAR(50) NOT NULL,
                        "animal_group" VARCHAR(50) NOT NULL,
                        "animal_color" TEXT NOT NULL,
                        "animal_lifespan" TEXT NOT NULL
                    );
            """
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)

def get_video_ids(cur, schema):
    cur.execute(f"""SELECT "Name" FROM {schema}.{table};""")
    ids = cur.fetchall()

    video_ids = [row["Name"] for row in ids]
    return video_ids
