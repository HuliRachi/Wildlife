from datawarehouse.data_utils import (
    get_conn_cursor,
    get_video_ids,
    close_conn_cursor,
    create_schema,
    create_table,
)
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():
    schema = "staging"
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        Animal_data = load_data()

        create_schema(schema)
        create_table(schema)

        table_ids = set(get_video_ids(cur, schema))

        for row in Animal_data:
            if row["name"] in table_ids:
                update_rows(cur, conn, schema, row)
            else:
                insert_rows(cur, conn, schema, row)

        ids_in_json = {row["name"] for row in Animal_data}
        ids_to_delete = table_ids - ids_in_json

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
            
        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)

@task
def core_table():
    schema = "core"
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = set(get_video_ids(cur, schema))
        current_animal_names = set()

        cur.execute(f'SELECT "Name" AS name, "class", "diet", "family", "group", "color", "lifespan" FROM staging.{table};')
        rows = cur.fetchall()

        for row in rows:
            animal_name = row["name"]
            current_animal_names.add(animal_name)

            if animal_name in table_ids:
                update_rows(cur, conn, schema, row)
            else:
                insert_rows(cur, conn, schema, row)

        ids_to_delete = table_ids - current_animal_names

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)
