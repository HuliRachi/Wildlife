import logging

logger = logging.getLogger(__name__)
table = "yt_api"

def insert_rows(cur, conn, schema, row):
    animal_name = row.get("name") or row.get("Name")
    try:
        if schema == "staging":
            cur.execute(
                f"""
                INSERT INTO {schema}.{table}("Name", "class", "diet", "family", "group", "color", "lifespan")
                VALUES (%(name)s, %(class)s, %(diet)s, %(family)s, %(group)s, %(color)s, %(lifespan)s);
                """,
                row,
            )
        else:
            cur.execute(
                f"""
                INSERT INTO {schema}.{table}("Name", "animal_class", "animal_diet", "animal_family", "animal_group", "animal_color", "animal_lifespan")
                VALUES (%(name)s, %(class)s, %(diet)s, %(family)s, %(group)s, %(color)s, %(lifespan)s);
                """,
                row,
            )
        conn.commit()
        logger.info(f"Inserted row into {schema} with Name: {animal_name}")
    except Exception as e:
        logger.error(f"Error inserting row into {schema} with Name: {animal_name} - {e}")
        raise e

def update_rows(cur, conn, schema, row):
    animal_name = row.get("name") or row.get("Name")
    try:
        if schema == "staging":
            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET "class" = %(class)s,
                    "diet" = %(diet)s,
                    "family" = %(family)s,
                    "group" = %(group)s,
                    "color" = %(color)s,
                    "lifespan" = %(lifespan)s
                WHERE "Name" = %(name)s;
                """,
                row,
            )
        else:
            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET "animal_class" = %(class)s,
                    "animal_diet" = %(diet)s,
                    "animal_family" = %(family)s,
                    "animal_group" = %(group)s,
                    "animal_color" = %(color)s,
                    "animal_lifespan" = %(lifespan)s
                WHERE "Name" = %(name)s;
                """,
                row,
            )
        conn.commit()
        logger.info(f"Updated row in {schema} with Name: {animal_name}")
    except Exception as e:
        logger.error(f"Error updating row in {schema} with Name: {animal_name} - {e}")
        raise e

def delete_rows(cur, conn, schema, ids_to_delete):
    try:
        formatted_ids = f"""({', '.join(f"'{id}'" for id in ids_to_delete)})"""
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Name" IN {formatted_ids};
            """
        )
        conn.commit()
        logger.info(f"Deleted rows from {schema} with Names: {formatted_ids}")
    except Exception as e:
        logger.error(f"Error deleting rows from {schema} - {e}")
        raise e
