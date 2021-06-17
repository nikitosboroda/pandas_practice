import psycopg2
from psycopg2 import sql, extras


def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        user='select',
        password="select"
    )


def move_df_to_db(df, schema_name, table_name):
    with get_connection() as connection:
        with connection.cursor() as cur:
            # cur.copy_from(df, f"{schema_name}.{table_name}", columns=)
            for _, row in df.iterrows():
                insertion_query = sql.SQL("INSERT INTO {schema}.{table} ({columns}) values %s").format(
                    schema=sql.Identifier(schema_name),
                    table=sql.Identifier(table_name),
                    columns=sql.SQL(",").join(
                        [sql.Identifier(col) for col in list(df.columns)])
                )
                extras.execute_values(cur, insertion_query, [tuple(row.tolist())])
