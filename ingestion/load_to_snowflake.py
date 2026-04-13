import os
import time
import snowflake.connector
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

SNOWFLAKE_CONN = {
    "account": os.getenv('SNOWFLAKE_ACCOUNT'),
    "user": os.getenv('SNOWFLAKE_USER'),
    "password": os.getenv('SNOWFLAKE_PASSWORD'),
    "database": os.getenv('SNOWFLAKE_DATABASE'),
    "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
    "schema": "RAW"
}


TABLES = [
    {"parquet": "data/raw/repos", "table" : "RAW_REPOS"},
    {"parquet": "data/raw/commits", "table" : "RAW_COMMITS"},
    {"parquet": "data/raw/pull_requests", "table" : "RAW_PULL_REQUESTS"},
    {"parquet": "data/raw/issues", "table" : "RAW_ISSUES"},
    {"parquet": "data/raw/contributors", "table" : "RAW_CONTRIBUTORS"},
]


def get_snowflake_type(dtype):
    dtype = str(dtype)

    if dtype.startswith("datetime"):
        return "TIMESTAMP_NTZ"
    elif dtype in ("int64", "int32", "int16", "int8"):
        return "NUMBER(38,0)"
    elif dtype == "float64":
        return "DOUBLE"
    elif dtype == "bool":
        return "BOOLEAN"
    else:
        return "VARCHAR"
    

def create_table(cursor, table_name, df):
    columns = []
    for col_name, dtype in df.dtypes.items():
        sf_type = get_snowflake_type(dtype)
        columns.append(f'     "{col_name.upper()}"   {sf_type} ')

    columns_sql = " ,\n".join(columns)
    
    ddl_sql = f"CREATE OR REPLACE TABLE {table_name} (\n{columns_sql}\n);"
    # print(f"DDL: {ddl_sql}")
    cursor.execute(ddl_sql)

    
def load_table(conn, cursor, spark, parquet_table, table_name):

    print(f"\n{'='*50}")
    print(f"Loading: {parquet_table} → {table_name}")
    print(f"{'='*50}")


    df_spark = spark.read.parquet(parquet_table)
    print(f"    Spark Row Count: {df_spark.count()}")


    spark_to_sf = {
        "LongType()":      "NUMBER(18,0)",
        "IntegerType()":   "NUMBER(10,0)",
        "DoubleType()":    "FLOAT",
        "FloatType()":     "FLOAT",
        "BooleanType()":   "BOOLEAN",
        "TimestampType()": "TIMESTAMP_NTZ",
        "StringType()":    "VARCHAR",
    }

    columns = []
    for field in df_spark.schema.fields:
        sf_type = spark_to_sf.get(str(field.dataType), "VARCHAR")
        columns.append(f'    "{field.name.upper()}" {sf_type}')

    ddl = f"CREATE OR REPLACE TABLE {table_name} (\n"
    ddl += ",\n".join(columns)
    ddl += "\n);"

    cursor.execute(ddl)

    part_files = [
        f for f in os.listdir(parquet_table)
        if f.endswith(".parquet")
    ]

    if not part_files:
        print(f"  WARNING: No parquet files found in {parquet_table}")
        return

    print(f"  Found {len(part_files)} part file(s)")

    for part_file in part_files:
        full_path = os.path.abspath(os.path.join(parquet_table, part_file))
        put_result = cursor.execute(
            f"PUT file://{full_path} @%{table_name} OVERWRITE=TRUE AUTO_COMPRESS=FALSE;"
        ).fetchall()
        print(f"  PUT {part_file}: {put_result[0][6]}")
    

    cursor.execute(f"""
        COPY INTO {table_name}
        FROM @%{table_name}
        FILE_FORMAT = (
            TYPE = PARQUET
            SNAPPY_COMPRESSION = FALSE
        )
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PURGE = TRUE;
    """)

    print(f"  ✅ Loaded into {table_name} successfully")

def main():

    spark = SparkSession.builder.appName("Load to Snowflake App").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n Connecting to snowflake.....")

    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cursor = conn.cursor()
    cursor.execute(f"USE WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')};")
    cursor.execute("USE DATABASE github_analytics;")
    cursor.execute("USE SCHEMA RAW;")
    print("Connected!\n")

    


    

    for table in TABLES:
        try:
            print(f"\n\nStarting Process For: {table['table']}")
            load_table(conn, cursor, spark, table['parquet'], table['table'])
        except Exception as e:
            print(f"  ERROR loading {table['table']}: {e}")


    print(f"\n{'='*50}")
    print("Verification — Row counts in Snowflake RAW schema:")
    print(f"{'='*50}")
    print(f"{"TABLE NAME":<20}|{"PARQUET_COUNT":<20}|{"SF_TABLE_COUNT":<20}|MATCH")

    for table in TABLES:
        df_spark_count = spark.read.parquet(table['parquet']).count()
        cursor.execute(f"SELECT COUNT(1) FROM {table['table']};")
        sql_count = cursor.fetchone()[0]
        match = df_spark_count == sql_count
        print(f"{table['table']:<20}|{df_spark_count:<20}|{sql_count:<20}|{"YES" if match else "NO"}")
    
    cursor.close()
    conn.close()
    spark.stop()

if __name__ == "__main__":
    main()

