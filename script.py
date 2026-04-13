import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
)

cursor = conn.cursor()

# list everything available to this user
cursor.execute("SHOW WAREHOUSES;")
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()