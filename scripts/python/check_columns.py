# check_columns.py
import psycopg2

conn = psycopg2.connect(
    host='localhost',
    database='vmobile_analysis', 
    user='postgres',
    password='1234',
    port='5432'
)

cur = conn.cursor()

# Check exact column names with case sensitivity
print("=== Checking exact column names ===")
cur.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'city_lookup' 
    ORDER BY ordinal_position;
""")
for row in cur.fetchall():
    print(f"Column: '{row[0]}' (Type: {row[1]})")

# Test the join directly
print("\n=== Testing the join ===")
cur.execute("""
    SELECT ur.usage_event_city_id, cl.\"CITY_ID\", cl.CITY_NAME
    FROM usage_records ur
    LEFT JOIN city_lookup cl ON ur.usage_event_city_id = cl.\"CITY_ID\"
    LIMIT 5;
""")
print("With double quotes:")
try:
    for row in cur.fetchall():
        print(row)
except Exception as e:
    print(f"Error with double quotes: {e}")

# Test without quotes
print("\nTesting without quotes:")
cur.execute("""
    SELECT ur.usage_event_city_id, cl.CITY_ID, cl.CITY_NAME
    FROM usage_records ur
    LEFT JOIN city_lookup cl ON ur.usage_event_city_id = cl.CITY_ID
    LIMIT 5;
""")
try:
    for row in cur.fetchall():
        print(row)
except Exception as e:
    print(f"Error without quotes: {e}")

cur.close()
conn.close()