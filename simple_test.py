# simple_test.py (keep this one)
import psycopg2

print("Testing PostgreSQL connection and database...")

try:
    # Connect to main postgres database
    conn = psycopg2.connect(
        host='localhost',
        database='postgres',
        user='postgres',
        password='1234',
        port='5432'
    )
    print("✅ Connected to PostgreSQL")
    
    # Check if our database exists
    conn.autocommit = True
    cursor = conn.cursor()
    
    cursor.execute("SELECT 1 FROM pg_database WHERE datname='vmobile_analysis'")
    exists = cursor.fetchone()
    
    if exists:
        print("✅ vmobile_analysis database exists")
        
        # Test connecting to it
        try:
            conn_vm = psycopg2.connect(
                host='localhost',
                database='vmobile_analysis',
                user='postgres',
                password='1234',
                port='5432'
            )
            print("✅ Successfully connected to vmobile_analysis!")
            conn_vm.close()
        except Exception as e:
            print(f"❌ Could not connect to vmobile_analysis: {e}")
    else:
        print("❌ vmobile_analysis does not exist - creating it...")
        cursor.execute('CREATE DATABASE vmobile_analysis')
        print("✅ Created vmobile_analysis database")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Connection failed: {e}")