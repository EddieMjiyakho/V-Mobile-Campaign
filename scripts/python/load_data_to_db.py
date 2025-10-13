import pandas as pd
import os
import sys
from sqlalchemy import create_engine, text
import psycopg2

# Direct configuration
POSTGRES_CONFIG = {
    'host': 'localhost',
    'database': 'vmobile_analysis',
    'user': 'postgres', 
    'password': '1234',
    'port': '5432'
}

def get_connection_string():
    return f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"

def get_db_connection_string():
    return f"PostgreSQL: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"

USE_SQLITE = False

print("Loading data to database for advanced analysis...")
print(f"Using: {'SQLite' if USE_SQLITE else 'PostgreSQL'}")

# Setup paths
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))
data_dir = os.path.join(project_root, 'data')
raw_data_dir = os.path.join(data_dir, 'raw')
processed_data_dir = os.path.join(data_dir, 'processed')
database_dir = os.path.join(data_dir, 'database')

print(f"Project root: {project_root}")
print(f"Data directory: {data_dir}")

# Create database directory if it doesn't exist
os.makedirs(database_dir, exist_ok=True)

try:
    # Create SQLAlchemy engine
    engine = create_engine(get_connection_string())
    
    # Test connection
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        print("Connected to PostgreSQL:", result.fetchone()[0])
    
    # Load all tables - LOOKUP FILES ARE IN RAW FOLDER
    tables_to_load = {
        'master_subscribers': ('processed', 'combined_subscribers_master.csv', ','),
        'city_lookup': ('raw', 'VMobile_city_lookup.csv', ';'),  # Use semicolon delimiter
        'usage_event_lookup': ('raw', 'VMobile_usage_event_lookup.csv', ';')  # Use semicolon delimiter
    }

    print("Loading tables to database...")
    for table_name, (folder, file_name, delimiter) in tables_to_load.items():
        if folder == 'processed':
            file_path = os.path.join(processed_data_dir, file_name)
        else:  # raw folder
            file_path = os.path.join(raw_data_dir, file_name)
            
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, delimiter=delimiter)
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Loaded {table_name}: {len(df)} records")
            print(f"Columns in {table_name}: {df.columns.tolist()}")
        else:
            print(f"ERROR: File not found: {file_path}")

    # Load and prepare usage data
    print("Preparing usage data...")
    
    def prepare_usage_data():
        usage_week1_path = os.path.join(raw_data_dir, 'VMobile_usage_records.csv')
        usage_week2_path = os.path.join(raw_data_dir, 'VMobile_usage_records_week_2.csv')
        
        if not os.path.exists(usage_week1_path):
            print(f"ERROR: Usage week1 file not found: {usage_week1_path}")
            return pd.DataFrame()
        if not os.path.exists(usage_week2_path):
            print(f"ERROR: Usage week2 file not found: {usage_week2_path}")
            return pd.DataFrame()
            
        # Load the data
        usage_week1 = pd.read_csv(usage_week1_path, delimiter=';')
        usage_week2 = pd.read_csv(usage_week2_path)
        
        # Standardize column names - BOTH FILES HAVE MSISDN
        column_mapping = {
            'MSISDN': 'msisdn',
            'USAGE_EVENT_DATE_TIME': 'usage_event_date_time',
            'USAGE_EVENT_CITY_ID': 'usage_event_city_id', 
            'USAGE_EVENT_TYPE_ID': 'usage_event_type_id',
            'USAGE_EVENT_TRACKING_QUANTITY': 'usage_event_tracking_quantity',
            'USAGE_EVENT_TRACKING_UNIT': 'usage_event_tracking_unit', 
            'USAGE_EVENT_BILLING_QUANTITY': 'usage_event_billing_quantity',
            'USAGE_EVENT_BILLING_UNIT': 'usage_event_billing_unit',
            'USAGE_EVENT_REVENUE': 'usage_event_revenue'
        }
        
        usage_week1_clean = usage_week1.rename(columns=column_mapping)
        usage_week2_clean = usage_week2.rename(columns=column_mapping)
        
        def clean_msisdn(msisdn):
            import re
            if pd.isna(msisdn):
                return msisdn
            cleaned = re.sub(r'[^\d]', '', str(msisdn))
            return cleaned
        
        def clean_revenue(revenue):
            if pd.isna(revenue):
                return 0.0
            revenue_str = str(revenue).replace(',', '.')
            try:
                return float(revenue_str)
            except ValueError:
                return 0.0
        
        # Clean the data
        usage_week1_clean['msisdn'] = usage_week1_clean['msisdn'].apply(clean_msisdn)
        usage_week2_clean['msisdn'] = usage_week2_clean['msisdn'].apply(clean_msisdn)
        usage_week1_clean['usage_event_revenue'] = usage_week1_clean['usage_event_revenue'].apply(clean_revenue)
        usage_week2_clean['usage_event_revenue'] = usage_week2_clean['usage_event_revenue'].apply(clean_revenue)
        
        # Combine and parse dates
        all_usage = pd.concat([usage_week1_clean, usage_week2_clean], ignore_index=True)
        all_usage['usage_event_date_time'] = pd.to_datetime(
            all_usage['usage_event_date_time'], 
            dayfirst=True, 
            errors='coerce'
        )
        
        return all_usage

    # Load usage data
    usage_data = prepare_usage_data()
    if not usage_data.empty:
        usage_data.to_sql('usage_records', engine, if_exists='replace', index=False)
        print(f"Loaded usage_records: {len(usage_data)} records")
    else:
        print("ERROR: No usage data to load")

    # Create indexes for better performance
    with engine.connect() as conn:
        print("Creating indexes for better performance...")
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_usage_msisdn ON usage_records (msisdn);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_usage_date ON usage_records (usage_event_date_time);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_usage_city ON usage_records (usage_event_city_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_subscribers_cell ON master_subscribers (cell_phone_number);"))
        print("Indexes created successfully")

    engine.dispose()
    print(f"\nSUCCESS: Data successfully loaded!")
    print(f"Database: {get_db_connection_string()}")

except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()