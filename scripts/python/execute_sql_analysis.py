import psycopg2
import pandas as pd
import os

# Direct configuration - no imports needed
POSTGRES_CONFIG = {
    'host': 'localhost',
    'database': 'vmobile_analysis',
    'user': 'postgres', 
    'password': '1234',
    'port': '5432'
}

def get_postgres_connection_string():
    config = POSTGRES_CONFIG
    return f"host={config['host']} dbname={config['database']} user={config['user']} password={config['password']} port={config['port']}"

def run_sql_analysis():
    # Setup paths - CORRECTED
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))  # Go up TWO levels to VMobile
    data_dir = os.path.join(project_root, 'data')
    sql_dir = os.path.join(project_root, 'scripts', 'sql')  # Correct: scripts/sql
    analysis_dir = os.path.join(sql_dir, 'analysis')
    output_dir = os.path.join(data_dir, 'processed')
    
    print(f"Script directory: {script_dir}")
    print(f"Project root: {project_root}")
    print(f"Looking for SQL files in: {analysis_dir}")
    
    # Check what files actually exist
    if os.path.exists(analysis_dir):
        actual_files = os.listdir(analysis_dir)
        print(f"Actual SQL files found: {actual_files}")
    else:
        print(f"ERROR: Analysis directory not found: {analysis_dir}")
        print("Available directories:")
        for root, dirs, files in os.walk(project_root):
            if 'sql' in root.lower():
                print(f"  {root}")
        return
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(get_postgres_connection_string())
        print("Connected to PostgreSQL successfully!")
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return
    
    # Define SQL files and their output names
    sql_queries = {
        'weekly_trends_analysis.sql': 'weekly_summary_trends.csv',
        'regional_analysis.sql': 'regional_analysis.csv', 
        'subscriber_details.sql': 'subscriber_details.csv'
    }
    
    print("Executing SQL analysis...")
    
    for sql_file, output_file in sql_queries.items():
        sql_path = os.path.join(analysis_dir, sql_file)
        
        if os.path.exists(sql_path):
            with open(sql_path, 'r') as f:
                query = f.read()
            
            print(f"Running {sql_file}...")
            try:
                result_df = pd.read_sql_query(query, conn)
                
                # Save results
                output_path = os.path.join(output_dir, output_file)
                result_df.to_csv(output_path, index=False)
                print(f"Saved {len(result_df)} records to {output_file}")
            except Exception as e:
                print(f"Error running {sql_file}: {e}")
        else:
            print(f"Warning: {sql_file} not found in {analysis_dir}")
    
    conn.close()
    print("\nPostgreSQL analysis complete! Files ready for Power BI.")

if __name__ == "__main__":
    run_sql_analysis()