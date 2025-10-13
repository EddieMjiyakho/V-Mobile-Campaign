# scripts/config/database_config.py

POSTGRES_CONFIG = {
    'host': 'localhost',
    'database': 'vmobile_analysis',
    'user': 'postgres', 
    'password': '1234',
    'port': '5432'
}

# Set to False to use PostgreSQL (True would use SQLite)
USE_SQLITE = False

# Connection string for psycopg2
def get_postgres_connection_string():
    config = POSTGRES_CONFIG
    return f"host={config['host']} dbname={config['database']} user={config['user']} password={config['password']} port={config['port']}"

# Connection string for SQLAlchemy
def get_sqlalchemy_connection_string():
    config = POSTGRES_CONFIG
    return f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"

# Add these missing functions that load_data_to_db.py expects:
def get_connection_string():
    """SQLAlchemy connection string (alias for get_sqlalchemy_connection_string)"""
    return get_sqlalchemy_connection_string()

def get_db_connection_string():
    """Display-friendly connection string"""
    config = POSTGRES_CONFIG
    return f"PostgreSQL: {config['host']}:{config['port']}/{config['database']}"