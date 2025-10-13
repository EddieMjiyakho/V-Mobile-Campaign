# scripts/python/test_imports_fixed.py

import sys
import os

def fix_imports():
    """Fix Python path for imports"""
    # Add the scripts directory to Python path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    scripts_parent_dir = os.path.dirname(script_dir)  # This should be the 'scripts' directory
    project_root = os.path.dirname(scripts_parent_dir)  # This should be the project root
    
    print(f"Script directory: {script_dir}")
    print(f"Scripts parent: {scripts_parent_dir}") 
    print(f"Project root: {project_root}")
    
    # Add both the project root and scripts directory to path
    sys.path.insert(0, project_root)
    sys.path.insert(0, scripts_parent_dir)
    
    print(f"Python path: {sys.path}")
    
    return project_root

def test_imports():
    print("🔄 TESTING IMPORTS WITH FIXED PATHS")
    print("=" * 50)
    
    project_root = fix_imports()
    
    # Test basic imports
    try:
        import pandas as pd
        print("✅ pandas import works!")
        print(f"pandas version: {pd.__version__}")
    except ImportError as e:
        print(f"❌ pandas import failed: {e}")
        return False
    
    try:
        from sqlalchemy import create_engine
        print("✅ sqlalchemy import works!")
        print(f"sqlalchemy version: {create_engine.__module__}")
    except ImportError as e:
        print(f"❌ sqlalchemy import failed: {e}")
        print("Install with: pip install sqlalchemy")
        return False
    
    # Test our config import
    try:
        # Try different import approaches
        try:
            from scripts.config.database_config import get_postgres_connection_string, get_sqlalchemy_connection_string, POSTGRES_CONFIG
            print("✅ database_config import works (scripts.config)!")
            print(f"Database: {POSTGRES_CONFIG['database']}")
        except ImportError:
            try:
                # Try direct import
                import importlib.util
                spec = importlib.util.spec_from_file_location("database_config", os.path.join(project_root, "scripts", "config", "database_config.py"))
                database_config = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(database_config)
                print("✅ database_config import works (direct)!")
                POSTGRES_CONFIG = database_config.POSTGRES_CONFIG
                print(f"Database: {POSTGRES_CONFIG['database']}")
                get_postgres_connection_string = database_config.get_postgres_connection_string
                get_sqlalchemy_connection_string = database_config.get_sqlalchemy_connection_string
            except Exception as e:
                print(f"❌ database_config import failed: {e}")
                return False
        
        # Test connection strings
        psycopg2_conn_str = get_postgres_connection_string()
        sqlalchemy_conn_str = get_sqlalchemy_connection_string()
        print(f"Psycopg2 connection string: {psycopg2_conn_str[:60]}...")
        print(f"SQLAlchemy connection string: {sqlalchemy_conn_str}")
        
    except NameError as e:
        print(f"❌ Name error: {e}")
        return False
    
    # Test PostgreSQL imports (since your config is PostgreSQL)
    try:
        import psycopg2
        print("✅ psycopg2 import works!")
    except ImportError as e:
        print(f"❌ psycopg2 import failed: {e}")
        print("Install with: pip install psycopg2-binary")
        return False
    
    print("\n🎉 All imports working!")
    return True

if __name__ == "__main__":
    success = test_imports()
    sys.exit(0 if success else 1)