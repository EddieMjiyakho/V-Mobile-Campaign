# scripts/python/run_complete_pipeline.py

import subprocess
import os
import sys

def run_pipeline():
    """Run the complete data pipeline from start to finish"""
    print("Starting V Mobile Data Pipeline...")
    
    scripts_dir = os.path.join(os.path.dirname(__file__))
    
    # Define the execution order for PostgreSQL
    scripts = [
        'subscriber_consolidation.py',
        'weekly_qualification_report.py', 
        'load_data_to_db.py',
        'execute_sql_analysis.py'
    ]
    
    for script in scripts:
        script_path = os.path.join(scripts_dir, script)
        print(f"\n{'='*50}")
        print(f"Running: {script}")
        print(f"{'='*50}")
        
        try:
            result = subprocess.run([sys.executable, script_path], check=True, capture_output=True, text=True)
            print(f"✓ {script} completed successfully")
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"✗ {script} failed with error:")
            print(e.stderr)
            return False
    
    print(f"\n{'='*50}")
    print("PostgreSQL Pipeline completed successfully!")
    print("Data is ready for Power BI dashboard refresh.")
    print(f"{'='*50}")
    return True

if __name__ == "__main__":
    run_pipeline()