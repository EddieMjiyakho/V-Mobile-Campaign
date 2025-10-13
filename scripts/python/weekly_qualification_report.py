# scripts/weekly_qualification_report.py

import pandas as pd # type: ignore
import os
import re
from datetime import datetime

# Setup paths
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))
data_dir = os.path.join(project_root, 'data')
raw_data_dir = os.path.join(data_dir, 'raw')
processed_data_dir = os.path.join(data_dir, 'processed')

print("Loading data for weekly qualification report...")

# Load the master subscriber list
master_subscribers_path = os.path.join(processed_data_dir, "combined_subscribers_master.csv")
master_subscribers = pd.read_csv(master_subscribers_path)
print(f"Master subscribers loaded: {master_subscribers.shape[0]} records")

# Load usage records with proper handling for BOTH weeks
usage_week1_path = os.path.join(raw_data_dir, "VMobile_usage_records.csv")
usage_week2_path = os.path.join(raw_data_dir, "VMobile_usage_records_week_2.csv")

def load_and_fix_usage_data(file_path, week_name):
    """Load usage data file, handling semicolon-separated format"""
    print(f"Loading {week_name} data...")
    
    # First, try to detect the format
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
        
        # Check if it's semicolon or comma separated
        if ';' in first_line:
            # Load with semicolon delimiter
            df = pd.read_csv(file_path, delimiter=';')
            print(f"{week_name} loaded with semicolon delimiter: {df.shape[0]} records")
        else:
            # Try comma delimiter
            df = pd.read_csv(file_path, delimiter=',')
            print(f"{week_name} loaded with comma delimiter: {df.shape[0]} records")
            
    except Exception as e:
        print(f"Error loading {week_name} with delimiter detection: {e}")
        # Fallback: load and manually split if needed
        df = pd.read_csv(file_path)
        if len(df.columns) == 1:
            print(f"{week_name} loaded as single column, splitting data...")
            first_col = df.columns[0]
            # Try splitting by semicolon first, then comma
            if df[first_col].str.contains(';').any():
                df = df[first_col].str.split(';', expand=True)
            else:
                df = df[first_col].str.split(',', expand=True)
            
            # Set column names from first row if it looks like a header
            if df.iloc[0].str.contains('MSISDN', case=False).any():
                df.columns = df.iloc[0]
                df = df[1:].reset_index(drop=True)
    
    return df

# Load both weeks with proper formatting
usage_week1 = load_and_fix_usage_data(usage_week1_path, "Week 1")
usage_week2 = load_and_fix_usage_data(usage_week2_path, "Week 2")

# Load lookup tables with proper delimiters
usage_event_lookup_path = os.path.join(raw_data_dir, "VMobile_usage_event_lookup.csv")
city_lookup_path = os.path.join(raw_data_dir, "VMobile_city_lookup.csv")

usage_event_lookup = pd.read_csv(usage_event_lookup_path, delimiter=';')
city_lookup = pd.read_csv(city_lookup_path, delimiter=';')

print("All data loaded successfully!")

# Debug: Check actual column names
print("Week 1 actual columns:", usage_week1.columns.tolist())
print("Week 2 actual columns:", usage_week2.columns.tolist())

# Standardize column names function
def standardize_columns(df):
    # Convert to lowercase and remove special characters
    new_columns = []
    for col in df.columns:
        # Remove BOM and other special characters, convert to lowercase
        clean_col = str(col).lower().replace('ï»¿', '').replace('"', '').strip()
        new_columns.append(clean_col)
    df.columns = new_columns
    return df

# Apply standardization to both datasets
usage_week1 = standardize_columns(usage_week1)
usage_week2 = standardize_columns(usage_week2)

print("After standardization:")
print("Week 1 columns:", usage_week1.columns.tolist())
print("Week 2 columns:", usage_week2.columns.tolist())

# Handle case where columns might still be combined (single column with all data)
if len(usage_week1.columns) == 1:
    print("Week 1 data is in single column, splitting...")
    first_col = usage_week1.columns[0]
    usage_week1 = usage_week1[first_col].str.split(';', expand=True)
    # Set expected column names
    expected_columns = [
        'msisdn', 'usage_event_date_time', 'usage_event_city_id', 
        'usage_event_type_id', 'usage_event_tracking_quantity', 
        'usage_event_tracking_unit', 'usage_event_billing_quantity', 
        'usage_event_billing_unit', 'usage_event_revenue'
    ]
    if len(usage_week1.columns) == len(expected_columns):
        usage_week1.columns = expected_columns

if len(usage_week2.columns) == 1:
    print("Week 2 data is in single column, splitting...")
    first_col = usage_week2.columns[0]
    usage_week2 = usage_week2[first_col].str.split(',', expand=True)
    # Set expected column names
    expected_columns = [
        'msisdn', 'usage_event_date_time', 'usage_event_city_id', 
        'usage_event_type_id', 'usage_event_tracking_quantity', 
        'usage_event_tracking_unit', 'usage_event_billing_quantity', 
        'usage_event_billing_unit', 'usage_event_revenue'
    ]
    if len(usage_week2.columns) == len(expected_columns):
        usage_week2.columns = expected_columns

print("Final column structure:")
print("Week 1 columns:", usage_week1.columns.tolist())
print("Week 2 columns:", usage_week2.columns.tolist())

# Function to clean MSISDN format
def clean_msisdn(msisdn):
    if pd.isna(msisdn):
        return msisdn
    # Remove +, spaces, and any non-digit characters
    cleaned = re.sub(r'[^\d]', '', str(msisdn))
    return cleaned

# Function to clean revenue format
def clean_revenue(revenue):
    if pd.isna(revenue):
        return 0.0
    # Convert to string, replace comma with point, then convert to float
    revenue_str = str(revenue).replace(',', '.').strip()
    try:
        return float(revenue_str)
    except ValueError:
        return 0.0

print("Standardizing MSISDN and revenue formats...")

# Apply cleaning to MSISDN columns
if 'msisdn' in usage_week1.columns:
    usage_week1['msisdn'] = usage_week1['msisdn'].apply(clean_msisdn)
else:
    print("Warning: 'msisdn' column not found in Week 1 data")

if 'msisdn' in usage_week2.columns:
    usage_week2['msisdn'] = usage_week2['msisdn'].apply(clean_msisdn)
else:
    print("Warning: 'msisdn' column not found in Week 2 data")

master_subscribers['cell_phone_number'] = master_subscribers['cell_phone_number'].apply(clean_msisdn)

# Apply revenue cleaning
if 'usage_event_revenue' in usage_week1.columns:
    usage_week1['usage_event_revenue'] = usage_week1['usage_event_revenue'].apply(clean_revenue)
if 'usage_event_revenue' in usage_week2.columns:
    usage_week2['usage_event_revenue'] = usage_week2['usage_event_revenue'].apply(clean_revenue)

print("Sample after standardization:")
if 'msisdn' in usage_week1.columns and 'usage_event_revenue' in usage_week1.columns:
    print("Week 1:", usage_week1[['msisdn', 'usage_event_revenue']].head(3).values.tolist())
if 'msisdn' in usage_week2.columns and 'usage_event_revenue' in usage_week2.columns:
    print("Week 2:", usage_week2[['msisdn', 'usage_event_revenue']].head(3).values.tolist())

# Combine both weeks of usage data
all_usage_data = pd.concat([usage_week1, usage_week2], ignore_index=True)
print(f"Combined usage data: {all_usage_data.shape[0]} records")

# Parse dates with multiple format attempts
print("Parsing dates...")
if 'usage_event_date_time' in all_usage_data.columns:
    date_formats = ['%d %m %Y %H:%M', '%Y/%m/%d %H:%M', '%Y%m%d %H:%M', '%d/%m/%Y %H:%M']

    for fmt in date_formats:
        try:
            parsed_dates = pd.to_datetime(all_usage_data['usage_event_date_time'], format=fmt, errors='coerce')
            success_rate = 1 - parsed_dates.isna().mean()
            if success_rate > 0.8:  # If most dates parsed successfully
                all_usage_data['usage_event_date_time'] = parsed_dates
                print(f"Successfully parsed dates with format: {fmt} ({success_rate:.1%} success)")
                break
        except:
            continue

    # If no format worked, use flexible parsing as last resort
    if all_usage_data['usage_event_date_time'].dtype == 'object':
        all_usage_data['usage_event_date_time'] = pd.to_datetime(
            all_usage_data['usage_event_date_time'], 
            errors='coerce'
        )
        print("Used flexible date parsing")

    print(f"Date range in usage data: {all_usage_data['usage_event_date_time'].min()} to {all_usage_data['usage_event_date_time'].max()}")
else:
    print("Warning: 'usage_event_date_time' column not found")

# For analysis, use the most recent complete week in the data
if 'usage_event_date_time' in all_usage_data.columns and not all_usage_data['usage_event_date_time'].isna().all():
    latest_date = all_usage_data['usage_event_date_time'].max()
    week_end = latest_date
    week_start = week_end - pd.Timedelta(days=6)

    print(f"Analyzing week: {week_start.date()} to {week_end.date()}")

    # Filter usage data for this specific week
    weekly_usage = all_usage_data[
        (all_usage_data['usage_event_date_time'] >= week_start) & 
        (all_usage_data['usage_event_date_time'] <= week_end)
    ].copy()

    print(f"Usage records for selected week: {weekly_usage.shape[0]}")
else:
    print("Warning: No valid dates found, using all data")
    weekly_usage = all_usage_data.copy()

# Calculate weekly revenue per subscriber
if 'msisdn' in weekly_usage.columns and 'usage_event_revenue' in weekly_usage.columns:
    weekly_revenue = weekly_usage.groupby('msisdn').agg({
        'usage_event_revenue': 'sum'
    }).reset_index()
    weekly_revenue = weekly_revenue.rename(columns={'usage_event_revenue': 'total_weekly_revenue'})

    print(f"Subscribers with usage this week: {weekly_revenue.shape[0]}")
else:
    print("Error: Required columns for revenue calculation not found")
    weekly_revenue = pd.DataFrame(columns=['msisdn', 'total_weekly_revenue'])

# Identify qualifying subscribers
qualifying_subscribers = weekly_revenue[weekly_revenue['total_weekly_revenue'] >= 30].copy()
print(f"Qualifying subscribers (revenue >= R30): {qualifying_subscribers.shape[0]}")

# Check matching with master subscribers
master_msisdns = set(master_subscribers['cell_phone_number'])
qualifying_msisdns = set(qualifying_subscribers['msisdn'])

missing_in_master = qualifying_msisdns - master_msisdns
print(f"Qualifying subscribers missing from master: {len(missing_in_master)}")
if missing_in_master:
    print("Sample missing MSISDNs:", list(missing_in_master)[:5])

# Merge with master subscriber data
qualifying_with_details = qualifying_subscribers.merge(
    master_subscribers,
    left_on='msisdn', 
    right_on='cell_phone_number',
    how='left'  # Keep all qualifying subscribers even if not in master
)

print(f"After merging with subscriber details: {qualifying_with_details.shape[0]} records")

# Calculate SMS and Voice counts
sms_event_ids = [6, 10]  # on-net-sms, other-mobile-sms  
voice_event_ids = [3, 4, 5, 8, 9]  # Various call types

# Count SMS (sum of quantities since SMS are counted)
if 'usage_event_type_id' in weekly_usage.columns and 'usage_event_billing_quantity' in weekly_usage.columns:
    sms_counts = weekly_usage[weekly_usage['usage_event_type_id'].isin(sms_event_ids)]
    sms_counts = sms_counts.groupby('msisdn').agg({
        'usage_event_billing_quantity': 'sum'
    }).reset_index()
    sms_counts = sms_counts.rename(columns={'usage_event_billing_quantity': 'total_sms_count'})
else:
    sms_counts = pd.DataFrame(columns=['msisdn', 'total_sms_count'])

# Count Voice calls (count of events since each call is one event)
if 'usage_event_type_id' in weekly_usage.columns:
    voice_counts = weekly_usage[weekly_usage['usage_event_type_id'].isin(voice_event_ids)]
    voice_counts = voice_counts.groupby('msisdn').size().reset_index()
    voice_counts = voice_counts.rename(columns={0: 'total_voice_call_count'})
else:
    voice_counts = pd.DataFrame(columns=['msisdn', 'total_voice_call_count'])

# Merge counts
qualifying_report = qualifying_with_details.merge(
    sms_counts, 
    on='msisdn', 
    how='left'
).merge(
    voice_counts, 
    on='msisdn', 
    how='left'
)

# Fill NaN values
qualifying_report['total_sms_count'] = qualifying_report['total_sms_count'].fillna(0).astype(int)
qualifying_report['total_voice_call_count'] = qualifying_report['total_voice_call_count'].fillna(0).astype(int)

# Fill missing names
qualifying_report['first_name'] = qualifying_report['first_name'].fillna('Unknown')
qualifying_report['last_name'] = qualifying_report['last_name'].fillna('Subscriber')
qualifying_report['region'] = qualifying_report['region'].fillna('Unknown')

# Create final report
final_report_columns = [
    'first_name', 'last_name', 'msisdn', 'total_weekly_revenue',
    'total_sms_count', 'total_voice_call_count', 'region'
]

# Only include columns that exist
available_columns = [col for col in final_report_columns if col in qualifying_report.columns]
final_report = qualifying_report[available_columns]

# Format and save
report_date = datetime.now().strftime('%Y%m%d')
if 'usage_event_date_time' in all_usage_data.columns and not all_usage_data['usage_event_date_time'].isna().all():
    week_start_str = week_start.strftime('%Y%m%d')
    week_end_str = week_end.strftime('%Y%m%d')
else:
    week_start_str = "Unknown"
    week_end_str = "Unknown"

print(f"\n=== WEEKLY QUALIFICATION REPORT ===")
print(f"Report Date: {report_date}")
print(f"Week: {week_start_str} to {week_end_str}")
print(f"Qualifying Subscribers: {final_report.shape[0]}")
print(f"Total Qualified Revenue: R{qualifying_report['total_weekly_revenue'].sum():.2f}")
print(f"Subscribers with complete details: {(qualifying_report['first_name'] != 'Unknown').sum()}")

# Save report
output_dir = os.path.join(data_dir, 'processed')
os.makedirs(output_dir, exist_ok=True)

report_filename = f"weekly_qualification_report_{report_date}.csv"
report_path = os.path.join(output_dir, report_filename)

final_report.to_csv(report_path, index=False)
print(f"\nReport saved to: {report_path}")
print("Data preparation complete! Ready for visualization.")