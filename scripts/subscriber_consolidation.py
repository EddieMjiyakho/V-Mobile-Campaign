# scripts/subscriber_consolidation.py

import pandas as pd
import os


# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
# Go up one level to project root, then to data folder
project_root = os.path.dirname(script_dir)
data_dir = os.path.join(project_root, 'data', 'raw')

# Define file paths for clarity
vmobile_path = os.path.join(data_dir, "VMobile_subscribers.csv")
bluemobile_path = os.path.join(data_dir, "VMobile_subscribers_bluemobile.csv")
arrowmobile_path = os.path.join(data_dir, "VMobile_subscribers_arrowmobile.csv")

# Load the three subscriber datasets
print("Loading subscriber data...")
df_vmobile = pd.read_csv(vmobile_path, delimiter=';')
df_bluemobile = pd.read_csv(bluemobile_path, delimiter=';')
df_arrowmobile = pd.read_csv(arrowmobile_path, delimiter=';')

# DEBUG: Check the actual column names
print("V Mobile columns:", df_vmobile.columns.tolist())
print("BlueMobile columns:", df_bluemobile.columns.tolist())
print("ArrowMobile columns:", df_arrowmobile.columns.tolist())

# Display the shape (rows, columns) of each to verify load
print(f"V Mobile data: {df_vmobile.shape}")
print(f"BlueMobile data: {df_bluemobile.shape}")
print(f"ArrowMobile data: {df_arrowmobile.shape}")

# Standardize V Mobile Columns
# Note: 'Location' is the Region. 'Birthday' is Date of Birth.
df_vmobile_standardized = df_vmobile.rename(columns={
    'Cell Number': 'cell_phone_number',
    'SIM Activation Date': 'sim_activation_date',
    'First Name': 'first_name',
    'Last Name': 'last_name',
    'Birthday': 'date_of_birth',
    'Location': 'region'
})
# Add a column to track the source system
df_vmobile_standardized['source_system_name'] = 'VMobile'

# Standardize BlueMobile Columns
# Note: 'Activate' is SIM Activation Date. 'Name' is First Name. 'Surname' is Last Name.
df_bluemobile_standardized = df_bluemobile.rename(columns={
    'Cell': 'cell_phone_number',
    'Activate': 'sim_activation_date',
    'Name': 'first_name',
    'Surname': 'last_name',
    'Date': 'date_of_birth',  # Assuming 'Date' is Date of Birth
    'City': 'region'
})
df_bluemobile_standardized['source_system_name'] = 'BlueMobile'

# Standardize ArrowMobile Columns
df_arrowmobile_standardized = df_arrowmobile.rename(columns={
    'CellNo': 'cell_phone_number',
    'SIMDate': 'sim_activation_date',
    'FirstName': 'first_name',
    'LastName': 'last_name',
    # ArrowMobile file doesn't have Date of Birth or Region in the screenshot
    'Area': 'region'
})
# Add missing columns to match the others
df_arrowmobile_standardized['date_of_birth'] = None  # Or pd.NA
df_arrowmobile_standardized['source_system_name'] = 'ArrowMobile'

# Combine all standardized DataFrames
print("Combining all subscriber data...")
combined_subscribers = pd.concat(
    [df_vmobile_standardized, df_bluemobile_standardized, df_arrowmobile_standardized],
    ignore_index=True  # This resets the index so it's continuous
)

print(f"Combined data shape: {combined_subscribers.shape}")
print(combined_subscribers.head())

# First, ensure 'sim_activation_date' is a datetime object for correct comparison
combined_subscribers['sim_activation_date'] = pd.to_datetime(combined_subscribers['sim_activation_date'], dayfirst=True, errors='coerce')

# Define the priority of source systems for tie-breaking
# Lower number = higher priority
source_priority = {'VMobile': 1, 'BlueMobile': 2, 'ArrowMobile': 3}
combined_subscribers['source_priority'] = combined_subscribers['source_system_name'].map(source_priority)

# Step 1: Sort the entire dataframe by our business rules.
# We sort by phone number, then by priority (VMobile first), then by SIM date (newest first).
combined_subscribers_sorted = combined_subscribers.sort_values(
    by=['cell_phone_number', 'source_priority', 'sim_activation_date'],
    ascending=[True, True, False]
)

# Step 2: Mark the first occurrence of each phone number as the master record.
combined_subscribers_sorted['is_master_record'] = False
combined_subscribers_sorted.loc[~combined_subscribers_sorted.duplicated(subset=['cell_phone_number']), 'is_master_record'] = True

# Let's create our final outputs
print("Creating final output tables...")

# Output 1: The combined table with the master record flag
final_combined_table = combined_subscribers_sorted.copy()

# Output 2: A table containing ONLY the master records
master_records_table = final_combined_table[final_combined_table['is_master_record'] == True].copy()

print(f"Final combined table shape: {final_combined_table.shape}")
print(f"Master records table shape: {master_records_table.shape}")
print(f"Number of unique subscribers: {master_records_table['cell_phone_number'].nunique()}")

# Save the results to the processed data folder
output_dir = "data/processed/"
# os.makedirs(output_dir, exist_ok=True)  # Create folder if it doesn't exist

final_combined_table.to_csv(os.path.join(output_dir, "combined_subscribers_all.csv"), index=False)
master_records_table.to_csv(os.path.join(output_dir, "combined_subscribers_master.csv"), index=False)

print("Data preparation complete! Files saved to 'data/processed/'")
print("1. 'combined_subscribers_all.csv' - All records with master flag")
print("2. 'combined_subscribers_master.csv' - Only master records")