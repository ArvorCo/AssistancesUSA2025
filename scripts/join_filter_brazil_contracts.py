import os
import sys
import pandas as pd

# Updated prime and sub file lists for Contracts
prime_paths = [
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_PrimeTransactions_2025-02-06_H06M35S30_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_PrimeTransactions_2025-02-06_H06M35S34_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_PrimeTransactions_2025-02-07_H08M05S18_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_PrimeTransactions_2025-02-07_H08M05S35_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_PrimeTransactions_2025-02-07_H08M07S00_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_PrimeTransactions_2025-02-07_H08M07S15_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_PrimeTransactions_2025-02-07_H08M07S17_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_PrimeTransactions_2025-02-07_H08M07S24_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_PrimeTransactions_2025-02-07_H08M07S52_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_PrimeTransactions_2025-02-07_H17M51S35_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_PrimeTransactions_2025-02-07_H17M52S53_1.txt'
]
sub_paths = [
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_Subawards_2025-02-06_H06M30S48_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_Subawards_2025-02-06_H06M30S49_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_Subawards_2025-02-07_H08M01S43_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_Subawards_2025-02-07_H08M03S20_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_Subawards_2025-02-07_H08M03S41_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_Subawards_2025-02-07_H08M05S10_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_Subawards_2025-02-07_H08M05S52_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_Subawards_2025-02-07_H08M06S18_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_Subawards_2025-02-07_H08M07S04_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_Subawards_2025-02-07_H17M47S01_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Contracts_Subawards_2025-02-07_H17M48S01_1.txt'
]
output_path = '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/Brazil_Joined_Contracts_Summary.csv'

# Check if prime file exists
for prime_path in prime_paths:
    if not os.path.exists(prime_path):
        print(f"Error: Prime contracts file not found at {prime_path}")
        sys.exit(1)

# Define output fields (updated to match the assistance script fields)
output_fields = [
    'contract_award_unique_key',
    'award_id_piid',
    'federal_action_obligation',
    'total_dollars_obligated',
    'prime_award_base_transaction_description',  # updated column for description
    'action_date',
    'awarding_agency_name',
    'awarding_sub_agency_name',
    'recipient_name',
    'recipient_parent_name',
    'recipient_country_name',
    'primary_place_of_performance_country_name',
    'subaward_number',
    'subaward_amount',
    'subaward_action_date',
    'subaward_type',
    'subaward_description',
    'subawardee_name',
    'subawardee_country_name',
    'foreign_funding_description',   # new field from prime file
    'recipient_phone_number',        # new field from prime file
    'recipient_country'              # duplicate from recipient_country_name if needed
]

# Read and combine all prime and sub files for contracts
prime_df = pd.concat([pd.read_csv(p, delimiter='|', dtype=str) for p in prime_paths], ignore_index=True)
sub_df = pd.concat([pd.read_csv(p, delimiter='|', dtype=str) for p in sub_paths], ignore_index=True)

# Merge using prime key "contract_award_unique_key" and sub key "prime_award_unique_key"
merged = pd.merge(prime_df, sub_df, left_on='contract_award_unique_key', right_on='prime_award_unique_key', how='left', suffixes=('_prime', '_sub'))

# Build a safe mask for description checking:
prime_desc_mask = (merged['prime_award_base_transaction_description']
                   .fillna('')
                   .str.lower()
                   .str.contains('brazil')) if 'prime_award_base_transaction_description' in merged.columns else pd.Series(False, index=merged.index)

mask = (
    merged['recipient_country_name'].fillna('').str.lower().str.contains('brazil') |
    merged['primary_place_of_performance_country_name'].fillna('').str.lower().str.contains('brazil') |
    merged['subawardee_country_name'].fillna('').str.lower().str.contains('brazil') |
    prime_desc_mask |
    merged['subaward_description'].fillna('').str.lower().str.contains('brazil') |
    merged['foreign_funding_description'].fillna('').str.lower().str.contains('brazil')
)
filtered = merged[mask]

# Select dynamic description column safely:
desc_col = 'prime_award_base_transaction_description' if 'prime_award_base_transaction_description' in filtered.columns else 'foreign_funding_description'

# Build the summary DataFrame using updated fields and dynamic description column
summary = pd.DataFrame({
    'contract_award_unique_key': filtered['contract_award_unique_key'],
    'award_id_piid': filtered['award_id_piid'],
    'federal_action_obligation': filtered['federal_action_obligation'],
    'total_dollars_obligated': filtered['total_dollars_obligated'],
    'prime_award_base_transaction_description': filtered[desc_col],  # updated mapping
    'action_date': filtered['action_date'],
    'awarding_agency_name': filtered['awarding_agency_name'],
    'awarding_sub_agency_name': filtered['awarding_sub_agency_name'],
    'recipient_name': filtered['recipient_name'],
    'recipient_parent_name': filtered['recipient_parent_name'],
    'recipient_country_name': filtered['recipient_country_name'],
    'primary_place_of_performance_country_name': filtered['primary_place_of_performance_country_name'],
    'subaward_number': filtered['subaward_number'].fillna(''),
    'subaward_amount': filtered['subaward_amount'].fillna(''),
    'subaward_action_date': filtered['subaward_action_date'].fillna(''),
    'subaward_type': filtered['subaward_type'].fillna(''),
    'subaward_description': filtered['subaward_description'].fillna(''),
    'subawardee_name': filtered['subawardee_name'].fillna(''),
    'subawardee_country_name': filtered['subawardee_country_name'].fillna(''),
    'foreign_funding_description': filtered['foreign_funding_description'],
    'recipient_phone_number': filtered['recipient_phone_number'],
    'recipient_country': filtered['recipient_country_name']
})

# Save the result using pipe delimiter, and print the count of records found
summary.to_csv(output_path, index=False, sep='|')
print(f"Found {len(summary)} joined contract records containing 'Brazil'. Summary saved to {output_path}")
