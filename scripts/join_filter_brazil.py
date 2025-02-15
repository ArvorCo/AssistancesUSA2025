import pandas as pd

# Updated prime and sub file lists for Assistance
# Updated prime_paths list for Assistance to include all _1 and _2 files
prime_paths = [
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_PrimeTransactions_2025-02-06_H06M45S05_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_PrimeTransactions_2025-02-06_H06M52S59_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_PrimeTransactions_2025-02-07_H08M14S20_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_PrimeTransactions_2025-02-07_H08M16S21_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_PrimeTransactions_2025-02-07_H08M16S21_2.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_PrimeTransactions_2025-02-07_H08M17S31_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_PrimeTransactions_2025-02-07_H08M17S31_2.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_PrimeTransactions_2025-02-07_H08M17S52_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_PrimeTransactions_2025-02-07_H08M19S56_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_PrimeTransactions_2025-02-07_H08M19S56_2.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_PrimeTransactions_2025-02-07_H08M20S30_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_PrimeTransactions_2025-02-07_H08M20S44_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_PrimeTransactions_2025-02-07_H17M59S45_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_PrimeTransactions_2025-02-07_H18M17S39_1.txt'
]
sub_paths = [
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_Subawards_2025-02-06_H06M32S35_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_Subawards_2025-02-06_H06M32S36_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_Subawards_2025-02-07_H08M03S25_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_Subawards_2025-02-07_H08M04S43_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_Subawards_2025-02-07_H08M04S57_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_Subawards_2025-02-07_H08M06S32_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_Subawards_2025-02-07_H08M06S38_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_Subawards_2025-02-07_H08M06S40_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_Subawards_2025-02-07_H08M07S30_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_Subawards_2025-02-07_H17M48S57_1.txt',
    '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/All_Assistance_Subawards_2025-02-07_H17M49S49_1.txt'
]
output_path = '/Users/leonardodias/Documents/Arvor/AssistancesUSA2025/data/Brazil_Joined_Summary.csv'

# Updated output_fields with extra organizational and text description fields
output_fields = [
    'award_unique_key',
    'award_id_fain',
    'federal_action_obligation',
    'total_obligated_amount',
    'transaction_description',
    'prime_award_base_transaction_description',
    'action_date',  # new date field added
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
    'subawardee_country_name'
]

# Read and combine all prime and sub files
prime_df = pd.concat([pd.read_csv(p, delimiter='|', dtype=str) for p in prime_paths], ignore_index=True)
sub_df = pd.concat([pd.read_csv(p, delimiter='|', dtype=str) for p in sub_paths], ignore_index=True)

# Merge prime with sub using a left join (include all prime records)
merged = pd.merge(prime_df, sub_df, left_on='assistance_award_unique_key', right_on='prime_award_unique_key', how='left', suffixes=('_prime', '_sub'))

# Build a safe mask for the description field:
if 'prime_award_base_transaction_description' in merged.columns:
    prime_desc_mask = merged['prime_award_base_transaction_description'].fillna('').str.lower().str.contains('brazil')
else:
    prime_desc_mask = pd.Series(False, index=merged.index)

mask = (
    merged['recipient_country_name'].fillna('').str.lower().str.contains('brazil') |
    merged['primary_place_of_performance_country_name'].fillna('').str.lower().str.contains('brazil') |
    merged['subawardee_country_name'].fillna('').str.lower().str.contains('brazil') |
    prime_desc_mask |
    merged['subaward_description'].fillna('').str.lower().str.contains('brazil') |
    merged['transaction_description'].fillna('').str.lower().str.contains('brazil')
)
filtered = merged[mask]

# After filtering, define a dynamic column for description
desc_col = 'prime_award_base_transaction_description' if 'prime_award_base_transaction_description' in filtered.columns else 'transaction_description'

# Build the joined summary DataFrame with additional prime fields, using the dynamic description column
summary = pd.DataFrame({
    'award_unique_key': filtered['assistance_award_unique_key'],
    'award_id_fain': filtered['award_id_fain'],
    'federal_action_obligation': filtered['federal_action_obligation'],
    'total_obligated_amount': filtered['total_obligated_amount'],
    'transaction_description': filtered['transaction_description'],  # existing mapping for backup
    'prime_award_base_transaction_description': filtered[desc_col],    # dynamic mapping
    'action_date': filtered['action_date'],  # new mapping for date
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
    'subawardee_country_name': filtered['subawardee_country_name'].fillna('')
})

# Save the result using pipe delimiter, and print the count of records found
summary.to_csv(output_path, index=False, sep='|')
print(f"Found {len(summary)} joined records containing 'Brazil'. Summary saved to {output_path}")
