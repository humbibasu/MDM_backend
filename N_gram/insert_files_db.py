# import os
# import pandas as pd
# from pymongo import MongoClient

# # Get the directory of the current script
# script_dir = os.path.dirname(os.path.abspath(__file__))

# # Construct the full path to the CSV file
# csv_path = os.path.join(script_dir, 'customer_data.csv')

# print(f"Looking for file at: {csv_path}")
# print(f"File exists: {os.path.exists(csv_path)}")
# print(f"File is readable: {os.access(csv_path, os.R_OK)}")

# # Try to read file contents
# with open(csv_path, 'r') as f:
#     print("File contents (first 500 characters):")
#     print(f.read()[:500])

# # Try to read with pandas
# try:
#     df = pd.read_csv(csv_path)
#     print(f"Successfully read {len(df)} rows")
#     print("First few rows:")
#     print(df.head())

#     # Connect to MongoDB
#     client = MongoClient('mongodb://localhost:27017/')
#     db = client['MDM']
#     collection = db['customer_records']

#     # Convert DataFrame to list of dictionaries
#     records = df.to_dict('records')

#     # Insert the records into MongoDB
#     result = collection.insert_many(records)

#     print(f"Inserted {len(result.inserted_ids)} documents into MongoDB")

#     # Close the connection
#     client.close()

# except Exception as e:
#     print(f"Error: {str(e)}")



##################################


# import pandas as pd
# from pymongo import MongoClient
# from datetime import datetime
# import re
# from collections import Counter

# # Standardization functions (keep these the same as in the previous script)
# def standardize_name(name):
#     name = re.sub(r'^(Mr\.|Mrs\.|Ms\.|Dr\.)\s*', '', name, flags=re.IGNORECASE)
#     return ' '.join(name.split(',')[::-1]).strip().title()
# def standardize_address(address):
#     address = address.upper()
#     address = re.sub(r'\bAPT\b', 'APARTMENT', address)
#     address = re.sub(r'\bST\b', 'STREET', address)
#     address = re.sub(r'\bAVE\b', 'AVENUE', address)
#     return address

# def standardize_date(date_str):
#     try:
#         return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
#     except ValueError:
#         try:
#             return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
#         except ValueError:
#             return date_str

# def standardize_phone(phone):
#     digits = re.sub(r'\D', '', phone)
#     if len(digits) == 10:
#         return f"+1-{digits[:3]}-{digits[3:6]}-{digits[6:]}"
#     elif len(digits) == 11 and digits.startswith('1'):
#         return f"+{digits[0]}-{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
#     return phone

# def standardize_email(email):
#     return email.lower()

# def standardize_gender(gender):
#     gender = gender.upper()
#     return 'M' if gender in ['M', 'MALE'] else 'F' if gender in ['F', 'FEMALE'] else gender

# def standardize_salutation(salutation):
#     salutation = salutation.upper().replace('.', '')
#     if salutation in ['MR', 'MISTER']:
#         return 'Mr'
#     elif salutation in ['MS', 'MISS', 'MRS']:
#         return 'Ms'
#     elif salutation in ['DR', 'DOCTOR']:
#         return 'Dr'
#     return salutation

# def standardize_industry_code(code):
#     return code.upper().replace('-', '').replace('_', '')

# # ... (keep all other standardization functions) ...

# # Connect to MongoDB and fetch data
# client = MongoClient('mongodb://localhost:27017/')
# db = client['customer_database']
# collection = db['customer_records']
# data = list(collection.find())
# df = pd.DataFrame(data)

# # Apply standardization
# df['name'] = df['name'].apply(standardize_name)
# df['address'] = df['address'].apply(standardize_address)
# df['date'] = df['date'].apply(standardize_date)
# df['phone'] = df['phone'].apply(standardize_phone)
# df['email'] = df['email'].apply(standardize_email)
# df['gender'] = df['gender'].apply(standardize_gender)
# df['salutation'] = df['salutation'].apply(standardize_salutation)
# df['industry_code'] = df['industry_code'].apply(standardize_industry_code)

# # Function to get most common value, preferring non-empty values
# def most_common_value(series):
#     values = [v for v in series if v]
#     if not values:
#         return ''
#     return Counter(values).most_common(1)[0][0]

# # Create a single golden record
# golden_record = pd.Series({
#     'name': most_common_value(df['name']),
#     'address': most_common_value(df['address']),
#     'date': most_common_value(df['date']),
#     'phone': most_common_value(df['phone']),
#     'email': most_common_value(df['email']),
#     'gender': most_common_value(df['gender']),
#     'salutation': most_common_value(df['salutation']),
#     'industry_code': most_common_value(df['industry_code'])
# })

# # Print the single golden record
# print("\nSingle Golden Record:")
# print("=" * 40)
# print(golden_record.to_string())
# print("\n")

# # Print comparison with original records
# print("Comparison with Original Records:")
# print("=" * 80)
# for _, record in df.iterrows():
#     print("\nOriginal Record:")
#     print("-" * 40)
#     print(record.to_string())
#     print("\nDifferences from Golden Record:")
    
#     # Compare fields that exist in both record and golden_record
#     common_fields = set(record.index) & set(golden_record.index)
#     differences = [field for field in common_fields if record[field] != golden_record[field]]
    
#     if differences:
#         for field in differences:
#             print(f"{field}: Original: {record[field]} | Golden: {golden_record[field]}")
#     else:
#         print("No differences")
#     print("-" * 40)

# # Print summary
# print(f"\nTotal number of original records: {len(df)}")
# print("Number of golden records: 1")

# # Close the connection
# client.close()

#################################



# import pandas as pd
# from pymongo import MongoClient
# from datetime import datetime
# import re
# from collections import Counter

# # Standardization functions
# def standardize_name(name):
#     if pd.isna(name):
#         return name
#     name = re.sub(r'^(Mr\.|Mrs\.|Ms\.|Dr\.)\s*', '', name, flags=re.IGNORECASE)
#     return ' '.join(name.split(',')[::-1]).strip().title()

# def standardize_address(address):
#     if pd.isna(address):
#         return address
#     address = address.upper()
#     address = re.sub(r'\bAPT\b', 'APARTMENT', address)
#     address = re.sub(r'\bST\b', 'STREET', address)
#     address = re.sub(r'\bAVE\b', 'AVENUE', address)
#     return address

# def standardize_date(date_str):
#     if pd.isna(date_str):
#         return date_str
#     try:
#         return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
#     except ValueError:
#         try:
#             return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
#         except ValueError:
#             return date_str

# def standardize_phone(phone):
#     if pd.isna(phone):
#         return phone
#     digits = re.sub(r'\D', '', phone)
#     if len(digits) == 10:
#         return f"+1-{digits[:3]}-{digits[3:6]}-{digits[6:]}"
#     elif len(digits) == 11 and digits.startswith('1'):
#         return f"+{digits[0]}-{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
#     return phone

# def standardize_email(email):
#     if pd.isna(email):
#         return email
#     return email.lower()

# def standardize_gender(gender):
#     if pd.isna(gender):
#         return gender
#     gender = gender.upper()
#     return 'M' if gender in ['M', 'MALE'] else 'F' if gender in ['F', 'FEMALE'] else gender

# def standardize_salutation(salutation):
#     if pd.isna(salutation):
#         return salutation
#     salutation = salutation.upper().replace('.', '')
#     if salutation in ['MR', 'MISTER']:
#         return 'Mr'
#     elif salutation in ['MS', 'MISS', 'MRS']:
#         return 'Ms'
#     elif salutation in ['DR', 'DOCTOR']:
#         return 'Dr'
#     return salutation

# def standardize_industry_code(code):
#     if pd.isna(code):
#         return code
#     return code.upper().replace('-', '').replace('_', '')

# # Connect to MongoDB and fetch data
# client = MongoClient('mongodb://localhost:27017/')
# db = client['MDM']
# collection = db['customer_records']
# data = list(collection.find())
# df = pd.DataFrame(data)

# # Ensure columns exist before applying standardization
# columns_to_standardize = ['name', 'address', 'date', 'phone', 'email', 'gender', 'salutation', 'industry_code']
# for column in columns_to_standardize:
#     if column not in df.columns:
#         df[column] = None  # Add missing columns with NaN values

# # Apply standardization
# df['name'] = df['name'].apply(standardize_name)
# df['address'] = df['address'].apply(standardize_address)
# df['date'] = df['date'].apply(standardize_date)
# df['phone'] = df['phone'].apply(standardize_phone)
# df['email'] = df['email'].apply(standardize_email)
# df['gender'] = df['gender'].apply(standardize_gender)
# df['salutation'] = df['salutation'].apply(standardize_salutation)
# df['industry_code'] = df['industry_code'].apply(standardize_industry_code)

# # Function to get most common value, preferring non-empty values
# def most_common_value(series):
#     values = [v for v in series if v]
#     if not values:
#         return ''
#     return Counter(values).most_common(1)[0][0]

# # Create a single golden record
# golden_record = pd.Series({
#     'name': most_common_value(df['name']),
#     'address': most_common_value(df['address']),
#     'date': most_common_value(df['date']),
#     'phone': most_common_value(df['phone']),
#     'email': most_common_value(df['email']),
#     'gender': most_common_value(df['gender']),
#     'salutation': most_common_value(df['salutation']),
#     'industry_code': most_common_value(df['industry_code'])
# })

# # Print the single golden record
# print("\nSingle Golden Record:")
# print("=" * 40)
# print(golden_record.to_string())
# print("\n")

# # Print comparison with original records
# print("Comparison with Original Records:")
# print("=" * 80)
# for _, record in df.iterrows():
#     print("\nOriginal Record:")
#     print("-" * 40)
#     print(record.to_string())
#     print("\nDifferences from Golden Record:")

#     # Compare fields that exist in both record and golden_record
#     differences = []
#     for field in golden_record.index:
#         if record.get(field) != golden_record[field]:
#             differences.append(field)
    
#     if differences:
#         for field in differences:
#             print(f"{field}: Original: {record.get(field)} | Golden: {golden_record[field]}")
#     else:
#         print("No differences")
#     print("-" * 40)

# # Print summary
# print(f"\nTotal number of original records: {len(df)}")
# print("Number of golden records: 1")

# # Close the connection
# client.close()


#############################

import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import re
from collections import Counter

# Standardization functions
def standardize_name(name):
    name = re.sub(r'^(Mr\.|Mrs\.|Ms\.|Dr\.)\s*', '', name, flags=re.IGNORECASE)
    return ' '.join(name.split(',')[::-1]).strip().title()

def standardize_address(address):
    address = address.upper()
    address = re.sub(r'\bAPT\b', 'APARTMENT', address)
    address = re.sub(r'\bST\b', 'STREET', address)
    address = re.sub(r'\bAVE\b', 'AVENUE', address)
    return address

def standardize_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        try:
            return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
        except ValueError:
            return date_str

def standardize_phone(phone):
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 10:
        return f"+1-{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits.startswith('1'):
        return f"+{digits[0]}-{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    return phone

def standardize_email(email):
    return email.lower()

def standardize_gender(gender):
    gender = gender.upper()
    return 'M' if gender in ['M', 'MALE'] else 'F' if gender in ['F', 'FEMALE'] else gender

def standardize_salutation(salutation):
    salutation = salutation.upper().replace('.', '')
    if salutation in ['MR', 'MISTER']:
        return 'Mr'
    elif salutation in ['MS', 'MISS', 'MRS']:
        return 'Ms'
    elif salutation in ['DR', 'DOCTOR']:
        return 'Dr'
    return salutation

def standardize_industry_code(code):
    return code.upper().replace('-', '').replace('_', '')

# Connect to MongoDB and fetch data
client = MongoClient('mongodb://localhost:27017/')
db = client['MDM']
collection = db['customer_records']

# Limit the number of records to 10 for merging
data = list(collection.find().limit(10))
df = pd.DataFrame(data)

# Check and standardize only if columns exist
if 'name' in df.columns:
    df['name'] = df['name'].apply(standardize_name)
if 'address' in df.columns:
    df['address'] = df['address'].apply(standardize_address)
if 'date' in df.columns:
    df['date'] = df['date'].apply(standardize_date)
if 'phone' in df.columns:
    df['phone'] = df['phone'].apply(standardize_phone)
if 'email' in df.columns:
    df['email'] = df['email'].apply(standardize_email)
if 'gender' in df.columns:
    df['gender'] = df['gender'].apply(standardize_gender)
if 'salutation' in df.columns:
    df['salutation'] = df['salutation'].apply(standardize_salutation)
if 'industry_code' in df.columns:
    df['industry_code'] = df['industry_code'].apply(standardize_industry_code)

# Function to get most common value, preferring non-empty values
def most_common_value(series):
    values = [v for v in series if v]
    if not values:
        return ''
    return Counter(values).most_common(1)[0][0]

# Create a single golden record
golden_record = pd.Series({
    'name': most_common_value(df['name']),
    'address': most_common_value(df['address']),
    'date': most_common_value(df['date']),
    'phone': most_common_value(df['phone']),
    'email': most_common_value(df['email']),
    'gender': most_common_value(df['gender']),
    'salutation': most_common_value(df['salutation']),
    'industry_code': most_common_value(df['industry_code'])
})

# Print the single golden record
print("\nSingle Golden Record:")
print("=" * 40)
print(golden_record.to_string())
print("\n")

# Print comparison with original records
print("Comparison with Original Records:")
print("=" * 80)
for _, record in df.iterrows():
    print("\nOriginal Record:")
    print("-" * 40)
    print(record.to_string())
    print("\nDifferences from Golden Record:")
    
    # Compare fields that exist in both record and golden_record
    common_fields = set(record.index) & set(golden_record.index)
    differences = [field for field in common_fields if record[field] != golden_record[field]]
    
    if differences:
        for field in differences:
            print(f"{field}: Original: {record[field]} | Golden: {golden_record[field]}")
    else:
        print("No differences")
    print("-" * 40)

# Print summary
print(f"\nTotal number of original records: {len(df)}")
print("Number of golden records: 1")

# Close the connection
client.close()

