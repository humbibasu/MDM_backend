# import pandas as pd
# from pymongo import MongoClient
# from fuzzywuzzy import fuzz
# from datetime import datetime

# # MongoDB connection setup
# client = MongoClient('mongodb://localhost:27017/')
# db = client['mdm_example']

# def load_data(collection_name):
#     collection = db[collection_name]
#     data = list(collection.find({}))
#     return pd.DataFrame(data)

# def normalize_columns(df, id_col, name_col, email_col, **kwargs):
#     column_mapping = {
#         id_col: 'id',
#         name_col: 'name',
#         email_col: 'email'
#     }
#     column_mapping.update(kwargs)
#     return df.rename(columns=column_mapping)

# # Load and normalize data from all collections
# crm_df = normalize_columns(load_data('crm_system'), 'customer_id', 'name', 'email', phone='phone')
# erp_df = normalize_columns(load_data('erp_system'), 'customer_id', 'name', 'email', billing_address='address')
# ecommerce_df = normalize_columns(load_data('ecommerce_system'), 'user_id', 'name', 'email', shipping_address='address')

# # Combine all data into a single DataFrame
# combined_df = pd.concat([crm_df, erp_df, ecommerce_df], ignore_index=True)

# def create_golden_record_and_update_status(df):
#     columns = ['id'] + [col for col in df.columns if col != 'id' and col != '_id']
    
#     golden_records = []
#     contributing_records = set()  # To keep track of records contributing to the golden record
    
#     for id_val, group in df.groupby('id'):
#         golden_record = {}
#         for col in columns:
#             if col in group.columns:
#                 non_null_values = group[col].dropna()
#                 if not non_null_values.empty:
#                     golden_record[col] = non_null_values.iloc[0]  # Take the first non-null value
        
#         # Add the golden record to the list
#         golden_records.append(golden_record)
        
#         # Track contributing records
#         for idx, row in group.iterrows():
#             if all(row[col] == golden_record[col] for col in columns if col in golden_record):
#                 contributing_records.add(row['id'])

#     # Create DataFrames from the lists
#     golden_record_df = pd.DataFrame(golden_records)
    
#     # Update records that contributed to the golden record
#     updated_df = df[df['id'].isin(contributing_records)].copy()
#     updated_df['status'] = 'end-dated'  # Or 'inactive'
#     updated_df['reference_tag'] = 'Referenced'
#     updated_df['end_date'] = datetime.now()

#     return golden_record_df, updated_df

# # Apply the function
# golden_record_df, updated_df = create_golden_record_and_update_status(combined_df)

# # Debug: Print golden_record_df info
# print("Golden Record DataFrame Info:")
# print(golden_record_df.info())
# print("\nGolden Record Columns:")
# print(golden_record_df.columns.tolist())

# # Debug: Print updated_df info
# print("\nUpdated DataFrame Info:")
# print(updated_df.info())
# print("\nUpdated DataFrame Columns:")
# print(updated_df.columns.tolist())

# def save_to_mongodb(df, collection_name):
#     if not df.empty:
#         collection = db[collection_name]
#         collection.delete_many({})  # Clear existing records
#         collection.insert_many(df.to_dict('records'))
#         print(f"Data saved to collection: {collection_name}")
#     else:
#         print(f"No data to save for collection: {collection_name}")

# # Save golden record and updated records back to MongoDB
# save_to_mongodb(golden_record_df, 'Golden2')  # Save to 'Golden2' collection
# save_to_mongodb(updated_df, 'mdm_example_updated3')  # Save the updated DataFrame back to MongoDB

# print("\nGolden Record DataFrame:")
# print(golden_record_df)

# print("\nUpdated DataFrame with statuses:")
# print(updated_df)

# print("Process completed successfully.")


#########################################

# import pandas as pd
# from pymongo import MongoClient
# from datetime import datetime

# # MongoDB connection setup
# client = MongoClient('mongodb://localhost:27017/')
# db = client['mdm_example']

# def load_data(collection_name):
#     collection = db[collection_name]
#     data = list(collection.find({}))
#     return pd.DataFrame(data)

# def normalize_columns(df, id_col, name_col, email_col, **kwargs):
#     column_mapping = {
#         id_col: 'id',
#         name_col: 'name',
#         email_col: 'email'
#     }
#     column_mapping.update(kwargs)
#     return df.rename(columns=column_mapping)

# # Load and normalize data from all collections
# crm_df = normalize_columns(load_data('crm_system'), 'customer_id', 'name', 'email', phone='phone')
# erp_df = normalize_columns(load_data('erp_system'), 'customer_id', 'name', 'email', billing_address='address')
# ecommerce_df = normalize_columns(load_data('ecommerce_system'), 'user_id', 'name', 'email', shipping_address='address')

# # Combine all data into a single DataFrame
# combined_df = pd.concat([crm_df, erp_df, ecommerce_df], ignore_index=True)

# def create_golden_record_and_update_status(df):
#     columns = ['id'] + [col for col in df.columns if col != 'id' and col != '_id']
    
#     golden_record = {}
#     contributing_records = set()  # To keep track of records contributing to the golden record
    
#     for id_val, group in df.groupby('id'):
#         # Create the golden record by selecting the first non-null value for each column
#         record_data = {}
#         for col in columns:
#             if col in group.columns:
#                 non_null_values = group[col].dropna()
#                 if not non_null_values.empty:
#                     record_data[col] = non_null_values.iloc[0]  # Take the first non-null value

#         # Add the golden record to the list
#         if not golden_record:
#             golden_record = record_data
#         else:
#             # Update existing golden record if needed
#             for col in columns:
#                 if col in record_data and col not in golden_record:
#                     golden_record[col] = record_data[col]
    
#         # Track contributing records
#         contributing_records.update(group['id'])
    
#     # Create DataFrame for the golden record
#     golden_record_df = pd.DataFrame([golden_record])
    
#     # Update records that contributed to the golden record
#     updated_df = df[df['id'].isin(contributing_records)].copy()
#     updated_df['status'] = 'end-dated'  # Or 'inactive'
#     updated_df['reference_tag'] = 'Referenced'
#     updated_df['end_date'] = datetime.now()

#     return golden_record_df, updated_df

# # Apply the function
# golden_record_df, updated_df = create_golden_record_and_update_status(combined_df)

# # Debug: Print golden_record_df info
# print("Golden Record DataFrame Info:")
# print(golden_record_df.info())
# print("\nGolden Record Columns:")
# print(golden_record_df.columns.tolist())

# # Debug: Print updated_df info
# print("\nUpdated DataFrame Info:")
# print(updated_df.info())
# print("\nUpdated DataFrame Columns:")
# print(updated_df.columns.tolist())

# def save_to_mongodb(df, collection_name):
#     if not df.empty:
#         collection = db[collection_name]
#         collection.delete_many({})  # Clear existing records
#         collection.insert_many(df.to_dict('records'))
#         print(f"Data saved to collection: {collection_name}")
#     else:
#         print(f"No data to save for collection: {collection_name}")

# # Save golden record and updated records back to MongoDB
# save_to_mongodb(golden_record_df, 'Golden')  # Save to 'Golden2' collection
# save_to_mongodb(updated_df, 'mdm_example_updated')  # Save the updated DataFrame back to MongoDB

# print("\nGolden Record DataFrame:")
# print(golden_record_df)

# print("\nUpdated DataFrame with statuses:")
# print(updated_df)

# print("Process completed successfully.")



#################################################

import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017/')
db = client['mdm_example']

def load_data(collection_name):
    collection = db[collection_name]
    data = list(collection.find({}))
    return pd.DataFrame(data)

def normalize_columns(df, id_col, name_col, email_col, **kwargs):
    column_mapping = {
        id_col: 'id',
        name_col: 'name',
        email_col: 'email'
    }
    column_mapping.update(kwargs)
    return df.rename(columns=column_mapping)

# Load and normalize data from all collections
crm_df = normalize_columns(load_data('crm_system'), 'customer_id', 'name', 'email', phone='phone')
erp_df = normalize_columns(load_data('erp_system'), 'customer_id', 'name', 'email', billing_address='address')
ecommerce_df = normalize_columns(load_data('ecommerce_system'), 'user_id', 'name', 'email', shipping_address='address')

# Combine all data into a single DataFrame
combined_df = pd.concat([crm_df, erp_df, ecommerce_df], ignore_index=True)

def create_golden_record_and_update_status(df):
    columns = ['id'] + [col for col in df.columns if col != 'id']
    
    golden_record = {}
    contributing_records_ids = set()
    
    for id_val, group in df.groupby('id'):
        # Create the golden record by selecting the first non-null value for each column
        record_data = {}
        for col in columns:
            if col in group.columns:
                non_null_values = group[col].dropna()
                if not non_null_values.empty:
                    record_data[col] = non_null_values.iloc[0]  # Take the first non-null value

        if not golden_record:
            golden_record = record_data
        else:
            # Update existing golden record if needed
            for col in columns:
                if col in record_data and (col not in golden_record or pd.isnull(golden_record[col])):
                    golden_record[col] = record_data[col]

        # Track contributing records' IDs
        contributing_records_ids.update(group['id'])

    # Create DataFrame for the golden record
    golden_record_df = pd.DataFrame([golden_record])
    
    # Update records that contributed to the golden record
    updated_df = df[df['id'].isin(contributing_records_ids)].copy()
    updated_df['status'] = 'end-dated'  # Or 'inactive'
    updated_df['reference_tag'] = 'Referenced'
    updated_df['end_date'] = datetime.now()

    return golden_record_df, updated_df

# Apply the function
golden_record_df, updated_df = create_golden_record_and_update_status(combined_df)

# Debug: Print golden_record_df info
print("Golden Record DataFrame Info:")
print(golden_record_df.info())
print("\nGolden Record Columns:")
print(golden_record_df.columns.tolist())

# Debug: Print updated_df info
print("\nUpdated DataFrame Info:")
print(updated_df.info())
print("\nUpdated DataFrame Columns:")
print(updated_df.columns.tolist())

def save_to_mongodb(df, collection_name):
    if not df.empty:
        collection = db[collection_name]
        collection.delete_many({})  # Clear existing records
        collection.insert_many(df.to_dict('records'))
        print(f"Data saved to collection: {collection_name}")
    else:
        print(f"No data to save for collection: {collection_name}")

# Save golden record and updated records back to MongoDB
save_to_mongodb(golden_record_df, 'Golden3')  # Save to 'Golden2' collection
save_to_mongodb(updated_df, 'mdm_example_updated')  # Save the updated DataFrame back to MongoDB

print("\nGolden Record DataFrame:")
print(golden_record_df)

print("\nUpdated DataFrame with statuses:")
print(updated_df)

print("Process completed successfully.")

