# # Updated Centralized MDM Example with MongoDB and Sample Datasets

# from pymongo import MongoClient
# import pandas as pd
# from fuzzywuzzy import fuzz
# from datetime import datetime

# # Connect to MongoDB
# client = MongoClient('mongodb://localhost:27017/')
# db = client['mdm_example']

# # Create collections for different "systems"
# crm_collection = db['crm_system']
# erp_collection = db['erp_system']
# ecommerce_collection = db['ecommerce_system']
# mdm_hub_collection = db['mdm_hub']

# # Clear existing data (for demonstration purposes)
# crm_collection.delete_many({})
# erp_collection.delete_many({})
# ecommerce_collection.delete_many({})
# mdm_hub_collection.delete_many({})

# # Sample data for CRM system
# crm_data = [
#     {"customer_id": "C001", "name": "John Smith", "email": "john.smith@email.com", "phone": "123-456-7890"},
#     {"customer_id": "C002", "name": "Jane Doe", "email": "jane.doe@email.com", "phone": "234-567-8901"},
#     {"customer_id": "C003", "name": "Mike Johnson", "email": "mike.johnson@email.com", "phone": "345-678-9012"},
#     {"customer_id": "C004", "name": "Emily Brown", "email": "emily.brown@email.com", "phone": "456-789-0123"},
#     {"customer_id": "C005", "name": "David Lee", "email": "david.lee@email.com", "phone": "567-890-1234"},
#     {"customer_id": "C006", "name": "Sarah Wilson", "email": "sarah.wilson@email.com", "phone": "678-901-2345"},
#     {"customer_id": "C007", "name": "Chris Taylor", "email": "chris.taylor@email.com", "phone": "789-012-3456"},
#     {"customer_id": "C008", "name": "Amanda Clark", "email": "amanda.clark@email.com", "phone": "890-123-4567"},
#     {"customer_id": "C009", "name": "Robert White", "email": "robert.white@email.com", "phone": "901-234-5678"},
#     {"customer_id": "C010", "name": "Jessica Brown", "email": "jessica.brown@email.com", "phone": "012-345-6789"}
# ]

# # Sample data for ERP system
# erp_data = [
#     {"customer_id": "CUST001", "name": "John Smith", "billing_address": "123 Main St, City1, State1", "credit_limit": 5000},
#     {"customer_id": "CUST002", "name": "Jane Doe", "billing_address": "456 Oak Ave, City2, State2", "credit_limit": 7500},
#     {"customer_id": "CUST003", "name": "Mike Johnson", "billing_address": "789 Pine Rd, City3, State3", "credit_limit": 10000},
#     {"customer_id": "CUST004", "name": "Emily Brown", "billing_address": "101 Elm St, City4, State4", "credit_limit": 6000},
#     {"customer_id": "CUST005", "name": "David Lee", "billing_address": "202 Maple Dr, City5, State5", "credit_limit": 8000},
#     {"customer_id": "CUST006", "name": "Sarah Wilson", "billing_address": "303 Cedar Ln, City6, State6", "credit_limit": 5500},
#     {"customer_id": "CUST007", "name": "Chris Taylor", "billing_address": "404 Birch Blvd, City7, State7", "credit_limit": 7000},
#     {"customer_id": "CUST008", "name": "Amanda Clark", "billing_address": "505 Walnut St, City8, State8", "credit_limit": 9000},
#     {"customer_id": "CUST009", "name": "Robert White", "billing_address": "606 Pineapple Pl, City9, State9", "credit_limit": 6500},
#     {"customer_id": "CUST010", "name": "Jessica Brown", "billing_address": "707 Apple Ave, City10, State10", "credit_limit": 8500}
# ]

# # Sample data for E-commerce system
# ecommerce_data = [
#     {"user_id": "U001", "name": "John S.", "email": "john.s@email.com", "shipping_address": "123 Main Street, City1, State1", "last_purchase_date": "2023-01-15"},
#     {"user_id": "U002", "name": "Jane D.", "email": "jane.d@email.com", "shipping_address": "456 Oak Avenue, City2, State2", "last_purchase_date": "2023-02-20"},
#     {"user_id": "U003", "name": "Michael J.", "email": "michael.j@email.com", "shipping_address": "789 Pine Road, City3, State3", "last_purchase_date": "2023-03-10"},
#     {"user_id": "U004", "name": "Emily B.", "email": "emily.b@email.com", "shipping_address": "101 Elm Street, City4, State4", "last_purchase_date": "2023-04-05"},
#     {"user_id": "U005", "name": "David L.", "email": "david.l@email.com", "shipping_address": "202 Maple Drive, City5, State5", "last_purchase_date": "2023-05-12"},
#     {"user_id": "U006", "name": "Sarah W.", "email": "sarah.w@email.com", "shipping_address": "303 Cedar Lane, City6, State6", "last_purchase_date": "2023-06-18"},
#     {"user_id": "U007", "name": "Christopher T.", "email": "christopher.t@email.com", "shipping_address": "404 Birch Boulevard, City7, State7", "last_purchase_date": "2023-07-22"},
#     {"user_id": "U008", "name": "Amanda C.", "email": "amanda.c@email.com", "shipping_address": "505 Walnut Street, City8, State8", "last_purchase_date": "2023-08-30"},
#     {"user_id": "U009", "name": "Rob W.", "email": "rob.w@email.com", "shipping_address": "606 Pineapple Place, City9, State9", "last_purchase_date": "2023-09-14"},
#     {"user_id": "U010", "name": "Jessica B.", "email": "jessica.b@email.com", "shipping_address": "707 Apple Avenue, City10, State10", "last_purchase_date": "2023-10-01"}
# ]

# # Function to load data into MongoDB
# def load_data_to_mongodb():
#     crm_collection.insert_many(crm_data)
#     erp_collection.insert_many(erp_data)
#     ecommerce_collection.insert_many(ecommerce_data)
#     print("Data loaded into MongoDB collections.")

# # Function to perform fuzzy matching
# def fuzzy_match(str1, str2, threshold=80):
#     return fuzz.ratio(str1.lower(), str2.lower()) >= threshold

# # Function to merge customer data
# def merge_customer_data(customer1, customer2):
#     merged = {}
#     for key in set(customer1.keys()) | set(customer2.keys()):
#         if key in customer1 and key in customer2:
#             merged[key] = customer1[key] if len(str(customer1[key])) >= len(str(customer2[key])) else customer2[key]
#         elif key in customer1:
#             merged[key] = customer1[key]
#         else:
#             merged[key] = customer2[key]
#     return merged

# # Function to update MDM hub
# def update_mdm_hub():
#     all_customers = list(crm_collection.find()) + list(erp_collection.find()) + list(ecommerce_collection.find())
    
#     for customer in all_customers:
#         existing = mdm_hub_collection.find_one({"name": customer["name"]})
#         if existing:
#             updated = merge_customer_data(existing, customer)
#             mdm_hub_collection.update_one({"_id": existing["_id"]}, {"$set": updated})
#         else:
#             mdm_hub_collection.insert_one(customer)

#     # Perform fuzzy matching to find and merge similar records
#     hub_customers = list(mdm_hub_collection.find())
#     for i, customer1 in enumerate(hub_customers):
#         for customer2 in hub_customers[i+1:]:
#             if fuzzy_match(customer1["name"], customer2["name"]):
#                 merged = merge_customer_data(customer1, customer2)
#                 mdm_hub_collection.update_one({"_id": customer1["_id"]}, {"$set": merged})
#                 mdm_hub_collection.delete_one({"_id": customer2["_id"]})

# # Function to create a new customer in MDM
# def create_customer_in_mdm(customer_data):
#     customer_data["created_at"] = datetime.now()
#     customer_data["source"] = "MDM"
#     result = mdm_hub_collection.insert_one(customer_data)
#     return result.inserted_id

# # Function to publish updates to source systems
# def publish_updates_to_sources():
#     for customer in mdm_hub_collection.find():
#         if "customer_id" in customer and customer["customer_id"].startswith("C"):
#             crm_collection.update_one({"customer_id": customer["customer_id"]}, {"$set": customer}, upsert=True)
#         if "customer_id" in customer and customer["customer_id"].startswith("CUST"):
#             erp_collection.update_one({"customer_id": customer["customer_id"]}, {"$set": customer}, upsert=True)
#         if "user_id" in customer:
#             ecommerce_collection.update_one({"user_id": customer["user_id"]}, {"$set": customer}, upsert=True)

# # Main function to orchestrate the MDM process
# def main():
#     # Load initial data
#     load_data_to_mongodb()
    
#     # Update MDM hub
#     update_mdm_hub()
    
#     # Create a new customer
#     new_customer_id = create_customer_in_mdm({
#         "name": "Alice Johnson",
#         "email": "alice.johnson@email.com",
#         "phone": "345-678-9012",
#         "address": "456 Oak Ave, City5, State5"
#     })
    
#     # Publish updates back to source systems
#     publish_updates_to_sources()
    
#     # Display final state of collections
#     print("\nMDM Hub:")
#     print(pd.DataFrame(list(mdm_hub_collection.find())))

#     print("\nCRM System:")
#     print(pd.DataFrame(list(crm_collection.find())))

#     print("\nERP System:")
#     print(pd.DataFrame(list(erp_collection.find())))

#     print("\nE-commerce System:")
#     print(pd.DataFrame(list(ecommerce_collection.find())))

# # Run the main function
# if __name__ == "__main__":
#     main()

# # Close the MongoDB connection
# client.close()





##############################################################################################################

from pymongo import MongoClient
import pandas as pd
from fuzzywuzzy import fuzz
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['mdm_example']

# Create collections for different "systems"
crm_collection = db['crm_system']
erp_collection = db['erp_system']
ecommerce_collection = db['ecommerce_system']
mdm_hub_collection = db['mdm_hub']

# Clear existing data (for demonstration purposes)
crm_collection.delete_many({})
erp_collection.delete_many({})
ecommerce_collection.delete_many({})
mdm_hub_collection.delete_many({})

# Sample data for CRM system
crm_data = [
    {"customer_id": "C001", "name": "John Smith", "email": "john.smith@email.com", "phone": "123-456-7890"},
    {"customer_id": "C002", "name": "Jane Doe", "email": "jane.doe@email.com", "phone": "234-567-8901"},
    {"customer_id": "C003", "name": "Mike Johnson", "email": "mike.johnson@email.com", "phone": "345-678-9012"},
    {"customer_id": "C004", "name": "Emily Brown", "email": "emily.brown@email.com", "phone": "456-789-0123"},
    {"customer_id": "C005", "name": "David Lee", "email": "david.lee@email.com", "phone": "567-890-1234"},
    {"customer_id": "C006", "name": "Sarah Wilson", "email": "sarah.wilson@email.com", "phone": "678-901-2345"},
    {"customer_id": "C007", "name": "Chris Taylor", "email": "chris.taylor@email.com", "phone": "789-012-3456"},
    {"customer_id": "C008", "name": "Amanda Clark", "email": "amanda.clark@email.com", "phone": "890-123-4567"},
    {"customer_id": "C009", "name": "Robert White", "email": "robert.white@email.com", "phone": "901-234-5678"},
    {"customer_id": "C010", "name": "Jessica Brown", "email": "jessica.brown@email.com", "phone": "012-345-6789"}
]

# Sample data for ERP system
erp_data = [
    {"customer_id": "CUST001", "name": "John Smith", "billing_address": "123 Main St, City1, State1", "credit_limit": 5000},
    {"customer_id": "CUST002", "name": "Jane Doe", "billing_address": "456 Oak Ave, City2, State2", "credit_limit": 7500},
    {"customer_id": "CUST003", "name": "Mike Johnson", "billing_address": "789 Pine Rd, City3, State3", "credit_limit": 10000},
    {"customer_id": "CUST004", "name": "Emily Brown", "billing_address": "101 Elm St, City4, State4", "credit_limit": 6000},
    {"customer_id": "CUST005", "name": "David Lee", "billing_address": "202 Maple Dr, City5, State5", "credit_limit": 8000},
    {"customer_id": "CUST006", "name": "Sarah Wilson", "billing_address": "303 Cedar Ln, City6, State6", "credit_limit": 5500},
    {"customer_id": "CUST007", "name": "Chris Taylor", "billing_address": "404 Birch Blvd, City7, State7", "credit_limit": 7000},
    {"customer_id": "CUST008", "name": "Amanda Clark", "billing_address": "505 Walnut St, City8, State8", "credit_limit": 9000},
    {"customer_id": "CUST009", "name": "Robert White", "billing_address": "606 Pineapple Pl, City9, State9", "credit_limit": 6500},
    {"customer_id": "CUST010", "name": "Jessica Brown", "billing_address": "707 Apple Ave, City10, State10", "credit_limit": 8500}
]

# Sample data for E-commerce system
ecommerce_data = [
    {"user_id": "U001", "name": "John S.", "email": "john.s@email.com", "shipping_address": "123 Main Street, City1, State1", "last_purchase_date": "2023-01-15"},
    {"user_id": "U002", "name": "Jane D.", "email": "jane.d@email.com", "shipping_address": "456 Oak Avenue, City2, State2", "last_purchase_date": "2023-02-20"},
    {"user_id": "U003", "name": "Michael J.", "email": "michael.j@email.com", "shipping_address": "789 Pine Road, City3, State3", "last_purchase_date": "2023-03-10"},
    {"user_id": "U004", "name": "Emily B.", "email": "emily.b@email.com", "shipping_address": "101 Elm Street, City4, State4", "last_purchase_date": "2023-04-05"},
    {"user_id": "U005", "name": "David L.", "email": "david.l@email.com", "shipping_address": "202 Maple Drive, City5, State5", "last_purchase_date": "2023-05-12"},
    {"user_id": "U006", "name": "Sarah W.", "email": "sarah.w@email.com", "shipping_address": "303 Cedar Lane, City6, State6", "last_purchase_date": "2023-06-18"},
    {"user_id": "U007", "name": "Christopher T.", "email": "christopher.t@email.com", "shipping_address": "404 Birch Boulevard, City7, State7", "last_purchase_date": "2023-07-22"},
    {"user_id": "U008", "name": "Amanda C.", "email": "amanda.c@email.com", "shipping_address": "505 Walnut Street, City8, State8", "last_purchase_date": "2023-08-30"},
    {"user_id": "U009", "name": "Rob W.", "email": "rob.w@email.com", "shipping_address": "606 Pineapple Place, City9, State9", "last_purchase_date": "2023-09-14"},
    {"user_id": "U010", "name": "Jessica B.", "email": "jessica.b@email.com", "shipping_address": "707 Apple Avenue, City10, State10", "last_purchase_date": "2023-10-01"}
]

# Function to load data into MongoDB
def load_data_to_mongodb():
    crm_collection.insert_many(crm_data)
    erp_collection.insert_many(erp_data)
    ecommerce_collection.insert_many(ecommerce_data)
    print("Data loaded into MongoDB collections.")

# Function to perform fuzzy matching
def fuzzy_match(str1, str2, threshold=80):
    return fuzz.ratio(str1.lower(), str2.lower()) >= threshold

# Function to merge customer data
def merge_customer_data(customer1, customer2):
    merged = {}
    for key in set(customer1.keys()) | set(customer2.keys()):
        if key in customer1 and key in customer2:
            merged[key] = customer1[key] if len(str(customer1[key])) >= len(str(customer2[key])) else customer2[key]
        elif key in customer1:
            merged[key] = customer1[key]
        else:
            merged[key] = customer2[key]
    return merged

# Function to update MDM hub
def update_mdm_hub():
    all_customers = list(crm_collection.find()) + list(erp_collection.find()) + list(ecommerce_collection.find())
    
    for customer in all_customers:
        existing = mdm_hub_collection.find_one({"name": customer["name"]})
        if existing:
            updated = merge_customer_data(existing, customer)
            mdm_hub_collection.update_one({"_id": existing["_id"]}, {"$set": updated})
        else:
            mdm_hub_collection.insert_one(customer)

    # Perform fuzzy matching to find and merge similar records
    hub_customers = list(mdm_hub_collection.find())
    for i, customer1 in enumerate(hub_customers):
        for customer2 in hub_customers[i+1:]:
            if fuzzy_match(customer1["name"], customer2["name"]):
                merged = merge_customer_data(customer1, customer2)
                mdm_hub_collection.update_one({"_id": customer1["_id"]}, {"$set": merged})
                mdm_hub_collection.delete_one({"_id": customer2["_id"]})

# Function to create a new customer in MDM
def create_customer_in_mdm(customer_data):
    customer_data['created_at'] = datetime.utcnow()
    mdm_hub_collection.insert_one(customer_data)

# Function to publish updates back to the source systems
def publish_updates_to_sources():
    for customer in mdm_hub_collection.find():
        try:
            if "customer_id" in customer and customer["customer_id"].startswith("C"):
                crm_data = {k: v for k, v in customer.items() if k not in ["_id", "user_id"]}
                crm_collection.update_one({"customer_id": customer["customer_id"]}, {"$set": crm_data}, upsert=True)
            
            if "customer_id" in customer and customer["customer_id"].startswith("CUST"):
                erp_data = {k: v for k, v in customer.items() if k not in ["_id", "user_id"]}
                erp_collection.update_one({"customer_id": customer["customer_id"]}, {"$set": erp_data}, upsert=True)
            
            if "user_id" in customer:
                ecommerce_data = {k: v for k, v in customer.items() if k not in ["_id", "customer_id"]}
                ecommerce_collection.update_one({"user_id": customer["user_id"]}, {"$set": ecommerce_data}, upsert=True)
        except Exception as e:
            print(f"Error updating customer {customer}: {e}")

# Load initial data into MongoDB collections
load_data_to_mongodb()

# Update MDM hub with data from different systems
update_mdm_hub()

# Publish the updates back to the source systems
publish_updates_to_sources()

print("Process completed.")
