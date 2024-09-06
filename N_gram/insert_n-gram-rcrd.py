from pymongo import MongoClient

# MongoDB connection
client = MongoClient("mongodb://localhost:27017")
db = client.MDM
collection = db.n_gram_records


# Sample dataset with 40 records
sample_data = [
    {"name": "John Smith", "address": "123 Main St, New York, NY", "phone": "212-555-1234"},
    {"name": "Jane Doe", "address": "456 Elm St, Los Angeles, CA", "phone": "310-555-5678"},
    {"name": "Bob Johnson", "address": "789 Oak Ave, Chicago, IL", "phone": "312-555-9012"},
    {"name": "Alice Brown", "address": "321 Pine Rd, Houston, TX", "phone": "713-555-3456"},
    {"name": "Charlie Davis", "address": "654 Maple Dr, Phoenix, AZ", "phone": "602-555-7890"},
    {"name": "Eva Wilson", "address": "987 Cedar Ln, Philadelphia, PA", "phone": "215-555-2345"},
    {"name": "Frank Miller", "address": "159 Birch St, San Antonio, TX", "phone": "210-555-6789"},
    {"name": "Grace Taylor", "address": "753 Walnut Ave, San Diego, CA", "phone": "619-555-0123"},
    {"name": "Henry Anderson", "address": "246 Spruce Ct, Dallas, TX", "phone": "214-555-4567"},
    {"name": "Ivy Martin", "address": "135 Ash Blvd, San Jose, CA", "phone": "408-555-8901"},
    {"name": "Jack Thompson", "address": "864 Oakwood Dr, Austin, TX", "phone": "512-555-2345"},
    {"name": "Karen White", "address": "975 Elmwood Rd, Jacksonville, FL", "phone": "904-555-6789"},
    {"name": "Leo Garcia", "address": "531 Pinecrest Ave, San Francisco, CA", "phone": "415-555-0123"},
    {"name": "Mia Rodriguez", "address": "642 Maplewood St, Columbus, OH", "phone": "614-555-4567"},
    {"name": "Nathan Lee", "address": "753 Cedarwood Ln, Fort Worth, TX", "phone": "817-555-8901"},
    {"name": "Olivia Clark", "address": "864 Birchwood Ct, Charlotte, NC", "phone": "704-555-2345"},
    {"name": "Paul Harris", "address": "975 Oakleaf Dr, Seattle, WA", "phone": "206-555-6789"},
    {"name": "Quinn Murphy", "address": "186 Pinegrove Rd, Denver, CO", "phone": "303-555-0123"},
    {"name": "Rachel King", "address": "297 Mapleleaf Ave, Washington, DC", "phone": "202-555-4567"},
    {"name": "Samuel Scott", "address": "408 Cedarhill St, Boston, MA", "phone": "617-555-8901"},
    {"name": "Tina Bell", "address": "532 Oak St, Miami, FL", "phone": "305-555-5678"},
    {"name": "Uma Green", "address": "123 Elm Dr, Atlanta, GA", "phone": "404-555-1234"},
    {"name": "Victor Black", "address": "987 Pine St, Portland, OR", "phone": "503-555-5678"},
    {"name": "Wendy Hill", "address": "654 Maple Ave, Nashville, TN", "phone": "615-555-9012"},
    {"name": "Xander Young", "address": "321 Spruce Rd, Albuquerque, NM", "phone": "505-555-3456"},
    {"name": "Yvonne Adams", "address": "864 Cedar Ave, Tucson, AZ", "phone": "520-555-7890"},
    {"name": "Zara Clark", "address": "135 Walnut Dr, Mesa, AZ", "phone": "480-555-0123"},
    {"name": "Aaron King", "address": "753 Birch Blvd, Fresno, CA", "phone": "559-555-4567"},
    {"name": "Bella Scott", "address": "975 Maple Ct, Omaha, NE", "phone": "402-555-2345"},
    {"name": "Carl Lewis", "address": "246 Pine Blvd, Las Vegas, NV", "phone": "702-555-6789"},
    {"name": "Diana Ross", "address": "531 Elmwood Ave, Kansas City, MO", "phone": "816-555-0123"},
    {"name": "Evan Parker", "address": "642 Cedar Dr, Sacramento, CA", "phone": "916-555-4567"},
    {"name": "Fiona Griffin", "address": "753 Oakwood Ln, Milwaukee, WI", "phone": "414-555-8901"},
    {"name": "George Turner", "address": "864 Pinehill Rd, Minneapolis, MN", "phone": "612-555-2345"},
    {"name": "Holly Murphy", "address": "975 Spruceleaf St, Detroit, MI", "phone": "313-555-6789"},
    {"name": "Iris Campbell", "address": "186 Maplewood Ln, Memphis, TN", "phone": "901-555-0123"},
    {"name": "Jake Martinez", "address": "297 Oakwood Dr, Louisville, KY", "phone": "502-555-4567"},
    {"name": "Liam Phillips", "address": "408 Pinegrove Ave, Baltimore, MD", "phone": "410-555-8901"},
    {"name": "Mason Brooks", "address": "123 Cedarhill Ct, Richmond, VA", "phone": "804-555-2345"},
    {"name": "Noah Bennett", "address": "456 Mapleleaf Ln, Raleigh, NC", "phone": "919-555-6789"},
    {"name": "Olivia Walker", "address": "789 Pinecrest Ave, Arlington, VA", "phone": "703-555-0123"}
]

def insert_data():
    # Check if data already exists
    if collection.count_documents({}) == 0:
        collection.insert_many(sample_data)
        print(f"{len(sample_data)} records inserted successfully.")
    else:
        print("Data already exists in the database. No new records inserted.")

if __name__ == "__main__":
    insert_data()
