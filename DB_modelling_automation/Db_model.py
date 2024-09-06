import json
from sqlalchemy import create_engine, inspect, text, Table, Column, Integer, String, JSON
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from getpass import getpass

# Base class for declarative models  #used to define tables
Base = declarative_base()

# Define tables for storing schema metadata and foreign keys  #
schema_metadata_table = Table(
    'schema_metadata', Base.metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),  # Primary key, auto-incremented integer.
    Column('schema_name', String, nullable=False),  # name of the schema
    Column('table_name', String, nullable=False),   # name of the table 
    Column('table_metadata', JSON, nullable=False)  # Store table metadata as JSON JSON column to store metadata about the table.
)

foreign_key_relations_table = Table(
    'foreign_key_relations', Base.metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('schema_name', String, nullable=False),
    Column('table_name', String, nullable=False),
    Column('fk_constraints', JSON, nullable=False)  # Store foreign key constraints as JSON
)

# Creates a database engine for connecting to PostgreSQL.
def create_db_engine(username=None, password=None):
    if username and password:
        return create_engine(f'postgresql://{username}:{password}@localhost:5432/postgres')
    elif username:
        return create_engine(f'postgresql://{username}@localhost:5432/postgres')
    else:
        return create_engine('postgresql://postgres@localhost:5432/postgres')

#Tries to connect to the database with various username/password combinations. 
# If all fail, it prompts the user for credentials and attempts to connect again.
def attempt_connection():
    attempts = [
        (None, None),  # Try default connection
        ('postgres', None),  # Try with 'postgres' user, no password
    ]
    
    for username, password in attempts:
        try:
            engine = create_db_engine(username, password)
            with engine.connect():
                print(f"Successfully connected to PostgreSQL with user: {username or 'default'}")
                return engine
        except OperationalError:
            pass
    
    # If all attempts fail, prompt for credentials
    print("Automatic connection failed. Please enter your PostgreSQL credentials.")
    username = input("Username: ")
    password = getpass("Password: ")
    try:
        engine = create_db_engine(username, password)
        with engine.connect():
            print(f"Successfully connected to PostgreSQL with user: {username}")
            return engine
    except OperationalError as e:
        print(f"Failed to connect to PostgreSQL: {str(e)}")
        print("Please ensure PostgreSQL is running and accessible, and that your credentials are correct.")
        return None

#Lists non-system schemas in the database by using the SQLAlchemy inspector.
def list_schemas(engine):
    inspector = inspect(engine)
    schemas = inspector.get_schema_names()
    return [schema for schema in schemas if not schema.startswith('pg_') and schema != 'information_schema']

# Retrieves the list of non-template databases from PostgreSQL.
def get_databases(engine):
    with engine.connect() as connection:
        result = connection.execute(text("SELECT datname FROM pg_database WHERE datistemplate = false;"))
        return [row[0] for row in result]


# Retrieves schema metadata including tables, primary keys, and foreign keys for the specified schema.
def get_schema_metadata(engine, schema_name):
    inspector = inspect(engine)
    
    # Get tables
    tables = inspector.get_table_names(schema=schema_name)
    metadata = {
        'tables': tables,
        'primary_keys': {},
        'foreign_keys': {}
    }
    
    # Get primary keys
    for table in tables:
        pk_columns = inspector.get_pk_constraint(table, schema=schema_name)
        metadata['primary_keys'][table] = pk_columns['constrained_columns']
    
    # Get foreign keys
    for table in tables:
        fk_constraints = inspector.get_foreign_keys(table, schema=schema_name)
        metadata['foreign_keys'][table] = [fk['constrained_columns'] for fk in fk_constraints]
    
    return metadata

def store_metadata_and_keys(engine, schema_name, table_metadata, foreign_keys):
    # Create tables if they do not exist
    Base.metadata.create_all(engine)
    
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Insert table metadata
        for table_name, meta in table_metadata.items():
            meta_entry = {
                'schema_name': schema_name,
                'table_name': table_name,
                'table_metadata': json.dumps(meta)  # Convert metadata to JSON
            }
            print(f"Inserting metadata for table: {table_name}")
            print(f"Metadata entry: {meta_entry}")
            session.execute(schema_metadata_table.insert().values(meta_entry))
        
        # Insert foreign key constraints
        for table_name, fks in foreign_keys.items():
            fk_entry = {
                'schema_name': schema_name,
                'table_name': table_name,
                'fk_constraints': json.dumps(fks)  # Convert foreign key constraints to JSON
            }
            print(f"Inserting foreign key constraints for table: {table_name}")
            print(f"Foreign key entry: {fk_entry}")
            session.execute(foreign_key_relations_table.insert().values(fk_entry))
        
        # Commit the session to save the data
        session.commit()
        print("Metadata and foreign keys successfully stored.")
    except Exception as e:
        session.rollback()  # Rollback in case of error
        print(f"Error storing metadata: {str(e)}")
    finally:
        session.close()

# Attempt to connect
engine = attempt_connection()

if engine:
    # List databases
    databases = get_databases(engine)
    print("Available databases:", databases)
    
    # List schemas in the current database
    schemas = list_schemas(engine)
    print("Available schemas in the current database:", schemas)
    
    # Prompt for schema name
    schema_name = input("Enter the schema name to get metadata: ")

    if schema_name in schemas:
        # Get schema metadata
        metadata = get_schema_metadata(engine, schema_name)
        print(f"Metadata for schema '{schema_name}':", json.dumps(metadata, indent=4))
        
        # Extract metadata and foreign keys
        table_metadata = {
            table: {
                'primary_keys': metadata['primary_keys'].get(table, []),
                'foreign_keys': metadata['foreign_keys'].get(table, [])
            } for table in metadata['tables']
        }
        foreign_keys = metadata['foreign_keys']
        
        print("Table metadata:", table_metadata)
        print("Foreign keys:", foreign_keys)
        
        # Store metadata and keys
        store_metadata_and_keys(engine, schema_name, table_metadata, foreign_keys)
    else:
        print(f"Schema '{schema_name}' does not exist in the current database.")
else:
    print("Unable to connect to the database. Please check your PostgreSQL setup and try again.")
