"""
MongoDB to PostgreSQL ETL Script
Handles nested arrays and objects by creating relational tables
"""

import pymongo
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import json
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import re

class MongoToPostgresETL:
    def __init__(self, mongo_config: Dict, postgres_config: Dict):
        """
        Initialize ETL with connection configurations
        
        Args:
            mongo_config: {'host': 'localhost', 'port': 27017, 'db': 'source_db', 
                          'username': 'user', 'password': 'pass'}
            postgres_config: {'host': 'localhost', 'port': 5432, 'db': 'target_db',
                            'user': 'user', 'password': 'pass'}
        """
        self.mongo_config = mongo_config
        self.postgres_config = postgres_config
        self.mongo_client = None
        self.mongo_db = None
        self.pg_conn = None
        self.pg_cursor = None
        self.schema_map = {}
        
        # PostgreSQL reserved keywords that need to be quoted
        self.postgres_reserved_words = {
            'limit', 'group', 'order', 'user', 'primary', 'key', 'foreign', 
            'references', 'select', 'insert', 'update', 'delete', 'where',
            'from', 'table', 'database', 'schema', 'column', 'index',
            'values', 'set', 'into', 'as', 'on', 'join', 'left', 'right',
            'inner', 'outer', 'full', 'cross', 'natural', 'using', 'having',
            'distinct', 'all', 'any', 'some', 'exists', 'not', 'and', 'or',
            'between', 'in', 'like', 'ilike', 'similar', 'to', 'is', 'null',
            'true', 'false', 'unknown', 'case', 'when', 'then', 'else', 'end',
            'cast', 'extract', 'date', 'time', 'timestamp', 'interval', 'year',
            'month', 'day', 'hour', 'minute', 'second', 'zone', 'current_date',
            'current_time', 'current_timestamp', 'localtime', 'localtimestamp',
            'session_user', 'current_user', 'current_catalog', 'current_schema',
            'current_role', 'grant', 'revoke', 'privileges', 'public', 'option',
            'cascade', 'restrict', 'alter', 'add', 'drop', 'rename', 'column',
            'constraint', 'default', 'check', 'unique', 'primary', 'foreign',
            'create', 'table', 'database', 'schema', 'view', 'sequence', 'index',
            'trigger', 'function', 'procedure', 'language', 'plpgsql', 'sql',
            'return', 'returns', 'begin', 'end', 'declare', 'variable', 'cursor',
            'for', 'loop', 'while', 'if', 'else', 'elsif', 'then', 'exit',
            'continue', 'raise', 'exception', 'when', 'others', 'transaction',
            'commit', 'rollback', 'savepoint', 'release', 'isolation', 'level',
            'read', 'write', 'serializable', 'repeatable', 'read', 'committed',
            'uncommitted', 'vacuum', 'analyze', 'explain', 'verbose', 'costs',
            'buffers', 'timing', 'format', 'text', 'xml', 'json', 'csv'
        }
        
    def connect_mongodb(self):
        """Establish MongoDB connection"""
        try:
            # Build connection string with proper authentication
            if self.mongo_config.get('username'):
                auth_source = self.mongo_config.get('auth_db', 'admin')  # Default to 'admin'
                connection_string = (
                    f"mongodb://{self.mongo_config['username']}:{self.mongo_config['password']}"
                    f"@{self.mongo_config['host']}:{self.mongo_config['port']}"
                    f"/{self.mongo_config['db']}?authSource={auth_source}&directConnection=true"
                )
            else:
                connection_string = (
                    f"mongodb://{self.mongo_config['host']}:{self.mongo_config['port']}"
                    f"/{self.mongo_config['db']}?directConnection=true"
                )
            
            self.mongo_client = pymongo.MongoClient(
                connection_string,
                serverSelectionTimeoutMS=5000
            )
            
            # Test connection
            self.mongo_client.admin.command('ping')
            self.mongo_db = self.mongo_client[self.mongo_config['db']]
            print(f"✓ Connected to MongoDB: {self.mongo_config['db']}")
        except Exception as e:
            print(f"✗ MongoDB connection failed: {e}")
            raise
    
    def connect_postgresql(self):
        """Establish PostgreSQL connection and create database if it doesn't exist"""
        target_db = self.postgres_config['db']
        
        try:
            # First, connect to the default 'postgres' database to check/create target db
            temp_conn = psycopg2.connect(
                host=self.postgres_config['host'],
                port=self.postgres_config['port'],
                database='postgres',  # Connect to default database
                user=self.postgres_config['user'],
                password=self.postgres_config['password']
            )
            temp_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            temp_cursor = temp_conn.cursor()
            
            # Check if target database exists
            temp_cursor.execute(
                "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
                (target_db,)
            )
            exists = temp_cursor.fetchone()
            
            if not exists:
                # Create the database
                print(f"Database '{target_db}' does not exist. Creating...")
                temp_cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db))
                )
                print(f"✓ Created database: {target_db}")
            else:
                print(f"Database '{target_db}' already exists")
            
            # Close temporary connection
            temp_cursor.close()
            temp_conn.close()
            
            # Now connect to the target database
            self.pg_conn = psycopg2.connect(
                host=self.postgres_config['host'],
                port=self.postgres_config['port'],
                database=target_db,
                user=self.postgres_config['user'],
                password=self.postgres_config['password']
            )
            self.pg_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.pg_cursor = self.pg_conn.cursor()
            print(f"✓ Connected to PostgreSQL: {target_db}")
            
        except Exception as e:
            print(f"✗ PostgreSQL connection failed: {e}")
            raise
    
    def sanitize_name(self, name: str) -> str:
        """Sanitize collection/field names for PostgreSQL"""
        # Remove special characters and replace with underscore
        name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # Ensure it doesn't start with a number
        if name[0].isdigit():
            name = 'col_' + name
        return name.lower()
    
    def handle_reserved_keywords(self, field_name: str) -> str:
        """Handle PostgreSQL reserved keywords by renaming them"""
        sanitized = self.sanitize_name(field_name)
        
        # If it's a reserved word, add a prefix
        if sanitized in self.postgres_reserved_words:
            return f'field_{sanitized}'
        return sanitized
    
    def infer_postgres_type(self, value: Any) -> str:
        """Infer PostgreSQL data type from Python value"""
        if value is None:
            return "TEXT"
        elif isinstance(value, bool):
            return "BOOLEAN"
        elif isinstance(value, int):
            return "BIGINT"
        elif isinstance(value, float):
            return "DOUBLE PRECISION"
        elif isinstance(value, str):
            return "TEXT"
        elif isinstance(value, dict):
            return "JSONB"
        elif isinstance(value, list):
            return "JSONB"  # Will be handled separately if it's array of objects
        else:
            return "TEXT"
    
    def analyze_collection_schema(self, collection_name: str, sample_size: int = 100) -> Dict:
        """
        Analyze MongoDB collection to infer schema
        Identifies nested arrays that should become separate tables
        """
        collection = self.mongo_db[collection_name]
        documents = list(collection.find().limit(sample_size))
        
        if not documents:
            return {'fields': {}, 'nested_arrays': {}}
        
        schema = {
            'fields': defaultdict(set),
            'nested_arrays': {},
            'nested_objects': []
        }
        
        def analyze_document(doc, prefix=''):
            for key, value in doc.items():
                if key == '_id':
                    continue
                    
                field_path = f"{prefix}{key}" if prefix else key
                
                if isinstance(value, list) and value:
                    # Check if it's an array of objects
                    if isinstance(value[0], dict):
                        schema['nested_arrays'][field_path] = []
                        for item in value:
                            if isinstance(item, dict):
                                schema['nested_arrays'][field_path].append(item)
                    else:
                        # Array of primitives - store as JSONB or separate table
                        schema['fields'][field_path].add('JSONB')
                elif isinstance(value, dict):
                    # Nested object - flatten it
                    analyze_document(value, f"{field_path}_")
                else:
                    schema['fields'][field_path].add(self.infer_postgres_type(value))
        
        for doc in documents:
            analyze_document(doc)
        
        # Determine final type for each field (most specific)
        final_schema = {
            'fields': {},
            'nested_arrays': schema['nested_arrays']
        }
        
        for field, types in schema['fields'].items():
            types_list = list(types)
            if 'TEXT' in types_list:
                final_schema['fields'][field] = 'TEXT'
            elif 'JSONB' in types_list:
                final_schema['fields'][field] = 'JSONB'
            elif 'DOUBLE PRECISION' in types_list:
                final_schema['fields'][field] = 'DOUBLE PRECISION'
            elif 'BIGINT' in types_list:
                final_schema['fields'][field] = 'BIGINT'
            else:
                final_schema['fields'][field] = types_list[0]
        
        return final_schema
    
    def create_table(self, table_name: str, fields: Dict[str, str], parent_table: str = None):
        """Create PostgreSQL table with given schema"""
        sanitized_table = self.sanitize_name(table_name)
        
        # Start with ID column
        columns = ["id SERIAL PRIMARY KEY"]
        
        # Add parent reference if this is a child table
        if parent_table:
            parent_id_col = f"{self.sanitize_name(parent_table)}_id"
            columns.append(f"{parent_id_col} BIGINT REFERENCES {self.sanitize_name(parent_table)}(id)")
        
        # Add mongo_id to maintain reference to original document
        if not parent_table:
            columns.append("mongo_id TEXT UNIQUE")
        
        # Add other fields with reserved keyword handling
        for field, field_type in fields.items():
            sanitized_field = self.handle_reserved_keywords(field)
            
            # Avoid conflicts with reserved/auto-generated columns
            if sanitized_field == 'id':
                sanitized_field = 'original_id'
            elif sanitized_field == 'mongo_id' and not parent_table:
                sanitized_field = 'mongo_field_id'
            
            columns.append(f"{sanitized_field} {field_type}")
        
        create_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(sanitized_table),
            sql.SQL(', '.join(columns))
        )
        
        try:
            self.pg_cursor.execute(create_query)
            print(f"✓ Created table: {sanitized_table}")
        except Exception as e:
            print(f"✗ Error creating table {sanitized_table}: {e}")
            print(f"  Table columns: {columns}")
            raise
    
    def build_field_mapping(self, collection_name: str, schema: Dict) -> Dict[str, str]:
        """Build a mapping of MongoDB field names to PostgreSQL column names"""
        field_map = {'_id': 'mongo_id'}  # Always map _id to mongo_id
        
        for field in schema['fields'].keys():
            pg_column = self.handle_reserved_keywords(field)
            
            # Apply same transformations as create_table
            if pg_column == 'id':
                pg_column = 'original_id'
            elif pg_column == 'mongo_id':
                pg_column = 'mongo_field_id'
            
            field_map[field] = pg_column
        
        return field_map

    def get_table_columns(self, table_name: str) -> set:
        """Get actual column names from PostgreSQL table"""
        sanitized_table = self.sanitize_name(table_name)
        try:
            self.pg_cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (sanitized_table,))
            return {row[0] for row in self.pg_cursor.fetchall()}
        except Exception as e:
            print(f"Warning: Could not get columns for {sanitized_table}: {e}")
            return set()
    
    def insert_data(self, table_name: str, data: List[Dict], parent_table: str = None, field_mapping: Dict[str, str] = None):
        """Insert data into PostgreSQL table with robust error handling"""
        if not data:
            return
        
        sanitized_table = self.sanitize_name(table_name)
        
        # Get actual columns in the table
        table_columns = self.get_table_columns(table_name)
        
        success_count = 0
        error_count = 0
        
        for record in data:
            try:
                # Prepare columns and values
                columns = []
                values = []
                
                for key, value in record.items():
                    # Use field mapping if provided, otherwise sanitize
                    if field_mapping and key in field_mapping:
                        pg_column = field_mapping[key]
                    else:
                        pg_column = self.handle_reserved_keywords(key)
                        
                        # Apply same renaming logic as create_table
                        if pg_column == 'id':
                            pg_column = 'original_id'
                        elif pg_column == 'mongo_id' and key != 'mongo_id':
                            pg_column = 'mongo_field_id'
                    
                    # Skip if column doesn't exist in table
                    if pg_column not in table_columns:
                        continue
                    
                    columns.append(pg_column)
                    
                    # Handle special types
                    if isinstance(value, (dict, list)):
                        values.append(json.dumps(value))
                    elif isinstance(value, bool):
                        values.append(value)
                    else:
                        values.append(value)
                
                # Skip if no valid columns
                if not columns:
                    error_count += 1
                    continue
                
                # Create INSERT query
                insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
                    sql.Identifier(sanitized_table),
                    sql.SQL(', '.join([sql.Identifier(col).as_string(self.pg_cursor) for col in columns])),
                    sql.SQL(', '.join(['%s'] * len(values)))
                )
                
                self.pg_cursor.execute(insert_query, values)
                result_id = self.pg_cursor.fetchone()[0]
                success_count += 1
                return result_id  # Return inserted ID for parent records
                
            except Exception as e:
                error_count += 1
                # Log error but continue processing
                if error_count <= 3:  # Only show first 3 errors to avoid spam
                    print(f"  ⚠ Error inserting record (error {error_count}): {str(e)[:100]}")
                continue
        
        if error_count > 3:
            print(f"  ⚠ Total insertion errors: {error_count} (suppressed {error_count - 3} messages)")
        
        return None
    
    def process_collection(self, collection_name: str):
        """Process a MongoDB collection and migrate to PostgreSQL"""
        print(f"\n{'='*60}")
        print(f"Processing collection: {collection_name}")
        print(f"{'='*60}")
        
        # Analyze schema
        schema = self.analyze_collection_schema(collection_name)
        self.schema_map[collection_name] = schema
        
        # Build field mapping
        field_mapping = self.build_field_mapping(collection_name, schema)
        
        # Create main table
        self.create_table(collection_name, schema['fields'])
        
        # Create tables for nested arrays
        nested_mappings = {}
        for nested_field, sample_data in schema['nested_arrays'].items():
            if sample_data:
                nested_schema = defaultdict(set)
                for item in sample_data:
                    for key, value in item.items():
                        nested_schema[key].add(self.infer_postgres_type(value))
                
                nested_fields = {
                    field: list(types)[0] for field, types in nested_schema.items()
                }
                
                nested_table_name = f"{collection_name}_{nested_field}"
                self.create_table(nested_table_name, nested_fields, parent_table=collection_name)
                nested_mappings[nested_field] = self.build_field_mapping(nested_table_name, {'fields': nested_fields})
        
        # Migrate data
        collection = self.mongo_db[collection_name]
        total_docs = collection.count_documents({})
        print(f"Migrating {total_docs} documents...")
        
        processed = 0
        successful = 0
        failed = 0
        
        for doc in collection.find():
            try:
                # Extract nested arrays
                nested_arrays_data = {}
                for nested_field in schema['nested_arrays'].keys():
                    keys = nested_field.split('_')
                    value = doc
                    for key in keys:
                        value = value.get(key, [])
                        if not isinstance(value, dict):
                            break
                    
                    if isinstance(value, list):
                        nested_arrays_data[nested_field] = value
                
                # Prepare main record
                main_record = {'mongo_id': str(doc.get('_id', f'generated_{processed}'))}
                
                def flatten_doc(d, prefix=''):
                    for key, value in d.items():
                        if key == '_id':
                            continue
                        
                        field_path = f"{prefix}{key}" if prefix else key
                        
                        if field_path in schema['nested_arrays']:
                            continue
                        elif isinstance(value, dict):
                            flatten_doc(value, f"{field_path}_")
                        elif isinstance(value, list):
                            main_record[field_path] = value
                        else:
                            main_record[field_path] = value
                
                flatten_doc(doc)
                
                # Insert main record with field mapping
                parent_id = self.insert_data(collection_name, [main_record], field_mapping=field_mapping)
                
                if parent_id:
                    successful += 1
                    
                    # Insert nested array records
                    for nested_field, nested_data in nested_arrays_data.items():
                        if nested_data and isinstance(nested_data, list):
                            nested_table = f"{collection_name}_{nested_field}"
                            parent_ref_col = f"{self.sanitize_name(collection_name)}_id"
                            
                            for item in nested_data:
                                if isinstance(item, dict):
                                    nested_record = {parent_ref_col: parent_id}
                                    nested_record.update(item)
                                    self.insert_data(
                                        nested_table, 
                                        [nested_record], 
                                        parent_table=collection_name,
                                        field_mapping=nested_mappings.get(nested_field)
                                    )
                else:
                    failed += 1
                
                processed += 1
                if processed % 100 == 0:
                    print(f"  Processed: {processed}/{total_docs} (✓ {successful}, ✗ {failed})")
                    
            except Exception as e:
                failed += 1
                if failed <= 3:
                    print(f"  ⚠ Document processing error: {str(e)[:100]}")
        
        print(f"✓ Completed: {collection_name} - {successful} successful, {failed} failed out of {processed} total")
    
    def is_view(self, collection_name: str) -> bool:
        """Check if a collection is a view"""
        try:
            # Get collection info
            coll_info = self.mongo_db.list_collections(filter={'name': collection_name})
            for info in coll_info:
                if info.get('type') == 'view':
                    return True
            return False
        except:
            return False
    def run_etl(self):
        """Execute the complete ETL process"""
        try:
            # Connect to databases
            self.connect_mongodb()
            self.connect_postgresql()
            
            # Get all collections
            all_collections = self.mongo_db.list_collection_names()
            
            # Filter out system collections and views
            collections = []
            for collection_name in all_collections:
                # Skip system collections
                if collection_name.startswith('system.'):
                    print(f"⊘ Skipping system collection: {collection_name}")
                    continue
                
                # Skip views
                if self.is_view(collection_name):
                    print(f"⊘ Skipping view: {collection_name}")
                    continue
                
                collections.append(collection_name)
            
            print(f"\nFound {len(collections)} collections to migrate (skipped {len(all_collections) - len(collections)})")
            
            # Process each collection
            for collection_name in collections:
                self.process_collection(collection_name)
            
            print(f"\n{'='*60}")
            print("ETL Process Completed Successfully!")
            print(f"{'='*60}")
            
        except Exception as e:
            print(f"\n✗ ETL Process Failed: {e}")
            raise
        finally:
            # Close connections
            if self.mongo_client:
                self.mongo_client.close()
            if self.pg_cursor:
                self.pg_cursor.close()
            if self.pg_conn:
                self.pg_conn.close()
            print("\n✓ Connections closed")


# Example usage
if __name__ == "__main__":
    # MongoDB configuration
    mongo_config = {
        'host': 'localhost',
        'port': 27017,
        'db': 'arc-one',
        'username': 'root',  # Set to None if no auth
        'password': 'admin123',
        'auth_db': 'arc-one'
    }
    
    # PostgreSQL configuration
    postgres_config = {
        'host': 'localhost',
        'port': 5432,
        'db': 'target_database5',
        'user': 'user-name',
        'password': 'strong-password'
    }
    
    # Create and run ETL
    etl = MongoToPostgresETL(mongo_config, postgres_config)
    etl.run_etl()