"""
Production-Ready MongoDB to PostgreSQL ETL Pipeline
Features: Logging, Error Recovery, Monitoring, Connection Pooling, Batch Processing, Incremental Sync
"""
import pymongo
import psycopg2
from psycopg2 import sql, pool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from bson import ObjectId
import json
import os
import sys
from typing import Dict, List, Any, Optional
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import re
import logging
from logging.handlers import RotatingFileHandler
import time
from contextlib import contextmanager
import signal
from enum import Enum
import threading
import schedule

# ========================= Configuration =========================
class ETLStatus(Enum):
    """ETL execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    SYNCING = "syncing"

@dataclass
class ETLMetrics:
    """Metrics for monitoring ETL execution"""
    start_time: datetime
    end_time: Optional[datetime] = None
    collections_total: int = 0
    collections_processed: int = 0
    collections_failed: int = 0
    documents_total: int = 0
    documents_processed: int = 0
    documents_failed: int = 0
    sync_cycles: int = 0
    last_sync_time: Optional[datetime] = None
    current_collection: Optional[str] = None
    status: ETLStatus = ETLStatus.PENDING
    error_messages: List[str] = None
    
    def __post_init__(self):
        if self.error_messages is None:
            self.error_messages = []
    
    @property
    def duration_seconds(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return (datetime.now() - self.start_time).total_seconds()
    
    @property
    def docs_per_second(self) -> float:
        duration = self.duration_seconds
        return self.documents_processed / duration if duration > 0 else 0
    
    def to_dict(self) -> Dict:
        data = asdict(self)
        data['status'] = self.status.value
        data['start_time'] = self.start_time.isoformat()
        data['end_time'] = self.end_time.isoformat() if self.end_time else None
        data['last_sync_time'] = self.last_sync_time.isoformat() if self.last_sync_time else None
        return data

# ========================= Logging Setup =========================
def setup_logging(log_level: str = "INFO", log_file: str = "etl_pipeline.log") -> logging.Logger:
    """Configure comprehensive logging"""
    logger = logging.getLogger("ETL")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

# ========================= Main ETL Class =========================
class ProductionMongoToPostgresETL:
    """Production-ready ETL pipeline with enterprise features including incremental sync"""
    
    def __init__(
        self,
        mongo_config: Dict,
        postgres_config: Dict,
        etl_config: Optional[Dict] = None
    ):
        """
        Initialize production ETL pipeline
        
        Args:
            mongo_config: MongoDB connection configuration
            postgres_config: PostgreSQL connection configuration
            etl_config: ETL-specific configuration (batch size, workers, etc.)
        """
        self.mongo_config = mongo_config
        self.postgres_config = postgres_config
        self.etl_config = etl_config or {}
        
        # Setup logging
        log_level = self.etl_config.get('log_level', 'INFO')
        log_file = self.etl_config.get('log_file', 'etl_pipeline.log')
        self.logger = setup_logging(log_level, log_file)
        
        # Initialize connections
        self.mongo_client: Optional[pymongo.MongoClient] = None
        self.mongo_db = None
        self.pg_pool: Optional[pool.ThreadedConnectionPool] = None
        
        # State management
        self.schema_map = {}
        self.metrics = ETLMetrics(start_time=datetime.now())
        self.should_stop = False
        self.sync_thread: Optional[threading.Thread] = None
        self.sync_running = False
        self.initial_migration_completed = False
        
        # Configuration
        self.batch_size = self.etl_config.get('batch_size', 1000)
        self.sample_size = self.etl_config.get('sample_size', 100)
        self.max_retries = self.etl_config.get('max_retries', 3)
        self.retry_delay = self.etl_config.get('retry_delay', 5)
        self.connection_timeout = self.etl_config.get('connection_timeout', 30)
        self.skip_views = self.etl_config.get('skip_views', True)
        self.skip_system = self.etl_config.get('skip_system', True)
        self.override_tables = self.etl_config.get('override_tables', False)
        self.sync_duration = self.etl_config.get('sync_duration', 0)  # in minutes, 0 means no sync
        
        # PostgreSQL reserved words
        self.postgres_reserved_words = self._load_reserved_words()
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        self.logger.info("Production ETL Pipeline initialized")
    
    def _load_reserved_words(self) -> set:
        """Load PostgreSQL reserved keywords"""
        return {
            'limit', 'group', 'order', 'user', 'primary', 'key', 'foreign',
            'references', 'select', 'insert', 'update', 'delete', 'where',
            'from', 'table', 'database', 'schema', 'column', 'index',
            'values', 'set', 'into', 'as', 'on', 'join', 'left', 'right',
            'inner', 'outer', 'full', 'cross', 'natural', 'using', 'having',
            'distinct', 'all', 'any', 'some', 'exists', 'not', 'and', 'or',
            'between', 'in', 'like', 'ilike', 'similar', 'to', 'is', 'null',
            'true', 'false', 'unknown', 'case', 'when', 'then', 'else', 'end'
        }
    
    def _setup_signal_handlers(self):
        """Setup handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            self.logger.warning(f"Received signal {signum}. Initiating graceful shutdown...")
            self.should_stop = True
            self.metrics.status = ETLStatus.PAUSED
            self.stop_sync()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    # ========================= Connection Management =========================
    
    def connect_mongodb(self) -> bool:
        """Establish MongoDB connection with retry logic"""
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Attempting MongoDB connection (attempt {attempt + 1}/{self.max_retries})")
                
                if self.mongo_config.get('username'):
                    auth_source = self.mongo_config.get('auth_db', 'admin')
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
                    serverSelectionTimeoutMS=self.connection_timeout * 1000,
                    connectTimeoutMS=self.connection_timeout * 1000,
                    socketTimeoutMS=self.connection_timeout * 1000,
                    retryWrites=True,
                    retryReads=True,
                    maxPoolSize=50
                )
                
                # Test connection
                self.mongo_client.admin.command('ping')
                self.mongo_db = self.mongo_client[self.mongo_config['db']]
                
                self.logger.info(f"✓ Connected to MongoDB: {self.mongo_config['db']}")
                return True
                
            except Exception as e:
                self.logger.error(f"MongoDB connection attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    self.logger.critical("MongoDB connection failed after all retries")
                    raise
        
        return False
    
    def connect_postgresql(self) -> bool:
        """Establish PostgreSQL connection pool"""
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Attempting PostgreSQL connection (attempt {attempt + 1}/{self.max_retries})")
                
                target_db = self.postgres_config['db']
                
                # Create database if it doesn't exist
                temp_conn = psycopg2.connect(
                    host=self.postgres_config['host'],
                    port=self.postgres_config['port'],
                    database='postgres',
                    user=self.postgres_config['user'],
                    password=self.postgres_config['password'],
                    connect_timeout=self.connection_timeout
                )
                temp_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                temp_cursor = temp_conn.cursor()
                
                temp_cursor.execute(
                    "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
                    (target_db,)
                )
                
                if not temp_cursor.fetchone():
                    self.logger.info(f"Creating database: {target_db}")
                    temp_cursor.execute(
                        sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db))
                    )
                
                temp_cursor.close()
                temp_conn.close()
                
                # Create connection pool
                self.pg_pool = pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=10,
                    host=self.postgres_config['host'],
                    port=self.postgres_config['port'],
                    database=target_db,
                    user=self.postgres_config['user'],
                    password=self.postgres_config['password'],
                    connect_timeout=self.connection_timeout
                )
                
                self.logger.info(f"✓ Connected to PostgreSQL: {target_db}")
                return True
                
            except Exception as e:
                self.logger.error(f"PostgreSQL connection attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    self.logger.critical("PostgreSQL connection failed after all retries")
                    raise
        
        return False
    
    @contextmanager
    def get_pg_connection(self):
        """Context manager for PostgreSQL connections from pool"""
        conn = None
        try:
            conn = self.pg_pool.getconn()
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            yield conn
        except Exception as e:
            self.logger.error(f"Error with PostgreSQL connection: {e}")
            raise
        finally:
            if conn:
                self.pg_pool.putconn(conn)
    
    # ========================= Schema Management =========================
    
    def sanitize_name(self, name: str) -> str:
        """Sanitize names for PostgreSQL"""
        name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        if name and name[0].isdigit():
            name = 'col_' + name
        return name.lower()[:63]  # PostgreSQL identifier limit
    
    def handle_reserved_keywords(self, field_name: str) -> str:
        """Handle PostgreSQL reserved keywords"""
        sanitized = self.sanitize_name(field_name)
        if sanitized in self.postgres_reserved_words:
            return f'field_{sanitized}'
        return sanitized
    
    def infer_postgres_type(self, value: Any) -> str:
        """Infer PostgreSQL data type"""
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
        elif isinstance(value, (dict, list)):
            return "JSONB"
        elif isinstance(value, ObjectId):
            return "TEXT"
        else:
            return "TEXT"
    
    def analyze_collection_schema(self, collection_name: str) -> Dict:
        """Analyze MongoDB collection schema"""
        try:
            collection = self.mongo_db[collection_name]
            documents = list(collection.find().limit(self.sample_size))
            
            if not documents:
                self.logger.warning(f"No documents found in collection: {collection_name}")
                return {'fields': {}, 'nested_arrays': {}}
            
            schema = {
                'fields': defaultdict(set),
                'nested_arrays': {}
            }
            
            def analyze_document(doc, prefix=''):
                for key, value in doc.items():
                    if key == '_id':
                        continue
                    
                    field_path = f"{prefix}{key}" if prefix else key
                    
                    if isinstance(value, list) and value:
                        if isinstance(value[0], dict):
                            schema['nested_arrays'][field_path] = []
                            for item in value[:10]:  # Sample first 10 items
                                if isinstance(item, dict):
                                    schema['nested_arrays'][field_path].append(item)
                        else:
                            schema['fields'][field_path].add('JSONB')
                    elif isinstance(value, dict):
                        analyze_document(value, f"{field_path}_")
                    else:
                        schema['fields'][field_path].add(self.infer_postgres_type(value))
            
            for doc in documents:
                analyze_document(doc)
            
            # Determine final types
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
            
            self.logger.debug(f"Schema analyzed for {collection_name}: {len(final_schema['fields'])} fields, {len(final_schema['nested_arrays'])} nested arrays")
            return final_schema
            
        except Exception as e:
            self.logger.error(f"Error analyzing schema for {collection_name}: {e}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in PostgreSQL"""
        sanitized_table = self.sanitize_name(table_name)
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = %s
                        );
                    """, (sanitized_table,))
                    return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Error checking if table {sanitized_table} exists: {e}")
            return False
    
    def drop_table(self, table_name: str):
        """Drop PostgreSQL table"""
        sanitized_table = self.sanitize_name(table_name)
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                            sql.Identifier(sanitized_table)
                        )
                    )
            self.logger.info(f"✓ Dropped table: {sanitized_table}")
        except Exception as e:
            self.logger.error(f"Error dropping table {sanitized_table}: {e}")
            raise
    
    def create_table(self, table_name: str, fields: Dict[str, str], parent_table: Optional[str] = None):
        """Create PostgreSQL table"""
        sanitized_table = self.sanitize_name(table_name)
        
        # Check if table exists and handle override
        if self.table_exists(sanitized_table):
            if self.override_tables:
                self.logger.info(f"Table {sanitized_table} exists, overriding...")
                self.drop_table(sanitized_table)
            else:
                self.logger.info(f"Table {sanitized_table} exists, skipping creation")
                return
        
        columns = ["id SERIAL PRIMARY KEY"]
        
        if parent_table:
            parent_id_col = f"{self.sanitize_name(parent_table)}_id"
            columns.append(f"{parent_id_col} BIGINT REFERENCES {self.sanitize_name(parent_table)}(id) ON DELETE CASCADE")
        
        if not parent_table:
            columns.append("mongo_id TEXT UNIQUE NOT NULL")
            columns.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            columns.append("updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            columns.append("last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        for field, field_type in fields.items():
            sanitized_field = self.handle_reserved_keywords(field)
            
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
            with self.get_pg_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_query)
                    
                    # Create indexes only if not parent table
                    if not parent_table:
                        index_name = f"idx_{sanitized_table}_mongo_id"[:63]
                        cursor.execute(
                            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (mongo_id)").format(
                                sql.Identifier(index_name),
                                sql.Identifier(sanitized_table)
                            )
                        )
                        
                        # Create index for last_modified for sync
                        index_name_modified = f"idx_{sanitized_table}_modified"[:63]
                        cursor.execute(
                            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (last_modified)").format(
                                sql.Identifier(index_name_modified),
                                sql.Identifier(sanitized_table)
                            )
                        )
            
            self.logger.info(f"✓ Created table: {sanitized_table}")
            
        except Exception as e:
            self.logger.error(f"Error creating table {sanitized_table}: {e}")
            raise
    
    # ========================= Data Migration =========================
    
    def build_field_mapping(self, collection_name: str, schema: Dict) -> Dict[str, str]:
        """Build MongoDB to PostgreSQL field mapping"""
        field_map = {'_id': 'mongo_id'}
        
        for field in schema['fields'].keys():
            pg_column = self.handle_reserved_keywords(field)
            
            if pg_column == 'id':
                pg_column = 'original_id'
            elif pg_column == 'mongo_id':
                pg_column = 'mongo_field_id'
            
            field_map[field] = pg_column
        
        return field_map
    
    def get_table_columns(self, table_name: str, cursor) -> set:
        """Get PostgreSQL table columns"""
        sanitized_table = self.sanitize_name(table_name)
        try:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (sanitized_table,))
            return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            self.logger.warning(f"Could not get columns for {sanitized_table}: {e}")
            return set()
    
    def insert_batch(
        self,
        table_name: str,
        records: List[Dict],
        field_mapping: Dict[str, str],
        conn,
        parent_table: Optional[str] = None
    ) -> List[int]:
        """Insert batch of records"""
        if not records:
            return []
        
        sanitized_table = self.sanitize_name(table_name)
        inserted_ids = []
        
        with conn.cursor() as cursor:
            table_columns = self.get_table_columns(table_name, cursor)
            
            for record in records:
                try:
                    columns = []
                    values = []
                    
                    for key, value in record.items():
                        pg_column = field_mapping.get(key)
                        if not pg_column:
                            pg_column = self.handle_reserved_keywords(key)
                            if pg_column == 'id':
                                pg_column = 'original_id'
                        
                        if pg_column not in table_columns:
                            continue
                        
                        columns.append(pg_column)
                        
                        # Convert value
                        if isinstance(value, ObjectId):
                            values.append(str(value))
                        elif isinstance(value, (dict, list)):
                            values.append(json.dumps(value, default=str))
                        else:
                            values.append(value)
                    
                    if not columns:
                        continue
                    
                    # Add last_modified for main tables
                    if not parent_table and 'last_modified' in table_columns:
                        columns.append('last_modified')
                        values.append(datetime.now())
                    
                    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
                        sql.Identifier(sanitized_table),
                        sql.SQL(', ').join([sql.Identifier(col) for col in columns]),
                        sql.SQL(', ').join([sql.SQL('%s')] * len(values))
                    )
                    
                    cursor.execute(insert_query, values)
                    inserted_ids.append(cursor.fetchone()[0])
                    
                except Exception as e:
                    self.logger.debug(f"Error inserting record: {e}")
                    self.metrics.documents_failed += 1
                    continue
        
        return inserted_ids
    
    def update_record(
        self,
        table_name: str,
        record: Dict,
        field_mapping: Dict[str, str],
        conn
    ) -> bool:
        """Update existing record"""
        sanitized_table = self.sanitize_name(table_name)
        
        try:
            with conn.cursor() as cursor:
                table_columns = self.get_table_columns(table_name, cursor)
                
                set_clauses = []
                values = []
                
                for key, value in record.items():
                    if key == '_id':
                        continue
                    
                    pg_column = field_mapping.get(key)
                    if not pg_column:
                        pg_column = self.handle_reserved_keywords(key)
                    
                    if pg_column not in table_columns or pg_column in ['id', 'mongo_id', 'created_at']:
                        continue
                    
                    set_clauses.append(f"{pg_column} = %s")
                    
                    # Convert value
                    if isinstance(value, ObjectId):
                        values.append(str(value))
                    elif isinstance(value, (dict, list)):
                        values.append(json.dumps(value, default=str))
                    else:
                        values.append(value)
                
                # Add last_modified
                if 'last_modified' in table_columns:
                    set_clauses.append('last_modified = %s')
                    values.append(datetime.now())
                
                if not set_clauses:
                    return False
                
                # Add mongo_id for WHERE clause
                mongo_id = str(record.get('_id', ''))
                values.append(mongo_id)
                
                update_query = sql.SQL("UPDATE {} SET {} WHERE mongo_id = %s").format(
                    sql.Identifier(sanitized_table),
                    sql.SQL(', ').join(set_clauses)
                )
                
                cursor.execute(update_query, values)
                return cursor.rowcount > 0
                
        except Exception as e:
            self.logger.debug(f"Error updating record: {e}")
            return False
    
    def record_exists(self, table_name: str, mongo_id: str, conn) -> bool:
        """Check if record exists in PostgreSQL"""
        sanitized_table = self.sanitize_name(table_name)
        
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL("SELECT 1 FROM {} WHERE mongo_id = %s").format(
                        sql.Identifier(sanitized_table)
                    ),
                    (mongo_id,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            self.logger.debug(f"Error checking record existence: {e}")
            return False
    
    def process_collection(self, collection_name: str):
        """Process a single collection"""
        try:
            self.logger.info(f"{'='*60}")
            self.logger.info(f"Processing collection: {collection_name}")
            self.metrics.current_collection = collection_name
            
            # Analyze schema
            schema = self.analyze_collection_schema(collection_name)
            self.schema_map[collection_name] = schema
            
            # Build field mapping
            field_mapping = self.build_field_mapping(collection_name, schema)
            
            # Create main table
            self.create_table(collection_name, schema['fields'])
            
            # Create nested tables
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
                    nested_mappings[nested_field] = self.build_field_mapping(
                        nested_table_name,
                        {'fields': nested_fields}
                    )
            
            # Migrate data in batches
            collection = self.mongo_db[collection_name]
            total_docs = collection.count_documents({})
            self.metrics.documents_total += total_docs
            
            self.logger.info(f"Migrating {total_docs} documents in batches of {self.batch_size}...")
            
            processed = 0
            successful = 0
            
            batch = []
            
            for doc in collection.find().batch_size(self.batch_size):
                if self.should_stop:
                    self.logger.warning("ETL paused by user request")
                    break
                
                try:
                    # Extract nested arrays
                    nested_arrays_data = {}
                    for nested_field in schema['nested_arrays'].keys():
                        value = doc
                        for key in nested_field.split('_'):
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
                            else:
                                main_record[field_path] = value
                    
                    flatten_doc(doc)
                    batch.append((main_record, nested_arrays_data))
                    
                    # Process batch
                    if len(batch) >= self.batch_size:
                        with self.get_pg_connection() as conn:
                            for record, nested_data in batch:
                                parent_ids = self.insert_batch(
                                    collection_name,
                                    [record],
                                    field_mapping,
                                    conn
                                )
                                
                                if parent_ids:
                                    successful += 1
                                    
                                    # Insert nested records
                                    for nested_field, nested_items in nested_data.items():
                                        if nested_items:
                                            nested_table = f"{collection_name}_{nested_field}"
                                            parent_ref_col = f"{self.sanitize_name(collection_name)}_id"
                                            
                                            nested_records = []
                                            for item in nested_items:
                                                if isinstance(item, dict):
                                                    nested_record = {parent_ref_col: parent_ids[0]}
                                                    nested_record.update(item)
                                                    nested_records.append(nested_record)
                                            
                                            if nested_records:
                                                self.insert_batch(
                                                    nested_table,
                                                    nested_records,
                                                    nested_mappings.get(nested_field, {}),
                                                    conn,
                                                    parent_table=collection_name
                                                )
                        
                        processed += len(batch)
                        batch = []
                        
                        if processed % (self.batch_size * 10) == 0:
                            self.logger.info(f"Progress: {processed}/{total_docs} ({(processed/total_docs*100):.1f}%)")
                
                except Exception as e:
                    self.logger.error(f"Error processing document: {e}")
                    self.metrics.documents_failed += 1
            
            # Process remaining batch
            if batch and not self.should_stop:
                with self.get_pg_connection() as conn:
                    for record, nested_data in batch:
                        parent_ids = self.insert_batch(collection_name, [record], field_mapping, conn)
                        if parent_ids:
                            successful += 1
                processed += len(batch)
            
            self.metrics.documents_processed += processed
            self.metrics.collections_processed += 1
            
            self.logger.info(f"✓ Completed {collection_name}: {successful}/{total_docs} successful")
            
        except Exception as e:
            self.logger.error(f"Failed to process collection {collection_name}: {e}")
            self.metrics.collections_failed += 1
            self.metrics.error_messages.append(f"{collection_name}: {str(e)}")
            raise
    
    def is_view(self, collection_name: str) -> bool:
        """Check if collection is a view"""
        try:
            coll_info = self.mongo_db.list_collections(filter={'name': collection_name})
            for info in coll_info:
                if info.get('type') == 'view':
                    return True
            return False
        except:
            return False
    
    # ========================= Incremental Sync =========================
    
    def get_last_sync_time(self, table_name: str) -> Optional[datetime]:
        """Get last sync time for a table"""
        sanitized_table = self.sanitize_name(table_name)
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        sql.SQL("SELECT MAX(last_modified) FROM {}").format(
                            sql.Identifier(sanitized_table)
                        )
                    )
                    result = cursor.fetchone()[0]
                    return result if result else None
        except Exception as e:
            self.logger.debug(f"Could not get last sync time for {sanitized_table}: {e}")
            return None
    
    def sync_collection(self, collection_name: str):
        """Sync incremental changes for a collection"""
        try:
            self.logger.info(f"Syncing collection: {collection_name}")
            
            # Get last sync time
            last_sync = self.get_last_sync_time(collection_name)
            
            collection = self.mongo_db[collection_name]
            schema = self.schema_map.get(collection_name)
            field_mapping = self.build_field_mapping(collection_name, schema)
            
            # Query for modified documents - using MongoDB's _id as change indicator
            # This is a simple approach - you might want to use a proper timestamp field
            query = {}
            if last_sync:
                # If we have a last sync time, we need a way to find changed documents
                # For now, we'll sync all documents and let the update logic handle conflicts
                pass
            
            # Get all documents for now (you can optimize this with proper change tracking)
            all_docs = list(collection.find({}))
            
            if not all_docs:
                self.logger.debug(f"No documents found for {collection_name}")
                return
            
            self.logger.info(f"Processing {len(all_docs)} documents in {collection_name}")
            
            inserted = 0
            updated = 0
            
            with self.get_pg_connection() as conn:
                for doc in all_docs:
                    if self.should_stop:
                        break
                    
                    mongo_id = str(doc['_id'])
                    
                    # Check if record exists
                    if self.record_exists(collection_name, mongo_id, conn):
                        # Update existing record
                        main_record = {'_id': mongo_id}
                        
                        def flatten_doc(d, prefix=''):
                            for key, value in d.items():
                                if key == '_id':
                                    continue
                                
                                field_path = f"{prefix}{key}" if prefix else key
                                
                                if field_path in schema.get('nested_arrays', {}):
                                    continue
                                elif isinstance(value, dict):
                                    flatten_doc(value, f"{field_path}_")
                                else:
                                    main_record[field_path] = value
                        
                        flatten_doc(doc)
                        
                        if self.update_record(collection_name, main_record, field_mapping, conn):
                            updated += 1
                    else:
                        # Insert new record
                        main_record = {'mongo_id': mongo_id}
                        
                        def flatten_doc(d, prefix=''):
                            for key, value in d.items():
                                if key == '_id':
                                    continue
                                
                                field_path = f"{prefix}{key}" if prefix else key
                                
                                if field_path in schema.get('nested_arrays', {}):
                                    continue
                                elif isinstance(value, dict):
                                    flatten_doc(value, f"{field_path}_")
                                else:
                                    main_record[field_path] = value
                        
                        flatten_doc(doc)
                        
                        parent_ids = self.insert_batch(collection_name, [main_record], field_mapping, conn)
                        if parent_ids:
                            inserted += 1
            
            self.logger.info(f"✓ Synced {collection_name}: {inserted} inserted, {updated} updated")
            
        except Exception as e:
            self.logger.error(f"Error syncing collection {collection_name}: {e}")
    
    def incremental_sync(self):
        """Perform incremental sync for all collections"""
        if self.sync_duration <= 0:
            return
        
        self.sync_running = True
        self.metrics.status = ETLStatus.SYNCING
        
        try:
            self.logger.info("Starting incremental sync...")
            
            # Get collections that were successfully migrated
            for collection_name in self.schema_map.keys():
                if self.should_stop:
                    break
                self.sync_collection(collection_name)
            
            self.metrics.sync_cycles += 1
            self.metrics.last_sync_time = datetime.now()
            self.logger.info(f"Incremental sync completed. Cycle: {self.metrics.sync_cycles}")
            
        except Exception as e:
            self.logger.error(f"Incremental sync failed: {e}")
        finally:
            self.metrics.status = ETLStatus.COMPLETED
            self.sync_running = False
    
    def start_sync_scheduler(self):
        """Start the incremental sync scheduler"""
        if self.sync_duration <= 0:
            self.logger.info("Sync duration is 0, incremental sync disabled")
            return
        
        def sync_job():
            if not self.sync_running:
                self.incremental_sync()
        
        # Schedule sync job
        schedule.every(self.sync_duration).minutes.do(sync_job)
        
        def scheduler_loop():
            while not self.should_stop:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        
        self.sync_thread = threading.Thread(target=scheduler_loop, daemon=True)
        self.sync_thread.start()
        self.logger.info(f"Incremental sync scheduler started. Interval: {self.sync_duration} minutes")
    
    def stop_sync(self):
        """Stop the incremental sync scheduler"""
        self.should_stop = True
        if self.sync_thread and self.sync_thread.is_alive():
            self.sync_thread.join(timeout=30)
        self.logger.info("Incremental sync scheduler stopped")
    
    # ========================= Main ETL Execution =========================
    
    def run_etl(self) -> Dict:
        """Execute complete ETL process"""
        self.metrics.status = ETLStatus.RUNNING
        
        try:
            # Connect to databases
            self.connect_mongodb()
            self.connect_postgresql()
            
            # Get collections
            all_collections = self.mongo_db.list_collection_names()
            
            collections = []
            for collection_name in all_collections:
                if self.skip_system and collection_name.startswith('system.'):
                    self.logger.info(f"⊘ Skipping system collection: {collection_name}")
                    continue
                
                if self.skip_views and self.is_view(collection_name):
                    self.logger.info(f"⊘ Skipping view: {collection_name}")
                    continue
                
                collections.append(collection_name)
            
            self.metrics.collections_total = len(collections)
            self.logger.info(f"Found {len(collections)} collections to migrate")
            
            # Check if this is initial migration or sync-only mode
            initial_migration_needed = False
            for collection_name in collections:
                sanitized_table = self.sanitize_name(collection_name)
                if not self.table_exists(sanitized_table):
                    initial_migration_needed = True
                    break
            
            if initial_migration_needed or self.override_tables:
                # Perform full migration
                self.logger.info("Starting full migration...")
                for collection_name in collections:
                    if self.should_stop:
                        break
                    self.process_collection(collection_name)
                self.initial_migration_completed = True
            else:
                # Skip migration, only setup for sync
                self.logger.info("All tables exist, skipping full migration...")
                for collection_name in collections:
                    if self.should_stop:
                        break
                    # Just analyze schema for sync
                    schema = self.analyze_collection_schema(collection_name)
                    self.schema_map[collection_name] = schema
                self.initial_migration_completed = True
            
            # Start incremental sync if configured
            if self.sync_duration > 0 and self.initial_migration_completed:
                self.logger.info(f"Starting incremental sync scheduler with {self.sync_duration} minute interval")
                self.start_sync_scheduler()
            
            # Finalize
            self.metrics.end_time = datetime.now()
            
            if self.should_stop:
                self.metrics.status = ETLStatus.PAUSED
                self.logger.warning("ETL paused by user")
            else:
                self.metrics.status = ETLStatus.COMPLETED
                self.logger.info("="*60)
                self.logger.info("ETL Process Completed Successfully!")
                self.logger.info(f"Duration: {self.metrics.duration_seconds:.2f} seconds")
                self.logger.info(f"Collections: {self.metrics.collections_processed}/{self.metrics.collections_total}")
                self.logger.info(f"Documents: {self.metrics.documents_processed} processed, {self.metrics.documents_failed} failed")
                self.logger.info(f"Performance: {self.metrics.docs_per_second:.1f} docs/sec")
                if self.sync_duration > 0:
                    self.logger.info(f"Incremental sync: Enabled ({self.sync_duration} minute interval)")
                self.logger.info("="*60)
            
            return self.metrics.to_dict()
            
        except Exception as e:
            self.metrics.status = ETLStatus.FAILED
            self.metrics.end_time = datetime.now()
            self.logger.critical(f"ETL Process Failed: {e}", exc_info=True)
            raise
        
        finally:
            if self.sync_duration <= 0 or self.should_stop:
                self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            self.stop_sync()
            
            if self.mongo_client:
                self.mongo_client.close()
                self.logger.info("✓ MongoDB connection closed")
            
            if self.pg_pool:
                self.pg_pool.closeall()
                self.logger.info("✓ PostgreSQL connection pool closed")
        
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

# ========================= Configuration Helper =========================
def load_config_from_env() -> tuple:
    """Load configuration from environment variables"""
    from dotenv import load_dotenv
    load_dotenv()  # Load from .env file
    
    mongo_config = {
        'host': os.getenv('MONGO_HOST', 'localhost'),
        'port': int(os.getenv('MONGO_PORT', 27017)),
        'db': os.getenv('MONGO_DB', 'source_db'),
        'username': os.getenv('MONGO_USER'),
        'password': os.getenv('MONGO_PASSWORD'),
        'auth_db': os.getenv('MONGO_AUTH_DB', 'admin')
    }
    
    postgres_config = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': int(os.getenv('POSTGRES_PORT', 5432)),
        'db': os.getenv('POSTGRES_DB', 'target_db'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD')
    }
    
    etl_config = {
        'batch_size': int(os.getenv('ETL_BATCH_SIZE', 1000)),
        'sample_size': int(os.getenv('ETL_SAMPLE_SIZE', 100)),
        'max_retries': int(os.getenv('ETL_MAX_RETRIES', 3)),
        'retry_delay': int(os.getenv('ETL_RETRY_DELAY', 5)),
        'connection_timeout': int(os.getenv('ETL_CONNECTION_TIMEOUT', 30)),
        'log_level': os.getenv('ETL_LOG_LEVEL', 'INFO'),
        'log_file': os.getenv('ETL_LOG_FILE', 'etl_pipeline.log'),
        'skip_views': os.getenv('ETL_SKIP_VIEWS', 'true').lower() == 'true',
        'skip_system': os.getenv('ETL_SKIP_SYSTEM', 'true').lower() == 'true',
        'override_tables': os.getenv('ETL_OVERRIDE_TABLES', 'false').lower() == 'true',
        'sync_duration': int(os.getenv('ETL_SYNC_DURATION', 0))
    }
    
    return mongo_config, postgres_config, etl_config