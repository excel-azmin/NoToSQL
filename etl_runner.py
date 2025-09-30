#!/usr/bin/env python3
"""
ETL Runner - Main entry point for the ETL pipeline
"""
import os
import sys
import time
import json
from datetime import datetime
from production_etl import ProductionMongoToPostgresETL, load_config_from_env

def test_postgresql_connection(postgres_config):
    """Test PostgreSQL connection separately"""
    import psycopg2
    try:
        print("Testing PostgreSQL connection...")
        
        # Try connecting to postgres database first
        test_conn = psycopg2.connect(
            host=postgres_config['host'],
            port=postgres_config['port'],
            database='postgres',
            user=postgres_config['user'],
            password=postgres_config['password'],
            connect_timeout=10
        )
        test_conn.close()
        print("✓ PostgreSQL connection successful")
        return True
    except Exception as e:
        print(f"✗ PostgreSQL connection failed: {e}")
        return False

def main():
    """Main entry point for ETL pipeline"""
    print("=" * 60)
    print("Production MongoDB to PostgreSQL ETL Pipeline")
    print("=" * 60)
    
    try:
        # Load configuration from environment variables
        print("Loading configuration from environment...")
        mongo_config, postgres_config, etl_config = load_config_from_env()
        
        # Display configuration (without passwords)
        safe_mongo_config = mongo_config.copy()
        safe_mongo_config['password'] = '***' if mongo_config.get('password') else None
        
        safe_postgres_config = postgres_config.copy()
        safe_postgres_config['password'] = '***' if postgres_config.get('password') else None
        
        print(f"MongoDB Source: {safe_mongo_config['host']}:{safe_mongo_config['port']}/{safe_mongo_config['db']}")
        print(f"PostgreSQL Target: {safe_postgres_config['host']}:{safe_postgres_config['port']}/{safe_postgres_config['db']}")
        print(f"Batch Size: {etl_config['batch_size']}")
        print(f"Override Tables: {etl_config['override_tables']}")
        print(f"Sync Duration: {etl_config['sync_duration']} minutes")
        print("-" * 60)
        
        # Test PostgreSQL connection first
        if not test_postgresql_connection(postgres_config):
            print("\nPlease check your PostgreSQL configuration:")
            print("1. Ensure PostgreSQL is running: sudo systemctl status postgresql")
            print("2. Check if user exists: sudo -u postgres psql -c '\du'")
            print("3. Check if database exists: sudo -u postgres psql -c '\l'")
            print("4. Verify password: sudo -u postgres psql -c \"ALTER USER \\\"user-name\\\" WITH PASSWORD 'strong-password';\"")
            return 1
        
        # Create and run ETL
        etl = ProductionMongoToPostgresETL(mongo_config, postgres_config, etl_config)
        
        print("Starting ETL process...")
        start_time = datetime.now()
        
        metrics = etl.run_etl()
        
        duration = (datetime.now() - start_time).total_seconds()
        
        print("\n" + "=" * 60)
        print("ETL Execution Summary")
        print("=" * 60)
        print(f"Status: {metrics['status']}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Collections: {metrics['collections_processed']}/{metrics['collections_total']}")
        print(f"Documents: {metrics['documents_processed']} processed, {metrics['documents_failed']} failed")
        print(f"Performance: {metrics.get('docs_per_second', 0):.1f} docs/sec")
        
        if etl_config['sync_duration'] > 0:
            print(f"Incremental Sync: Enabled (every {etl_config['sync_duration']} minutes)")
            print("ETL will continue running for incremental sync...")
            print("Press Ctrl+C to stop")
            
            # Keep main thread alive for sync
            try:
                while True:
                    time.sleep(60)
            except KeyboardInterrupt:
                print("\nShutting down ETL pipeline...")
        
        # Save metrics to file
        metrics_file = f"etl_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(metrics_file, 'w') as f:
            json.dump(metrics, f, indent=2)
        print(f"Metrics saved to: {metrics_file}")
        
        return 0
        
    except KeyboardInterrupt:
        print("\nETL pipeline interrupted by user")
        return 130
    except Exception as e:
        print(f"\nETL pipeline failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())