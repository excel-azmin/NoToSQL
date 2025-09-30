#!/usr/bin/env python3
"""
ETL Monitor - Monitoring and metrics collection for ETL pipeline
"""
import json
import time
import psutil
import requests
from datetime import datetime
import logging

class ETLOperationsMonitor:
    """Monitor ETL pipeline operations and metrics"""
    
    def __init__(self, metrics_file: str = "etl_metrics.json"):
        self.metrics_file = metrics_file
        self.setup_logging()
    
    def setup_logging(self):
        """Setup monitoring logging"""
        self.logger = logging.getLogger("ETLMonitor")
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def collect_system_metrics(self) -> dict:
        """Collect system resource metrics"""
        return {
            'timestamp': datetime.now().isoformat(),
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_usage': psutil.disk_usage('/').percent,
            'active_connections': len(psutil.net_connections())
        }
    
    def load_etl_metrics(self) -> dict:
        """Load ETL metrics from file"""
        try:
            with open(self.metrics_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
    
    def check_etl_health(self, metrics: dict) -> dict:
        """Check ETL pipeline health"""
        health_status = {
            'status': 'unknown',
            'issues': [],
            'recommendations': []
        }
        
        if not metrics:
            health_status['status'] = 'no_data'
            health_status['issues'].append('No ETL metrics found')
            return health_status
        
        # Check for failures
        if metrics.get('documents_failed', 0) > 0:
            health_status['issues'].append(f"{metrics['documents_failed']} documents failed processing")
        
        if metrics.get('collections_failed', 0) > 0:
            health_status['issues'].append(f"{metrics['collections_failed']} collections failed processing")
        
        # Check performance
        docs_per_second = metrics.get('docs_per_second', 0)
        if docs_per_second < 10:
            health_status['recommendations'].append("Consider optimizing batch size or increasing resources")
        
        # Determine overall status
        if metrics.get('status') == 'completed':
            health_status['status'] = 'healthy' if not health_status['issues'] else 'degraded'
        elif metrics.get('status') == 'failed':
            health_status['status'] = 'failed'
        else:
            health_status['status'] = 'running'
        
        return health_status
    
    def generate_report(self):
        """Generate monitoring report"""
        etl_metrics = self.load_etl_metrics()
        system_metrics = self.collect_system_metrics()
        health_status = self.check_etl_health(etl_metrics)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'etl_metrics': etl_metrics,
            'system_metrics': system_metrics,
            'health_status': health_status
        }
        
        # Print summary
        print("\n" + "=" * 60)
        print("ETL Pipeline Monitoring Report")
        print("=" * 60)
        print(f"ETL Status: {etl_metrics.get('status', 'unknown')}")
        print(f"System Health: CPU {system_metrics['cpu_percent']}% | Memory {system_metrics['memory_percent']}%")
        print(f"Pipeline Health: {health_status['status']}")
        
        if health_status['issues']:
            print("Issues:")
            for issue in health_status['issues']:
                print(f"  - {issue}")
        
        if health_status['recommendations']:
            print("Recommendations:")
            for rec in health_status['recommendations']:
                print(f"  - {rec}")
        
        return report
    
    def continuous_monitoring(self, interval: int = 60):
        """Run continuous monitoring"""
        print(f"Starting continuous monitoring (interval: {interval}s)")
        print("Press Ctrl+C to stop monitoring")
        
        try:
            while True:
                self.generate_report()
                time.sleep(interval)
                print("-" * 40)
        except KeyboardInterrupt:
            print("\nMonitoring stopped")

def main():
    """Main function for ETL monitoring"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL Pipeline Monitor')
    parser.add_argument('--continuous', action='store_true', help='Run continuous monitoring')
    parser.add_argument('--interval', type=int, default=60, help='Monitoring interval in seconds')
    parser.add_argument('--metrics-file', default='etl_metrics.json', help='ETL metrics file path')
    
    args = parser.parse_args()
    
    monitor = ETLOperationsMonitor(args.metrics_file)
    
    if args.continuous:
        monitor.continuous_monitoring(args.interval)
    else:
        monitor.generate_report()

if __name__ == "__main__":
    main()