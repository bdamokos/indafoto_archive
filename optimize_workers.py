import os
import sqlite3
import time
import logging
from datetime import datetime
import json
import threading
import queue
from pathlib import Path
import shutil
from tqdm import tqdm
import argparse
import niquests as requests
from indafoto import (
    init_db, get_image_links, process_image_list,
    check_disk_space, estimate_space_requirements,
    get_free_space_gb, HEADERS, COOKIES, extract_image_id
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('worker_optimization.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
MIN_WORKERS = 1
MAX_WORKERS = 16
INITIAL_WORKERS = 4
MIN_SUCCESS_RATE = 0.95  # 95% success rate required
MIN_SPEED_MBPS = 1.0  # Minimum 1 MB/s required
OPTIMIZATION_FILE = "worker_optimization.json"

class WorkerOptimizer:
    def __init__(self, min_workers=MIN_WORKERS, max_workers=MAX_WORKERS, initial_workers=INITIAL_WORKERS):
        self.conn = init_db()
        self.cursor = self.conn.cursor()
        self.current_workers = initial_workers
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.initial_workers = initial_workers
        self.optimization_data = self.load_optimization_data()
        self.running = True
        self.test_queue = queue.Queue()
        self.results_queue = queue.Queue()
        self.last_tested_page = self.get_next_test_page() - 1  # Initialize to last completed page
        
        # Initialize session pool
        self.session_pool = queue.Queue(maxsize=MAX_WORKERS * 2)
        for _ in range(MAX_WORKERS * 2):
            session = requests.Session()
            session.headers.update(HEADERS)
            session.cookies.update(COOKIES)
            # Configure session for better performance
            adapter = requests.adapters.HTTPAdapter(pool_maxsize=5, pool_block=True)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            self.session_pool.put(session)
    
    def get_session(self):
        """Get a session from the pool."""
        return self.session_pool.get()
    
    def return_session(self, session):
        """Return a session to the pool."""
        self.session_pool.put(session)
    
    def cleanup_sessions(self):
        """Clean up all sessions in the pool."""
        while not self.session_pool.empty():
            try:
                session = self.session_pool.get_nowait()
                session.close()
            except queue.Empty:
                break
    
    def load_optimization_data(self):
        """Load previous optimization results if available."""
        if os.path.exists(OPTIMIZATION_FILE):
            try:
                with open(OPTIMIZATION_FILE, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load optimization data: {e}")
        return {
            'worker_counts': {},
            'best_worker_count': None,
            'last_optimization': None
        }
    
    def save_optimization_data(self):
        """Save optimization results to file."""
        try:
            with open(OPTIMIZATION_FILE, 'w') as f:
                json.dump(self.optimization_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save optimization data: {e}")
    
    def get_next_test_page(self):
        """Get the next page to test, avoiding already completed pages."""
        self.cursor.execute("""
            SELECT page_number 
            FROM completed_pages 
            ORDER BY page_number DESC 
            LIMIT 1
        """)
        result = self.cursor.fetchone()
        return (result[0] + 1) if result else 0
    
    def test_worker_count(self, worker_count):
        """Test a specific number of workers and return performance metrics."""
        logger.info(f"Testing with {worker_count} workers...")
        
        # Start from the page after the last tested page
        test_page = self.last_tested_page + 1
        
        while True:
            search_page_url = f"https://indafoto.hu/search/list?profile=main&sphinx=1&search=advanced&textsearch=fulltext&textuser=&textmap=&textcompilation=&photo=&datefrom=&dateto=&licence%5B1%5D=I1%3BI2%3BII1%3BII2&licence%5B2%5D=I1%3BI2%3BI3&page_offset={test_page}"
            
            # Get a session from the pool
            session = self.get_session()
            try:
                # Get image data using the session
                image_data_list = get_image_links(search_page_url, session=session)
                if not image_data_list:
                    logger.error(f"No images found on page {test_page}")
                    test_page += 1
                    continue
                    
                # Check if we have any new images to process
                new_images = 0
                for image_data in image_data_list:
                    try:
                        url = image_data.get('image_url', '')
                        if not url:
                            continue
                            
                        # Extract image ID from URL
                        image_id = extract_image_id(url)
                        if not image_id:
                            continue
                        
                        # Check if we already have this image
                        self.cursor.execute("""
                            SELECT id FROM images 
                            WHERE url LIKE ?
                        """, (f"%{image_id}%",))
                        if not self.cursor.fetchone():
                            new_images += 1
                    except Exception as e:
                        logger.error(f"Error checking image: {e}")
                        continue
                
                if new_images > 0:
                    logger.info(f"Found {new_images} new images on page {test_page}")
                    break
                else:
                    logger.warning(f"No new images found on page {test_page}, trying next page...")
                    test_page += 1
            finally:
                # Return the session to the pool
                self.return_session(session)
        
        # Track metrics
        start_time = time.time()
        start_size = sum(f.stat().st_size for f in Path("indafoto_archive").rglob('*') if f.is_file())
        
        # Process images with current worker count
        success, stats = process_image_list(image_data_list, self.conn, self.cursor)
        
        # Calculate metrics
        end_time = time.time()
        end_size = sum(f.stat().st_size for f in Path("indafoto_archive").rglob('*') if f.is_file())
        
        duration = end_time - start_time
        total_bytes = end_size - start_size
        speed_mbps = (total_bytes / duration) / (1024 * 1024) if duration > 0 else 0
        
        # Calculate success rate only for actual downloads (excluding skipped images)
        total_downloads = stats['total_images'] - stats['skipped_count']
        success_rate = stats['processed_count'] / total_downloads if total_downloads > 0 else 0
        
        # Calculate estimated completion time for different average image sizes
        TOTAL_IMAGES = 15000 * 36  # 540,000 images
        
        estimated_completion = {}
        for avg_size_mb in [1.0, 2.0]:
            total_data_gb = (TOTAL_IMAGES * avg_size_mb) / 1024
            
            if speed_mbps > 0:
                estimated_seconds = (TOTAL_IMAGES * avg_size_mb) / speed_mbps
                estimated_hours = estimated_seconds / 3600
                estimated_days = estimated_hours / 24
            else:
                estimated_seconds = float('inf')
                estimated_hours = float('inf')
                estimated_days = float('inf')
            
            estimated_completion[f"{avg_size_mb}MB"] = {
                'seconds': estimated_seconds,
                'hours': estimated_hours,
                'days': estimated_days,
                'total_data_gb': total_data_gb
            }
        
        metrics = {
            'worker_count': worker_count,
            'duration': duration,
            'total_bytes': total_bytes,
            'speed_mbps': speed_mbps,
            'success_rate': success_rate,
            'processed_count': stats['processed_count'],
            'failed_count': stats['failed_count'],
            'skipped_count': stats['skipped_count'],
            'total_images': stats['total_images'],
            'total_downloads': total_downloads,
            'test_page': test_page,
            'estimated_completion': estimated_completion
        }
        
        # Update the last tested page
        self.last_tested_page = test_page
        
        logger.info(f"Test results for {worker_count} workers (page {test_page}):")
        logger.info(f"  Speed: {speed_mbps:.2f} MB/s")
        logger.info(f"  Success rate: {success_rate:.2%} (of {total_downloads} actual downloads)")
        logger.info(f"  Processed: {stats['processed_count']}")
        logger.info(f"  Failed: {stats['failed_count']}")
        logger.info(f"  Skipped: {stats['skipped_count']}")
        if speed_mbps > 0:
            logger.info("\nEstimated completion times:")
            for avg_size, est in estimated_completion.items():
                logger.info(f"  For {avg_size} average image size:")
                logger.info(f"    Total data: {est['total_data_gb']:.1f} GB")
                logger.info(f"    Time: {est['days']:.1f} days ({est['hours']:.1f} hours)")
        
        return metrics
    
    def optimize_workers(self):
        """Find the optimal number of workers through testing."""
        logger.info("Starting worker optimization...")
        
        def test_and_get_metrics(worker_count):
            """Helper function to test a worker count and return metrics."""
            metrics = self.test_worker_count(worker_count)
            if not metrics:
                logger.warning(f"Failed to get metrics for {worker_count} workers, skipping...")
                return None
                
            # Store results
            self.optimization_data['worker_counts'][str(worker_count)] = metrics
            
            # Log performance metrics for this configuration
            logger.info(f"Results for {worker_count} workers:")
            logger.info(f"  Speed: {metrics['speed_mbps']:.2f} MB/s")
            logger.info(f"  Success rate: {metrics['success_rate']:.2%}")
            logger.info(f"  Processed: {metrics['processed_count']}")
            logger.info(f"  Failed: {metrics['failed_count']}")
            logger.info(f"  Skipped: {metrics['skipped_count']}")
            
            return metrics
        
        # Test worker counts in a specific order to find optimal throughput
        # Start with 1 worker as baseline
        current_workers = 1
        current_metrics = test_and_get_metrics(current_workers)
        if not current_metrics:
            logger.error("Failed to test with 1 worker")
            return self.initial_workers
            
        best_workers = current_workers
        best_speed = current_metrics['speed_mbps']
        
        # Test 2 workers to see if we can improve throughput
        next_metrics = test_and_get_metrics(2)
        if next_metrics and next_metrics['speed_mbps'] > best_speed:
            best_workers = 2
            best_speed = next_metrics['speed_mbps']
        
        # Test 3 workers
        next_metrics = test_and_get_metrics(3)
        if next_metrics and next_metrics['speed_mbps'] > best_speed:
            best_workers = 3
            best_speed = next_metrics['speed_mbps']
        
        # Test 4 workers
        next_metrics = test_and_get_metrics(4)
        if next_metrics and next_metrics['speed_mbps'] > best_speed:
            best_workers = 4
            best_speed = next_metrics['speed_mbps']
        
        # Test 5 workers
        next_metrics = test_and_get_metrics(5)
        if next_metrics and next_metrics['speed_mbps'] > best_speed:
            best_workers = 5
            best_speed = next_metrics['speed_mbps']
        
        # Test 8 workers (common power of 2)
        next_metrics = test_and_get_metrics(8)
        if next_metrics and next_metrics['speed_mbps'] > best_speed:
            best_workers = 8
            best_speed = next_metrics['speed_mbps']
        
        # Test 16 workers (max)
        next_metrics = test_and_get_metrics(16)
        if next_metrics and next_metrics['speed_mbps'] > best_speed:
            best_workers = 16
            best_speed = next_metrics['speed_mbps']
        
        # Find the best worker count from all tested configurations
        best_worker_count = None
        best_speed = 0
        
        for worker_count, metrics in self.optimization_data['worker_counts'].items():
            # Skip if we don't have complete data
            if 'estimated_completion' not in metrics:
                logger.warning(f"Skipping worker count {worker_count} - missing completion data")
                continue
                
            # Only consider configurations with 100% success rate
            if metrics['success_rate'] >= 0.99 and metrics['speed_mbps'] > best_speed:
                best_worker_count = int(worker_count)
                best_speed = metrics['speed_mbps']
        
        if best_worker_count:
            self.optimization_data['best_worker_count'] = best_worker_count
            self.optimization_data['last_optimization'] = datetime.now().isoformat()
            self.save_optimization_data()
            logger.info(f"Optimal worker count found: {best_worker_count}")
            return best_worker_count
        else:
            logger.warning("No optimal worker count found meeting criteria")
            return self.initial_workers
    
    def run_optimization(self):
        """Run the optimization process."""
        try:
            # Check disk space
            free_space_gb = check_disk_space("indafoto_archive")
            logger.info(f"Initial free space: {free_space_gb:.2f}GB")
            
            # Run optimization
            optimal_workers = self.optimize_workers()
            
            # Print summary
            logger.info("\nOptimization Summary:")
            logger.info(f"Best worker count: {optimal_workers}")
            
            # Create a formatted table of all results
            logger.info("\nDetailed Results Table:")
            # Header
            logger.info(f"{'Workers':>6} {'Speed (MB/s)':>12} {'Success Rate':>12} {'Days (1MB avg)':>12} {'Days (2MB avg)':>12}")
            logger.info("-" * 60)
            
            # Sort worker counts numerically
            worker_counts = sorted([int(w) for w in self.optimization_data['worker_counts'].keys()])
            
            for worker_count in worker_counts:
                try:
                    metrics = self.optimization_data['worker_counts'][str(worker_count)]
                    if 'estimated_completion' not in metrics:
                        logger.warning(f"Skipping worker count {worker_count} - missing completion data")
                        continue
                        
                    time_1mb = metrics['estimated_completion']['1.0MB']['days']
                    time_2mb = metrics['estimated_completion']['2.0MB']['days']
                    
                    # Format the row
                    row = (
                        f"{worker_count:>6} "
                        f"{metrics['speed_mbps']:>12.2f} "
                        f"{metrics['success_rate']:>12.2%} "
                        f"{time_1mb:>12.1f} "
                        f"{time_2mb:>12.1f}"
                    )
                    logger.info(row)
                except Exception as e:
                    logger.warning(f"Skipping worker count {worker_count} - error processing data: {e}")
                    continue
            
            return optimal_workers
            
        except Exception as e:
            logger.error(f"Error during optimization: {e}")
            return self.initial_workers
        finally:
            # Clean up sessions
            self.cleanup_sessions()
            self.conn.close()

def main():
    parser = argparse.ArgumentParser(description='Optimize worker count for Indafoto crawler')
    parser.add_argument('--min-workers', type=int, default=MIN_WORKERS,
                       help='Minimum number of workers to test')
    parser.add_argument('--max-workers', type=int, default=MAX_WORKERS,
                       help='Maximum number of workers to test')
    parser.add_argument('--initial-workers', type=int, default=INITIAL_WORKERS,
                       help='Initial number of workers to test')
    args = parser.parse_args()
    
    optimizer = WorkerOptimizer(
        min_workers=args.min_workers,
        max_workers=args.max_workers,
        initial_workers=args.initial_workers
    )
    optimal_workers = optimizer.run_optimization()
    
    logger.info(f"\nOptimization complete. Recommended worker count: {optimal_workers}")
    logger.info("You can now run the crawler with this worker count using:")
    logger.info(f"python indafoto.py --workers {optimal_workers}")

if __name__ == "__main__":
    main() 