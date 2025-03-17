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
        
        # Keep trying pages until we find one with new images
        test_page = self.get_next_test_page()
        
        while True:
            search_page_url = f"https://indafoto.hu/search/list?profile=main&sphinx=1&search=advanced&textsearch=fulltext&textuser=&textmap=&textcompilation=&photo=&datefrom=&dateto=&licence%5B1%5D=I1%3BI2%3BII1%3BII2&licence%5B2%5D=I1%3BI2%3BI3&page_offset={test_page}"
            
            # Get image data
            image_data_list = get_image_links(search_page_url)
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
            'test_page': test_page  # Add the page number to metrics
        }
        
        logger.info(f"Test results for {worker_count} workers (page {test_page}):")
        logger.info(f"  Speed: {speed_mbps:.2f} MB/s")
        logger.info(f"  Success rate: {success_rate:.2%} (of {total_downloads} actual downloads)")
        logger.info(f"  Processed: {stats['processed_count']}")
        logger.info(f"  Failed: {stats['failed_count']}")
        logger.info(f"  Skipped: {stats['skipped_count']}")
        
        return metrics
    
    def optimize_workers(self):
        """Find the optimal number of workers through testing."""
        logger.info("Starting worker optimization...")
        
        # Test different worker counts
        worker_counts = list(range(self.min_workers, self.max_workers + 1, 2))  # Test odd numbers
        worker_counts.extend(list(range(self.min_workers + 1, self.max_workers + 1, 2)))  # Test even numbers
        worker_counts = sorted(list(set(worker_counts)))  # Remove duplicates and sort
        
        logger.info(f"Will test worker counts: {worker_counts}")
        
        for worker_count in worker_counts:
            # Test current worker count
            metrics = self.test_worker_count(worker_count)
            if not metrics:
                logger.warning(f"Failed to get metrics for {worker_count} workers, skipping...")
                continue
                
            # Store results
            self.optimization_data['worker_counts'][str(worker_count)] = metrics
            
            # If we see a significant drop in performance, we can stop testing higher counts
            if worker_count > self.min_workers:
                prev_metrics = self.optimization_data['worker_counts'].get(str(worker_count - 1))
                if prev_metrics:
                    # Only consider actual downloads for performance comparison
                    prev_speed = prev_metrics['speed_mbps']
                    curr_speed = metrics['speed_mbps']
                    prev_success = prev_metrics['success_rate']
                    curr_success = metrics['success_rate']
                    
                    # If current performance is significantly worse than previous
                    if (curr_speed < prev_speed * 0.8 or  # 20% slower
                        curr_success < prev_success * 0.9):  # 10% lower success rate
                        logger.info(f"Performance degraded at {worker_count} workers, stopping tests")
                        logger.info(f"Previous speed: {prev_speed:.2f} MB/s, Current speed: {curr_speed:.2f} MB/s")
                        logger.info(f"Previous success rate: {prev_success:.2%}, Current success rate: {curr_success:.2%}")
                        break
        
        # Find the best worker count
        best_worker_count = None
        best_speed = 0
        
        for worker_count, metrics in self.optimization_data['worker_counts'].items():
            if (metrics['success_rate'] >= MIN_SUCCESS_RATE and 
                metrics['speed_mbps'] > best_speed):
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
            logger.info("\nDetailed results:")
            for worker_count, metrics in self.optimization_data['worker_counts'].items():
                logger.info(f"\nWorkers: {worker_count}")
                logger.info(f"  Speed: {metrics['speed_mbps']:.2f} MB/s")
                logger.info(f"  Success rate: {metrics['success_rate']:.2%}")
                logger.info(f"  Processed: {metrics['processed_count']}")
                logger.info(f"  Failed: {metrics['failed_count']}")
                logger.info(f"  Skipped: {metrics['skipped_count']}")
            
            return optimal_workers
            
        except Exception as e:
            logger.error(f"Error during optimization: {e}")
            return self.initial_workers
        finally:
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