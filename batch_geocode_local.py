import polars as pl
import requests
import logging
import os
import queue
import threading
import time
import sys
import json
import redis
import argparse
from datetime import datetime

from addr_utils import strip_unit

# Configure logging to console only
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add console handler to display logs
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Redis configuration
REDIS_HOST = "localhost"
REDIS_PORT = 16379
REDIS_DB = 0
CACHE_EXPIRY = 60 * 60 * 24 * 30  # 30 days in seconds

# Initialize Redis connection
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    redis_client.ping()  # Test connection
    logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    
    # Count and report the number of items in the Redis cache
    cache_keys = redis_client.keys("geocode:*")
    cache_count = len(cache_keys)
    logger.info(f"Redis cache contains {cache_count} geocoded addresses")
    
except Exception as e:
    logger.error(f"Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}: {e}")
    logger.error("This script requires Redis to be running. Please start Redis and try again.")
    sys.exit(1)

def get_cached_coordinates(address):
    """Get cached coordinates from Redis if available."""
    try:
        cached_data = redis_client.get(f"geocode:{address}")
        if cached_data:
            lat, lon = json.loads(cached_data)
            logger.debug(f"Cache hit for {address}")
            return lat, lon
    except Exception as e:
        logger.error(f"Error retrieving from cache: {e}")
        raise  # Re-raise the exception to fail fast
    
    return None

def cache_coordinates(address, lat, lon):
    """Cache coordinates in Redis."""
    if lat is None or lon is None:
        return
    
    try:
        redis_client.setex(
            f"geocode:{address}",
            CACHE_EXPIRY,
            json.dumps([lat, lon])
        )
        logger.debug(f"Cached coordinates for {address}")
    except Exception as e:
        logger.error(f"Error caching coordinates: {e}")
        raise  # Re-raise the exception to fail fast

def geocode_address(combined_address:str, session:requests.Session, base_url="http://localhost:8080"):
    """Geocode a single address using local Nominatim."""
    # Check cache first
    cached_coords = get_cached_coordinates(combined_address)
    if cached_coords:
        return combined_address, cached_coords
    
    params = {
        'q': combined_address + ", NSW, Australia",  # Add NSW (New South Wales) to the query
        'format': 'json',
        'countrycodes': 'au',  # Limit to Australia
        'limit': 1  # Just get the top result
    }
    
    try:
        response = session.get(f"{base_url}/search", params=params)
        if response.status_code == 200:
            results = response.json()
            if results and len(results) > 0:
                lat, lon = float(results[0]['lat']), float(results[0]['lon'])
                # Cache the successful result
                cache_coordinates(combined_address, lat, lon)
                return combined_address, (lat, lon)
            else:
                pass
                #logger.warning(f"No results found for {combined_address} got {results}")
        else:
            logger.error(f"Error {response.status_code} for {combined_address}")
        return combined_address, (None, None)
    except Exception as e:
        logger.error(f"Exception during geocoding {combined_address}: {str(e)}")
        return combined_address, (None, None)

def worker(address_queue, results_dict, results_lock, progress_counter, base_url="http://localhost:8080"):
    """Worker function that processes addresses from a queue."""
    local_session = requests.Session()
    while True:
        try:
            # Get the next address from the queue (non-blocking)
            combined_address = address_queue.get_nowait()
            # Process the address
            address_key, coords = geocode_address(combined_address, local_session, base_url)
            # Store the result in the shared results dictionary
            with results_lock:
                results_dict[address_key] = coords
                # Increment the progress counter
                progress_counter['count'] += 1
            # Mark the task as done
            address_queue.task_done()
        except queue.Empty:
            # No more addresses to process
            break
        except Exception as e:
            logger.error(f"Error processing address: {str(e)}")
            # Ensure task is marked as done even in case of error
            address_queue.task_done()

def progress_reporter(progress_counter, total_addresses, stop_event):
    """Report progress at regular intervals."""
    last_count = 0
    last_time = time.time()
    
    while not stop_event.is_set():
        time.sleep(10)  # Report every 10 seconds
        
        with progress_counter['lock']:
            current_count = progress_counter['count']
            current_time = time.time()
            
            # Calculate progress percentage and rate
            progress_percent = (current_count / total_addresses) * 100
            time_diff = current_time - last_time
            count_diff = current_count - last_count
            
            if time_diff > 0:
                rate = count_diff / time_diff
                eta_seconds = (total_addresses - current_count) / rate if rate > 0 else 0
                eta_str = time.strftime('%H:%M:%S', time.gmtime(eta_seconds))
            else:
                rate = 0
                eta_str = "N/A"
            
            # Log progress
            logger.info(f"Progress: {current_count}/{total_addresses} ({progress_percent:.2f}%) - Rate: {rate:.2f} addr/sec - ETA: {eta_str}")
            
            # Update last values
            last_count = current_count
            last_time = current_time

def main():
    """Main function to run the geocoding process."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Geocode addresses from a CSV file.')
    parser.add_argument('--limit', type=int, help='Limit the number of addresses to process (for testing)')
    args = parser.parse_args()

    logger.info("Starting geocoding process")
    df = pl.read_csv("large-files/sydney_property_data.csv", truncate_ragged_lines=True, separator="\t")
    
    # Apply limit if specified
    if args.limit:
        df = df.head(args.limit)
        logger.info(f"Limited to first {args.limit} addresses for testing")
    
    logger.info(f"Loaded {len(df)} properties to geocode")

    # Strip unit numbers from addresses when preparing the data
    logger.info("Stripping unit numbers from addresses...")
    stripped_addresses = []
    for row in df.iter_rows(named=True):
        address = row['address']
        stripped_address = strip_unit(address)
        stripped_addresses.append(f"{stripped_address} {str(row['post_code'])}")
    
    # Remove duplicates before geocoding
    logger.info("Removing duplicate addresses...")
    unique_addresses = list(set(stripped_addresses))
    logger.info(f"Removed {len(stripped_addresses) - len(unique_addresses)} duplicate addresses")
    logger.info(f"Prepared {len(unique_addresses)} unique addresses for geocoding")

    # Get the number of available CPU cores (workers)
    num_workers = os.cpu_count() or 4
    logger.info(f"Using {num_workers} workers for parallel geocoding")

    # Create a thread-safe queue and populate it with unique addresses
    address_queue = queue.Queue()
    for addr in unique_addresses:
        address_queue.put(addr)

    # Create results dictionary with lock for thread safety
    results = {}
    results_lock = threading.Lock()
    
    # Create a progress counter with a lock
    progress_counter = {'count': 0, 'lock': threading.Lock()}
    
    # Create a stop event for the progress reporter
    stop_event = threading.Event()

    # Start the progress reporter thread
    progress_thread = threading.Thread(
        target=progress_reporter,
        args=(progress_counter, len(unique_addresses), stop_event)
    )
    progress_thread.daemon = True
    progress_thread.start()

    # Create and start worker threads
    threads = []
    total_addresses = len(unique_addresses)
    start_time = time.time()
    logger.info(f"Starting geocoding of {total_addresses} unique addresses with {num_workers} workers")

    for _ in range(num_workers):
        thread = threading.Thread(
            target=worker,
            args=(address_queue, results, results_lock, progress_counter, "http://localhost:8080")
        )
        thread.daemon = True
        thread.start()
        threads.append(thread)

    # Wait for all addresses to be processed
    logger.info("Processing addresses...")
    address_queue.join()
    logger.info("All addresses processed")
    
    # Stop the progress reporter
    stop_event.set()
    progress_thread.join(timeout=1)
    
    elapsed_time = time.time() - start_time
    logger.info(f"Total processing time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    addresses_per_second = total_addresses / elapsed_time if elapsed_time > 0 else 0
    logger.info(f"Processing speed: {addresses_per_second:.2f} addresses/second")

    # Extract results
    success_count = 0
    lat_values = []
    lon_values = []
    
    for combined_address in unique_addresses:
        lat, lon = results.get(combined_address, (None, None))
        lat_values.append(lat)
        lon_values.append(lon)
        if lat is not None and lon is not None:
            success_count += 1

    # Create a simplified dataframe with just the unique combined address and coordinates
    simplified_df = pl.DataFrame({
        "address": unique_addresses,
        "lat": lat_values,
        "lon": lon_values
    })

    logger.info(f"Geocoding complete. Successfully geocoded {success_count}/{len(unique_addresses)} unique properties ({success_count/len(unique_addresses)*100:.2f}%)")
    simplified_df.write_csv("sydney_property_data_geocoded_no_unit.csv", separator="\t")
    logger.info("Saved geocoded data to sydney_property_data_geocoded_no_unit.csv")

if __name__ == "__main__":
    main()