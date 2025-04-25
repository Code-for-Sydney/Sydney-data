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
REDIS_PORT = 6379
REDIS_DB = 0
CACHE_EXPIRY = 60 * 60 * 24 * 30  # 30 days in seconds

# Initialize Redis connection
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    redis_client.ping()  # Test connection
    logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    redis_available = True
except Exception as e:
    logger.warning(f"Could not connect to Redis: {e}. Continuing without caching.")
    redis_available = False

def get_cached_coordinates(address):
    """Get cached coordinates from Redis if available."""
    if not redis_available:
        return None
    
    try:
        cached_data = redis_client.get(f"geocode:{address}")
        if cached_data:
            lat, lon = json.loads(cached_data)
            logger.debug(f"Cache hit for {address}")
            return lat, lon
    except Exception as e:
        logger.warning(f"Error retrieving from cache: {e}")
    
    return None

def cache_coordinates(address, lat, lon):
    """Cache coordinates in Redis."""
    if not redis_available or lat is None or lon is None:
        return
    
    try:
        redis_client.setex(
            f"geocode:{address}",
            CACHE_EXPIRY,
            json.dumps([lat, lon])
        )
        logger.debug(f"Cached coordinates for {address}")
    except Exception as e:
        logger.warning(f"Error caching coordinates: {e}")

def geocode_address(combined_address:str, session:requests.Session, base_url="http://localhost:8080"):
    """Geocode a single address using local Nominatim."""
    stripped_address = strip_unit(combined_address)
    
    # Check cache first
    cached_coords = get_cached_coordinates(stripped_address)
    if cached_coords:
        return stripped_address, cached_coords
    
    params = {
        'q': stripped_address + ", NSW, Australia",  # Add NSW (New South Wales) to the query
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
                cache_coordinates(stripped_address, lat, lon)
                return stripped_address, (lat, lon)
            else:
                pass
                #logger.warning(f"No results found for {combined_address} got {results}")
        else:
            logger.error(f"Error {response.status_code} for {combined_address}")
        return stripped_address, (None, None)
    except Exception as e:
        logger.error(f"Exception during geocoding {combined_address}: {str(e)}")
        return stripped_address, (None, None)

def worker(address_queue, results_dict, results_lock, base_url="http://localhost:8080"):
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
            # Mark the task as done
            address_queue.task_done()
        except queue.Empty:
            # No more addresses to process
            break
        except Exception as e:
            logger.error(f"Error processing address: {str(e)}")
            # Ensure task is marked as done even in case of error
            address_queue.task_done()

def main():
    """Main function to run the geocoding process."""
    logger.info("Starting geocoding process")
    df = pl.read_csv("large-files/sydney_property_data.csv", truncate_ragged_lines=True, separator="\t")
    logger.info(f"Loaded {len(df)} properties to geocode")

    # Get the number of available CPU cores (workers)
    num_workers = os.cpu_count() or 4
    logger.info(f"Using {num_workers} workers for parallel geocoding")

    # Prepare data with combined addresses directly
    combined_addresses = [f"{row['address']} {str(row['post_code'])}" for row in df.iter_rows(named=True)]
    logger.info(f"Prepared {len(combined_addresses)} addresses for geocoding")

    # Create a thread-safe queue and populate it with addresses
    address_queue = queue.Queue()
    for addr in combined_addresses:
        address_queue.put(addr)

    # Create results dictionary with lock for thread safety
    results = {}
    results_lock = threading.Lock()

    # Create and start worker threads
    threads = []
    total_addresses = len(combined_addresses)
    start_time = time.time()
    logger.info(f"Starting geocoding of {total_addresses} addresses with {num_workers} workers")

    for _ in range(num_workers):
        thread = threading.Thread(
            target=worker,
            args=(address_queue, results, results_lock, "http://localhost:8080")
        )
        thread.daemon = True
        thread.start()
        threads.append(thread)

    # Wait for all addresses to be processed
    logger.info("Processing addresses...")
    address_queue.join()
    logger.info("All addresses processed")
    elapsed_time = time.time() - start_time
    logger.info(f"Total processing time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    addresses_per_second = total_addresses / elapsed_time if elapsed_time > 0 else 0
    logger.info(f"Processing speed: {addresses_per_second:.2f} addresses/second")

    # Extract results
    success_count = 0
    lat_values = []
    lon_values = []

    for combined_address in combined_addresses:
        lat, lon = results.get(combined_address, (None, None))
        lat_values.append(lat)
        lon_values.append(lon)
        if lat is not None and lon is not None:
            success_count += 1

    # Create a simplified dataframe with just the combined address and coordinates
    simplified_df = pl.DataFrame({
        "address": combined_addresses,
        "lat": lat_values,
        "lon": lon_values
    })

    logger.info(f"Geocoding complete. Successfully geocoded {success_count}/{len(df)} properties ({success_count/len(df)*100:.2f}%)")
    simplified_df.write_csv("sydney_property_data_geocoded_no_unit.csv", separator="\t")
    logger.info("Saved geocoded data to sydney_property_data_geocoded_no_unit.csv")

if __name__ == "__main__":
    main()