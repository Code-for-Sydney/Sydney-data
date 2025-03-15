import polars as pl
import requests
import logging
import os
import queue
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='geocoding.log',
    filemode='w'
)
logger = logging.getLogger(__name__)

logger.info("Starting geocoding process")
df = pl.read_csv("sydney_property_data.csv", truncate_ragged_lines=True, separator="\t")[:1000]
logger.info(f"Loaded {len(df)} properties to geocode")

def geocode_address(combined_address:str, session:requests.Session, base_url="http://localhost:8080"):
    """Geocode a single address using local Nominatim."""
    params = {
        'q': combined_address,
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
                return combined_address, (lat, lon)
            logger.warning(f"No results found for {combined_address}")
        else:
            logger.error(f"Error {response.status_code} for {combined_address}")
        return combined_address, (None, None)
    except Exception as e:
        logger.error(f"Exception during geocoding {combined_address}: {str(e)}")
        return combined_address, (None, None)

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
    else:
        logger.warning(f"Failed to geocode {combined_address}")

# Create a simplified dataframe with just the combined address and coordinates
simplified_df = pl.DataFrame({
    "address": combined_addresses,
    "lat": lat_values,
    "lon": lon_values
})

logger.info(f"Geocoding complete. Successfully geocoded {success_count}/{len(df)} properties ({success_count/len(df)*100:.2f}%)")
simplified_df.write_csv("sydney_property_data_geocoded.csv", separator="\t")
logger.info("Saved geocoded data to sydney_property_data_geocoded.csv")