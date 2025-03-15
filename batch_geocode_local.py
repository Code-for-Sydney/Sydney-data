import polars as pl
import requests
import logging
from tqdm import tqdm
import concurrent.futures
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='geocoding.log',
    filemode='w'
)
logger = logging.getLogger(__name__)

logger.info("Starting geocoding process")
df = pl.read_csv("sydney_property_data.csv", truncate_ragged_lines=True, separator="\t")[:10000]
logger.info(f"Loaded {len(df)} properties to geocode")

# Create a session to reuse HTTP connections
session = requests.Session()

def geocode_address(combined_address:str, cache:dict, session:requests.Session, base_url="http://localhost:8080"):
    """Geocode a single address using local Nominatim."""
    # Use the combined address directly as the cache key
    cache_key = combined_address
    
    # Check if this address is already in the cache
    if cache_key in cache:
        return cache_key, cache[cache_key]
    
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
                # Store the result in the cache
                cache[cache_key] = (lat, lon)
                return cache_key, (lat, lon)
            logger.warning(f"No results found for {combined_address}")
        else:
            logger.error(f"Error {response.status_code} for {combined_address}")
        # Cache the failure too to avoid repeated API calls for failed addresses
        cache[cache_key] = (None, None)
        return cache_key, (None, None)
    except Exception as e:
        logger.error(f"Exception during geocoding {combined_address}: {str(e)}")
        return cache_key, (None, None)

def process_batch(batch, cache, base_url):
    """Process a batch of addresses with a new session."""
    local_session = requests.Session()
    results = {}
    for combined_address in batch:
        cache_key, coords = geocode_address(combined_address, cache, local_session, base_url)
        results[cache_key] = coords
    return results

# Get the number of available CPU cores (workers)
num_workers = os.cpu_count() or 4
logger.info(f"Using {num_workers} workers for parallel geocoding")

# Prepare data with combined addresses directly
combined_addresses = [f"{row['address']} {str(row['post_code'])}" for row in df.iter_rows(named=True)]

# Calculate batch size based on number of addresses and workers
batch_size = max(1, len(combined_addresses) // (num_workers * 2))
batches = [combined_addresses[i:i + batch_size] for i in range(0, len(combined_addresses), batch_size)]
logger.info(f"Split {len(combined_addresses)} addresses into {len(batches)} batches of ~{batch_size} each")

# Create a shared cache for all workers
geocode_cache = {}

# Process batches in parallel
results = {}
with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
    # Create a progress bar for the batches
    futures = {executor.submit(process_batch, batch, geocode_cache, "http://localhost:8080"): i 
               for i, batch in enumerate(batches)}
    
    with tqdm(total=len(batches), desc="Processing batches", unit="batch") as pbar:
        for future in concurrent.futures.as_completed(futures):
            batch_results = future.result()
            results.update(batch_results)
            pbar.update(1)

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
logger.info(f"Cache hits: {len(geocode_cache)} unique addresses processed")
simplified_df.write_csv("sydney_property_data_geocoded.csv", separator="\t")
logger.info("Saved geocoded data to sydney_property_data_geocoded.csv")