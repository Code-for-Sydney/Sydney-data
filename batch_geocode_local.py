import polars as pl
import requests
import logging
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='geocoding.log',
    filemode='w'
)
logger = logging.getLogger(__name__)

logger.info("Starting geocoding process")
df = pl.read_csv("sydney_property_data.csv", truncate_ragged_lines=True, separator="\t")
logger.info(f"Loaded {len(df)} properties to geocode")

# Create a cache to store geocoded addresses
geocode_cache = {}

def geocode_address(address:str, postcode:str, base_url="http://localhost:8080"):
    """Geocode a single address using local Nominatim."""
    # Create a cache key from address and postcode
    cache_key = f"{address}_{postcode}"
    
    # Check if this address is already in the cache
    if cache_key in geocode_cache:
        return geocode_cache[cache_key]
    
    params = {
        'q': address + " " + postcode,
        'format': 'json',
        'countrycodes': 'au',  # Limit to Australia
        'limit': 1  # Just get the top result
    }
    
    try:
        response = requests.get(f"{base_url}/search", params=params)
        if response.status_code == 200:
            results = response.json()
            if results and len(results) > 0:
                lat, lon = float(results[0]['lat']), float(results[0]['lon'])
                # Store the result in the cache
                geocode_cache[cache_key] = (lat, lon)
                return lat, lon
            logger.warning(f"No results found for {address}, {postcode}")
        else:
            logger.error(f"Error {response.status_code} for {address}, {postcode}")
        # Cache the failure too to avoid repeated API calls for failed addresses
        geocode_cache[cache_key] = (None, None)
        return None, None
    except Exception as e:
        logger.error(f"Exception during geocoding {address}, {postcode}: {str(e)}")
        return None, None

# the address is stored in column address and the postal code in post_code
success_count = 0
lat_values = []
lon_values = []

for row in tqdm(df.iter_rows(named=True), desc="Geocoding addresses", unit="property", total=len(df)):
    address = row['address']
    postcode = str(row['post_code'])
    lat, lon = geocode_address(address, postcode)
    lat_values.append(lat)
    lon_values.append(lon)
    if lat is not None and lon is not None:
        success_count += 1
    else:
        logger.warning(f"Failed to geocode {address}, {postcode}")
# Add the latitude and longitude columns to the dataframe
df = df.with_columns([
    pl.Series("lat", lat_values),
    pl.Series("lon", lon_values)
])

logger.info(f"Geocoding complete. Successfully geocoded {success_count}/{len(df)} properties ({success_count/len(df)*100:.2f}%)")
logger.info(f"Cache hits: {len(geocode_cache)} unique addresses processed")
df.write_csv("sydney_property_data_geocoded.csv", separator="\t")
logger.info("Saved geocoded data to sydney_property_data_geocoded.csv")