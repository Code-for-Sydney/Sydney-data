import pytest
import redis
import requests
import time
import os
from batch_geocode_local import (
    get_cached_coordinates,
    cache_coordinates,
    geocode_address,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    CACHE_EXPIRY
)

# Test configuration
TEST_ADDRESS = "14/154 BELLEVUE RD, 2037"
TEST_ADDRESS_STRIPPED = "154 BELLEVUE RD, 2037"
NOMINATIM_BASE_URL = "http://localhost:8080"

@pytest.fixture(scope="module")
def redis_client():
    """Create a Redis client for testing."""
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    # Test connection
    try:
        client.ping()
        print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        pytest.skip(f"Could not connect to Redis: {e}")
    return client

@pytest.fixture(scope="module")
def nominatim_session():
    """Create a session for Nominatim requests."""
    session = requests.Session()
    # Test connection to Nominatim
    try:
        response = session.get(f"{NOMINATIM_BASE_URL}/search", params={"q": "Sydney, Australia", "format": "json", "limit": 1})
        if response.status_code != 200:
            pytest.skip(f"Could not connect to Nominatim: {response.status_code}")
        print(f"Connected to Nominatim at {NOMINATIM_BASE_URL}")
    except Exception as e:
        pytest.skip(f"Could not connect to Nominatim: {e}")
    return session

def test_redis_connection(redis_client):
    """Test that we can connect to Redis."""
    assert redis_client.ping() is True

def test_nominatim_connection(nominatim_session):
    """Test that we can connect to Nominatim."""
    response = nominatim_session.get(f"{NOMINATIM_BASE_URL}/search", params={"q": "Sydney, Australia", "format": "json", "limit": 1})
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    assert "lat" in data[0]
    assert "lon" in data[0]

def test_cache_coordinates(redis_client):
    """Test that we can cache coordinates in Redis."""
    # Clear any existing cache for this test
    redis_client.delete(f"geocode:{TEST_ADDRESS_STRIPPED}")
    
    # Cache coordinates
    cache_coordinates(TEST_ADDRESS_STRIPPED, -33.123, 151.456)
    
    # Verify they were cached
    cached_data = redis_client.get(f"geocode:{TEST_ADDRESS_STRIPPED}")
    assert cached_data is not None
    
    # Parse the cached data
    lat, lon = eval(cached_data)
    assert lat == -33.123
    assert lon == 151.456
    
    # Check TTL is set
    ttl = redis_client.ttl(f"geocode:{TEST_ADDRESS_STRIPPED}")
    assert ttl > 0
    assert ttl <= CACHE_EXPIRY

def test_get_cached_coordinates(redis_client):
    """Test that we can retrieve cached coordinates from Redis."""
    # First cache some coordinates
    redis_client.delete(f"geocode:{TEST_ADDRESS_STRIPPED}")
    cache_coordinates(TEST_ADDRESS_STRIPPED, -33.123, 151.456)
    
    # Now retrieve them
    coords = get_cached_coordinates(TEST_ADDRESS_STRIPPED)
    assert coords is not None
    lat, lon = coords
    assert lat == -33.123
    assert lon == 151.456

def test_geocode_address_with_cache(nominatim_session, redis_client):
    """Test that geocoding uses the cache when available."""
    # Clear any existing cache
    redis_client.delete(f"geocode:{TEST_ADDRESS_STRIPPED}")
    
    # First geocode (should hit Nominatim)
    start_time = time.time()
    address, coords = geocode_address(TEST_ADDRESS, nominatim_session, NOMINATIM_BASE_URL)
    first_duration = time.time() - start_time
    
    assert coords is not None
    assert coords[0] is not None
    assert coords[1] is not None
    
    # Second geocode (should hit cache)
    start_time = time.time()
    address, cached_coords = geocode_address(TEST_ADDRESS, nominatim_session, NOMINATIM_BASE_URL)
    second_duration = time.time() - start_time
    
    assert cached_coords is not None
    assert cached_coords[0] is not None
    assert cached_coords[1] is not None
    assert cached_coords == coords
    
    # Cache should be faster
    assert second_duration < first_duration

def test_geocode_address_without_cache(nominatim_session, redis_client):
    """Test that geocoding works when cache is empty."""
    # Use a unique address to ensure it's not in cache
    unique_address = f"TEST ADDRESS {time.time()}"
    
    # Clear any existing cache
    redis_client.delete(f"geocode:{unique_address}")
    
    # Geocode (should hit Nominatim)
    address, coords = geocode_address(unique_address, nominatim_session, NOMINATIM_BASE_URL)
    
    # Even if Nominatim doesn't find it, we should get a valid response
    assert address == unique_address
    # Note: We don't assert coords values as they might be None if Nominatim doesn't find the address

def test_geocode_address_with_invalid_redis(redis_client, nominatim_session):
    """Test that geocoding works when Redis is unavailable."""
    # Temporarily change Redis host to an invalid one
    original_host = REDIS_HOST
    import batch_geocode_local
    batch_geocode_local.REDIS_HOST = "invalid-host"
    
    try:
        # Geocode should still work
        address, coords = geocode_address(TEST_ADDRESS, nominatim_session, NOMINATIM_BASE_URL)
        # The address should be stripped of the unit number, not the original address
        assert address == TEST_ADDRESS_STRIPPED
        # Note: We don't assert coords values as they might be None if Nominatim doesn't find the address
    finally:
        # Restore original Redis host
        batch_geocode_local.REDIS_HOST = original_host 