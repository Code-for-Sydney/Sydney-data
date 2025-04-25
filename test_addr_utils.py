import pytest
from addr_utils import strip_unit

def test_strip_unit_with_unit_number():
    """Test stripping unit number from addresses with unit numbers."""
    # Test with unit number at the beginning
    assert strip_unit("14/154 BELLEVUE RD, BELLEVUE HILL 2023") == "154 BELLEVUE RD, BELLEVUE HILL 2023"
    assert strip_unit("1/260 VICTORIA RD, GLADESVILLE 2111") == "260 VICTORIA RD, GLADESVILLE 2111"
    assert strip_unit("3/6 KULGOA AVE, RYDE 2112") == "6 KULGOA AVE, RYDE 2112"
    
    # Test with unit number in the middle
    assert strip_unit("1509/1 NETWORK PL, NORTH RYDE 2113") == "1 NETWORK PL, NORTH RYDE 2113"
    assert strip_unit("703/15 CHATHAM RD, WEST RYDE 2114") == "15 CHATHAM RD, WEST RYDE 2114"
    
    # Test with unit number at the end
    assert strip_unit("505/39 DEVLIN ST, RYDE 2112") == "39 DEVLIN ST, RYDE 2112"
    assert strip_unit("613/27 HALIFAX ST, MACQUARIE PARK 2113") == "27 HALIFAX ST, MACQUARIE PARK 2113"

def test_strip_unit_without_unit_number():
    """Test addresses without unit numbers should remain unchanged."""
    assert strip_unit("56 DUXFORD ST, PADDINGTON 2021") == "56 DUXFORD ST, PADDINGTON 2021"
    assert strip_unit("14 CECIL ST, PADDINGTON 2021") == "14 CECIL ST, PADDINGTON 2021"
    assert strip_unit("27 A DARVALL RD, EASTWOOD 2122") == "27 A DARVALL RD, EASTWOOD 2122"

def test_strip_unit_with_letter_suffix():
    """Test addresses with letter suffixes after the unit number."""
    assert strip_unit("27 A DARVALL RD, EASTWOOD 2122") == "27 A DARVALL RD, EASTWOOD 2122"
    assert strip_unit("2/647 BLAXLAND RD, EASTWOOD 2122") == "647 BLAXLAND RD, EASTWOOD 2122"

def test_strip_unit_with_strata_lot():
    """Test addresses with strata lot numbers."""
    assert strip_unit("1/SP73961") == "SP73961"
    assert strip_unit("2/SP73961") == "SP73961"
    assert strip_unit("218/SP98059") == "SP98059"

def test_strip_unit_edge_cases():
    """Test edge cases for the strip_unit function."""
    # Empty string
    assert strip_unit("") == ""
    
    # String with only unit number
    assert strip_unit("14/") == ""
    
    # String with no street number
    assert strip_unit("14/BELLEVUE RD, BELLEVUE HILL 2023") == "BELLEVUE RD, BELLEVUE HILL 2023"
    
    # String with multiple unit numbers (should only remove the first one)
    assert strip_unit("14/154/123 BELLEVUE RD, BELLEVUE HILL 2023") == "154/123 BELLEVUE RD, BELLEVUE HILL 2023" 