import re

def strip_unit(full_addr: str) -> str:
    """
    Extract the base address without the unit number from an Australian address.
    
    Args:
        full_addr: A string containing an Australian address, which may include a unit number.
        
    Returns:
        The address without the unit number.
        
    Examples:
        >>> strip_unit("14/154 BELLEVUE RD, BELLEVUE HILL 2023")
        "154 BELLEVUE RD, BELLEVUE HILL 2023"
        >>> strip_unit("56 DUXFORD ST, PADDINGTON 2021")
        "56 DUXFORD ST, PADDINGTON 2021"
    """
    if not full_addr:
        return ""
    
    # Pattern to match unit numbers at the beginning of the address
    # This matches patterns like "14/", "1/", "1509/", etc.
    unit_pattern = r'^\d+/(?=\d+|\w+|$)'
    
    # Check if the address starts with a unit number
    match = re.match(unit_pattern, full_addr)
    if match:
        # Remove the unit number and return the rest of the address
        remainder = full_addr[match.end():]
        return remainder if remainder else ""
    
    # If no unit number is found, return the original address
    return full_addr 