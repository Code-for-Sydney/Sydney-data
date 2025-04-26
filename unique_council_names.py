"""
Extract unique council names from Sydney property data
"""

import polars as pl

# Read the CSV file
df = pl.read_csv("large-files/nsw_property_data.csv")

# Get unique council names
unique_councils = df.select("council_name").unique().sort("council_name")

# Save to a CSV file using polars
unique_councils.write_csv("unique_council_names.csv")

print(f"Extracted {len(unique_councils)} unique council names to unique_council_names.txt") 