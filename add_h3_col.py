import polars as pl
import h3
import os

# File paths
input_file = os.path.join("sydney-data", "geocoded-addresses.csv")
output_file = os.path.join("sydney-data", "geocoded-addresses-h3.csv")

# Read the geocoded addresses CSV
print(f"Reading data from {input_file}...")
df = pl.read_csv(
    input_file, 
    separator="\t",
    truncate_ragged_lines=True,
    infer_schema_length=10000
)

# Print some stats
total_rows = len(df)
with_coords = df.filter((pl.col("lat").is_not_null()) & (pl.col("lon").is_not_null())).height
print(f"Total rows: {total_rows}")
print(f"Rows with coordinates: {with_coords} ({with_coords/total_rows:.2%})")

# Add H3 column using expression (more efficient than apply)
print("Adding H3 indexes...")
df = df.with_columns([
    pl.struct(["lat", "lon"])
    .map_elements(
        lambda x: h3.latlng_to_cell(float(x["lat"]), float(x["lon"]), 10) if x["lat"] is not None and x["lon"] is not None else None,
        return_dtype=pl.Utf8
    )
    .alias("h3_r10")
])

# Save to new CSV file
print(f"Saving results to {output_file}...")
df.write_csv(output_file, separator="\t")

print("Done!")
