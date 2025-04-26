"""
Filter the data to only include properties in Sydney
"""

import polars as pl

df = pl.read_csv("large-files/nsw_property_data.csv")
sydney_councils = [
    "BAYSIDE",
    "BLACKTOWN",
    "BLUE MOUNTAINS",
    "BURWOOD",
    "CAMDEN",
    "CAMPBELLTOWN",
    "CANADA BAY",
    "CANTERBURY-BANKSTOWN",
    "CITY OF PARRAMATTA",
    "CITY OF SYDNEY",
    "CUMBERLAND",
    "FAIRFIELD",
    "GEORGES RIVER",
    "HAWKESBURY",
    "HORNSBY",
    "HUNTERS HILL",
    "INNER WEST",
    "KU-RING-GAI",
    "LANE COVE",
    "LIVERPOOL",
    "MOSMAN",
    "NORTH SYDNEY",
    "NORTHERN BEACHES",
    "PENRITH",
    "RANDWICK",
    "RYDE",
    "STRATHFIELD",
    "SUTHERLAND",
    "THE HILLS SHIRE",
    "UNINCORPORATED SYDNEY HARBOUR",
    "WAVERLEY",
    "WILLOUGHBY",
    "WOLLONDILLY",
    "WOOLLAHRA"
]

# suburb name is stored in column council_name
df = df.filter(pl.col("council_name").is_in(sydney_councils))
print(df.head())

# save the filtered data
df.write_csv("sydney_property_data.csv", separator="\t")

