"""
Filter the data to only include properties in Sydney
"""

import polars as pl

df = pl.read_csv("nsw_property_data.csv")
sydney_suburbs = [
    # Eastern Suburbs
    "BONDI", "BONDI JUNCTION", "BRONTE", "CLOVELLY", "DOUBLE BAY", "PADDINGTON", 
    "POINT PIPER", "RANDWICK", "ROSE BAY", "VAUCLUSE", "WAVERLEY", "WOOLLAHRA",
    
    # Inner West
    "ANNANDALE", "ASHFIELD", "BALMAIN", "BURWOOD", "DULWICH HILL", "ENMORE", 
    "ERSKINEVILLE", "GLEBE", "LEICHHARDT", "MARRICKVILLE", "NEWTOWN", "REDFERN", 
    "ROZELLE", "STRATHFIELD", "SUMMER HILL",
    
    # North Shore
    "ARTARMON", "CHATSWOOD", "CREMORNE", "GORDON", "HORNSBY", "KILLARA", 
    "LANE COVE", "LINDFIELD", "MOSMAN", "NORTH SYDNEY", "PYMBLE", "ST LEONARDS", 
    "TURRAMURRA", "WAHROONGA", "WILLOUGHBY",
    
    # Northern Beaches
    "AVALON", "BALGOWLAH", "COLLAROY", "DEE WHY", "FRESHWATER", "MANLY", 
    "MONA VALE", "NEWPORT", "PALM BEACH",
    
    # Western Suburbs
    "AUBURN", "BANKSTOWN", "BLACKTOWN", "CABRAMATTA", "FAIRFIELD", "GRANVILLE", 
    "LIVERPOOL", "MERRYLANDS", "MOUNT DRUITT", "PARRAMATTA", "PENRITH", "WESTMEAD",
    
    # South/South-West
    "BEVERLY HILLS", "BRIGHTON-LE-SANDS", "CAMPSIE", "CANTERBURY", "CRONULLA", 
    "HURSTVILLE", "KOGARAH", "MIRANDA", "ROCKDALE", "SANS SOUCI", "SUTHERLAND",
    
    # Central/CBD
    "BARANGAROO", "DARLING HARBOUR", "DARLINGHURST", "HAYMARKET", "PYRMONT", 
    "SURRY HILLS", "SYDNEY CBD", "THE ROCKS", "ULTIMO", "WOOLLOOMOOLOO"
]

# suburb name is stored in column council_name
df = df.filter(pl.col("council_name").is_in(sydney_suburbs))
print(df.head())

# save the filtered data
df.write_csv("sydney_property_data.csv", separator="\t")

