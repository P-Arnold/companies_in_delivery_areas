import pandas as pd 
import geopandas as gpd 
from geopandas.tools import sjoin # rtree
import requests
import math

# Load in the shapefile of current areas
london_areas = gpd.read_file("../data/london_areas_2_6_21/london_areas_2_6_21.shp")

# Load csv of companies
companies_frame = pd.read_csv("../data/London Companies.csv")
companies_frame["lat"] = None
companies_frame["lon"] = None

# Get lat lons for postcodes
none_count = 0
STEP_SIZE = 100 # Max number of postcodes we can query the api with at one time
size = companies_frame.shape[0]
for i in range(math.ceil(size/STEP_SIZE)):
    low_range = i*STEP_SIZE
    high_range = min(((i+1)*STEP_SIZE -1), size-1) # set a limit at the number of rows in the file
    print(f"{low_range} : {high_range}")
    postcodes = companies_frame[low_range:high_range]["Postcode"]
    postcode_data = {
      "postcodes" : postcodes
    }
    response = requests.post("http://api.postcodes.io/postcodes", data=postcode_data).json()
    main_result = response.get("result", {})
    lats = []
    lons = []
    for r in main_result:
        if r and r.get("result"):
            lats.append(r.get("result", {}).get("latitude"))
            lons.append(r.get("result", {}).get("longitude"))
        else:
            lats.append(None)
            lons.append(None)
            none_count += 1
    companies_frame.iloc[low_range:high_range, companies_frame.columns.get_loc('lat')] = lats
    companies_frame.iloc[low_range:high_range, companies_frame.columns.get_loc('lon')] = lons
print(f"None: {none_count}")

# turn company frame into geo frame and do sjoin
companies_frame.dropna(subset=['lat','lon'], inplace=True) # Drop any companies where a lat lon wasn't found
geo_company_frame = gpd.GeoDataFrame(companies_frame, geometry=gpd.points_from_xy(companies_frame.lon, companies_frame.lat))
geo_company_frame.crs = 4326
left_join_companies = sjoin(geo_company_frame, london_areas, how="inner")

# Drop any duplicates and reduce the frame to only the relevant columns
reduced_frame = left_join_companies.drop_duplicates(subset=['Company Name'])
result_frame = left_join_companies[['Company Name', 'Address Line 1', 'Address Line 2', 'Postcode', 'SIC',
       'lat', 'lon', 'delivery_a', 'storefro_2']]

# Write out
result_frame.to_csv("companies_in_delivery_areas.csv", index=False)

