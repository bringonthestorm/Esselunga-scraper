import folium
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors


# Initialize geocoder
geolocator = Nominatim(user_agent="geoapiExercises", timeout=10)

# Create a folium map centered on Italy
italy_map = folium.Map(location=[41.8719, 12.5674], zoom_start=6)

# Function to get latitude and longitude from town + post code
def get_lat_lon(location, max_retries=3, wait_time=2):
    retries = 0
    while retries < max_retries:
        try:
            loc = geolocator.geocode(location)
            if loc:
                return (loc.latitude, loc.longitude)
            else:
                return (None, None)
        except GeocoderTimedOut:
            print(f"Timeout error for location {location}. Retrying ({retries + 1}/{max_retries})...")
            retries += 1
            time.sleep(wait_time)
    print(f"Max retries exceeded for location {location}.")
    return (None, None)

def generate_colors(n):
    cmap = plt.get_cmap('tab20')  # Tab20 has 20 base colors
    return [mcolors.rgb2hex(cmap(i % 20)) for i in range(n)]

# Map ids to colors dynamically using a pool of colors
colors_pool = generate_colors(1000)

# Function to determine the marker color based on the id

with open('store_to_map_new.json', 'r') as file:
    ssmap = json.load(file)

ssmaps = sorted([int(k) for k in ssmap.keys() if int(k) < 1000000])

def get_key_by_value(dictionary, target_value):
    for key, value_list in dictionary.items():
        if target_value in value_list:
            return key  # Return the key as soon as we find it
    return None

def get_marker_color(ssmap, street_id):
    id = get_key_by_value(ssmap, street_id)
    if id is not None:
        return colors_pool[int(id) % 1000]
    else:
        return colors_pool[0]


def process_location(entry):
    location_str = f"{entry['value']}, {entry['town']}, {entry['postCode']}"
    lat, lon = get_lat_lon(location_str)
    if lat and lon:
        return (lat, lon, entry['value'])
    else:
        return (None, None, entry['value'])


# Loop through the data and add markers to the map
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(process_location, streets[k]): k for k in streets.keys()}
    for future in as_completed(futures):
        lat, lon, value = future.result()
        if lat and lon:
            color = get_marker_color(ssmap, street_id)
            # Add a marker with the specified color
            folium.Marker([lat, lon], popup=value, icon=folium.Icon(color=color)).add_to(italy_map)
        else:
            print(f"Location not found for: {value}")


# Save map to an HTML file
italy_map.save('italy_map.html')

# Display the map in a Jupyter Notebook (if using a notebook environment)
italy_map
