import json
import pandas as pd
import requests
from dateutil import parser
from bs4 import BeautifulSoup

# Send a GET request to the URL
url = "https://www.regattanetwork.com/clubmgmt/applet_regatta_results.php?regatta_id=26198"
page = requests.get(url)

# Initialize an empty dictionary to store JSON objects for each iteration
payload = {}

# Get the HTML content of the page
soup = BeautifulSoup(page.content, 'html.parser')

# Get title
title_text = soup.head.title.text

# Find event description
h4_tags = soup.find_all('h4')
event_description_tag = h4_tags[0].text.strip()

# Split the event description based on '\r\n'
event_description_parts = event_description_tag.split('\r\n')

# Split the second part of the event description based on ' | ' to get location and date
location_and_date = event_description_parts[1].split(' | ')

# Extract event name, location, and date
event_name = event_description_parts[0]
location = location_and_date[0]
date = location_and_date[1]

parsed_date = parser.parse(date)

# Download image
img_url = soup.body.img['src']
img_url = "https://www.regattanetwork.com/clubmgmt" + img_url[1:]
response = requests.get(img_url)

# Check if the request was successful
if response.status_code == 200:
    # Save the image to a file
    with open("event_image.jpg", "wb") as f:
        f.write(response.content)
else:
    print("Failed to download image")

# Add title, event name, location, date, and image URL to the payload dictionary
payload['title'] = title_text
payload['event_name'] = event_name
payload['location'] = location
payload['parsed_date'] = parsed_date.isoformat()
payload['image_url'] = img_url

# Find all tables containing race results
race_results = soup.find_all('div', class_='scoring-table-wrapper')

# Find all h2 tags containing fleet information
h2_tags = soup.find_all('h2')

# Iterate over each fleet and its corresponding race results
for index, tag in enumerate(h2_tags):
    # Extract fleet name and description
    fleet_name = tag.a['name']
    fleet_description = tag.find('font', size='3').text.strip()

    # Initialize the JSON object for this fleet
    fleet_data = {
        'fleet_name': fleet_name,
        'fleet_description': fleet_description
    }

    # Find the corresponding table for this fleet
    table = race_results[index].find('table', class_='scoring-table')
    
    # Check if table exists
    if table:
        # Read the HTML table into a DataFrame, skipping the first row
        df = pd.read_html(str(table), flavor='bs4', header=0)[0]
        
        # Drop specific columns if they exist
        columns_to_drop = ['Unnamed: 6', 'Unnamed: 11']
        df = df.drop(columns=columns_to_drop, errors='ignore')

        # Convert DataFrame to JSON-compatible format
        fleet_data['data'] = df.to_dict(orient='records')
    else:
        fleet_data['data'] = []  # Empty list if table does not exist

    # Add fleet data to the payload dictionary with index as key
    payload[index] = fleet_data

# Convert the payload dictionary to a JSON string
json_data = json.dumps(payload, indent=4)

# Now, json_data contains the JSON string representing the payload dictionary

