import requests
from bs4 import BeautifulSoup
from dateutil import parser
from urllib.parse import urljoin

def scrape_regatta_page(url):
    """
    Scrapes information from a regatta page.
    
    Args:
    - url (str): The URL of the regatta page.
    
    Returns:
    - dict: A dictionary containing the scraped information.
    """
    # Send a GET request to the URL
    response = requests.get(url)
    
    # Initialize an empty dictionary to store scraped information
    regatta_info = {}
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract title
        title_text = soup.head.title.text
        regatta_info['title'] = title_text

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

        regatta_info['event_name'] = event_name
        regatta_info['location'] = location
        regatta_info['date'] = parsed_date.isoformat()

        # Download image
        img_url = soup.body.img['src']
        img_url = urljoin(url, img_url[1:])
        response = requests.get(img_url)

        # Check if the request was successful
        if response.status_code == 200:
            # Save the image to a file
            with open("event_image.jpg", "wb") as f:
                f.write(response.content)
                
            regatta_info['image_url'] = img_url
        else:
            print("Failed to download image")

    else:
        print("Failed to fetch the page. Status code:", response.status_code)
    
    return regatta_info
