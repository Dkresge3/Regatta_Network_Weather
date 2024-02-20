import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import datetime

def extract_regatta_links(url):
    """
    Extracts links to individual regatta pages from the given URL.
    
    Args:
    - url (str): The URL of the page containing regatta links.
    
    Returns:
    - list: A list of URLs to individual regatta pages.
    """
    # Send a GET request to the URL
    response = requests.get(url)
    
    # Initialize an empty list to store regatta links
    regatta_links = []
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all links on the page
        links = soup.find_all('a', href=True)
        
        # Extract links to individual regatta pages
        for link in links:
            href = link['href']
            if "regatta_id" in href:
                regatta_url = urljoin(url, href)
                regatta_links.append(regatta_url)
    
    else:
        print("Failed to fetch the page. Status code:", response.status_code)
    
    return regatta_links

def main():
    # Get the current year
    current_year = datetime.datetime.now().year
    
    # Loop through the years from 2008 to the current year
    for year in range(2020, current_year + 1):
        # URL of the page containing regatta links for the current year
        base_url = "https://www.regattanetwork.com/html/results.php?year={}".format(year)
        
        # Extract links to individual regatta pages for the current year
        regatta_links = extract_regatta_links(base_url)
        
        # Print the extracted links for the current year
        print("Year {}: Found {} regatta links".format(year, len(regatta_links)))
        for link in regatta_links:
            print(link)
        print()  # Add an empty line between years

if __name__ == "__main__":
    main()
