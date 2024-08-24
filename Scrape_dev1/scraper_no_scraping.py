import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import datetime
import json
import logging
import pandas as pd
from dateutil import parser
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_event_description(description):
    """
    Parses the event description to extract event name, location, and date using regex.
    Args:
    - description (str): The event description text.
    
    Returns:
    - A tuple containing event name, location, and date as strings, if matched.
    """
    pattern = re.compile(r'^(.*?) - Location: (.*?), Date: (.*)$')
    match = pattern.match(description)
    if match:
        return match.groups()
    else:
        logging.warning(f"Event description does not match expected format: {description}")
        return None, None, None

def html_to_df(table_html):
    """
    Converts HTML table data into a pandas DataFrame.
    Args:
    - table_html (str): HTML content of the table.
    
    Returns:
    - DataFrame: Pandas DataFrame containing the table data.
    """
    try:
        df = pd.read_html(table_html, header=0)[0]  # Assumes the first row is the header
        return df
    except ValueError as e:
        logging.error(f"Failed to convert HTML to DataFrame: {e}")
        return pd.DataFrame()

def scrape_regatta_page(url):
    try:
        response = requests.get(url)
        if response.status_code != 200:
            logging.error(f"Failed to fetch the page. Status code: {response.status_code}")
            return {}
        
        soup = BeautifulSoup(response.content, 'html.parser')
        payload = {'title': soup.head.title.text if soup.head.title else 'No Title'}
        
        h4_tags = soup.find_all('h4')
        if h4_tags:
            event_description = h4_tags[0].text.strip()
            event_name, location, date_str = parse_event_description(event_description)
            payload['event_description'] = event_description
            
            if date_str:
                try:
                    parsed_date = parser.parse(date_str, fuzzy=True)
                    payload.update({
                        'event_name': event_name,
                        'location': location,
                        'parsed_date': parsed_date.isoformat()
                    })
                except ValueError:
                    logging.warning(f"Could not parse date from string: {date_str}")

        tables = soup.find_all('table')
        if tables:
            for index, table in enumerate(tables):
                df = html_to_df(str(table))
                if not df.empty:
                    payload[f'table_{index}'] = df.to_dict(orient='records')

        logging.info(f"Successfully scraped regatta page: {url}")
        return {'url': url, 'payload': payload}
    except Exception as e:
        logging.exception(f"Error scraping regatta page: {url}. Error: {e}")
        return {}

def extract_regatta_links(url):
    try:
        response = requests.get(url)
        regatta_links = []
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)
            for link in links:
                href = link['href']
                if "regatta_id" in href:
                    regatta_url = urljoin(url, href)
                    regatta_links.append(regatta_url)
            logging.info(f"Successfully extracted links from: {url}")
        else:
            logging.error(f"Failed to fetch the page for link extraction. Status code: {response.status_code}")
        return regatta_links
    except Exception as e:
        logging.exception(f"Error extracting regatta links from: {url}. Error: {e}")

def main():
    current_year = datetime.datetime.now().year
    all_data = []
    for year in range(2023, current_year + 1):
        base_url = f"https://www.regattanetwork.com/html/results.php?year={year}"
        regatta_links = extract_regatta_links(base_url)
        logging.info(f"Year {year}: Found {len(regatta_links)} regatta links")
        for link in regatta_links:
            scraped_data = scrape_regatta_page(link)
            if scraped_data:  # Ensure scraped_data is not empty
                all_data.append(scraped_data)
    
    # Saving the compiled data into a JSON file

    with open('regattas_data.json', 'w') as json_file:
        json.dump(all_data, json_file, indent=4)
        logging.info("Successfully saved all scraped data to regattas_data.json")

if __name__ == "__main__":
    main()