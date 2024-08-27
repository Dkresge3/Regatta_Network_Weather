import requests
from bs4 import BeautifulSoup
import argparse
import datetime
from postgres_connection import get_postgres_connection
import logging
import random
import time
import json

# Set logging level to debug
logging.basicConfig(level=logging.DEBUG)


def fetch_and_save_html(url):
    # Fetch HTML data from the URL
    logging.debug(f"Fetching HTML data from {url}")
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.content


def main(max_loops):
    try:
        post_gres_conn = get_postgres_connection("interactive", "dv")
        logging.debug("Connection to postgres successful")
        cursor = post_gres_conn.cursor()
        query = "SELECT * from regatta_network_raw.regatta_links_root"
        cursor.execute(query)
        result_links = cursor.fetchall()
        cursor.close()
        post_gres_conn.close()
    except Exception as e:
        raise Exception("Connection failed") from e

    all_data = []
    loop_count = 0
    for link in result_links:
        if max_loops and loop_count >= max_loops:
            break
        loop_count += 1
        time.sleep(random.uniform(1, 6))
        logging.info(f"Scraping {link[3]}")
        scraped_data = fetch_and_save_html(link[3])
        if scraped_data:  # Ensure scraped_data is not empty
            decoded_data = scraped_data.decode("utf-8")
            all_data.append({"Name": link[1], "URL": link[3], "HTML": decoded_data})

    logging.info(f"Scraped {len(all_data)} regatta links")

    # Saving the compiled data into a JSON file
    with open("regattas_data.json", "w") as json_file:
        json.dump(all_data, json_file, indent=4)
        logging.info("Successfully saved all scraped data to regattas_data.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape regatta data.")
    parser.add_argument(
        "--max_loops", type=int, help="The maximum number of loops to run"
    )

    args = parser.parse_args()
    main(args.max_loops)
