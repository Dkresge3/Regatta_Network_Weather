import pandas as pd
from bs4 import BeautifulSoup
import argparse

# from postgres_connection import get_postgres_connection
import logging
from datetime import datetime
import requests


logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)


def html_table_to_dataframe_with_links(html_content):
    df_site_table = pd.DataFrame()
    logging.debug("Parsing HTML content")
    tables = pd.read_html(html_content)

    # Assuming the first table is the one you want
    df = tables[1]
    df = df.drop(index=0).reset_index(drop=True)
    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    # Find the table rows
    table = soup.find_all("table")[1]  # Assuming the second table is the one you want
    rows = table.find_all("tr")

    # Extract links and add them to the DataFrame
    links = []
    for row in rows:
        logging.debug("Processing rows")
        row_links = []
        cells = row.find_all("td")
        logging.debug(f"Found {len(cells)} cells in the row")
        for cell in cells:
            link = cell.find("a")
            if link:
                row_links.append(link.get("href"))
            else:
                row_links.append(None)
        if row_links:
            logging.debug(f"Adding links: {row_links}")
            links.append(row_links)

    # Convert the list of links to a DataFrame
    links_df = pd.DataFrame(links, columns=df.columns)

    # Rename the columns in the links DataFra

    # Combine the original DataFrame with the links DataFrame
    # combined_df = df.join(links_df, rsuffix='_link')
    # combined_df = combined_df.drop(columns=['2', '3'])
    # Rename the columns in the combined DataFrame
    # new_columns = ['Regatta_listed_date', 'Regatta_listed_name', 'Regatta_event_link', 'Regatta_results_link' ...]  # Replace with your desired column names
    # combined_df.columns = new_columns
    # Combine the last two columns in the DataFrame
    # last_column = combined_df.columns[-1]
    # second_to_last_column = combined_df.columns[-2]
    # combined_df['coalesce'] = combined_df.iloc[:, -2:].bfill(axis=1).iloc[:, -1]
    # Drop the original columns
    # combined_df = combined_df.drop(columns=[second_to_last_column, last_column])
    return df_site_table


def get_ingestioned_regatta_list(year):

    try:
        connection = get_postgres_connection("interactive", "dv")
        logging.debug("Connection successful")
        # Execute SQL query
        query = f"SELECT * FROM ingestion_records WHERE condition = 'regatta_date' AND EXTRACT(YEAR FROM regatta_date) IN {year}"
        cursor = connection.cursor()
        cursor.execute(query)
        # Fetch all the rows from the cursor
        rows = cursor.fetchall()
        # Create a DataFrame from the fetched rows
        result_df = pd.DataFrame(
            rows, columns=[column[0] for column in cursor.description]
        )

        # Close the cursor and connection
        cursor.close()
        connection.close()
        return result_df
    except Exception as e:
        raise Exception("Connection failed") from e


def extract_regatta_results_page(url):
    try:
        response = requests.get(url)
        logging.debug(f"Status code returned: {response.status_code}")
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        return response.content
    except requests.exceptions.RequestException as e:
        logging.error(f"Error extracting regatta links from: {url}. Error: {e}")


def main(year):
    for y in year:
        base_url = f"https://www.regattanetwork.com/html/results.php?year={y}"
        logging.debug(f"Scraping {y} regatta network data {base_url}")
        df_with_links = html_table_to_dataframe_with_links(
            extract_regatta_results_page(base_url)
        )
        # combined_df = pd.concat([combined_df, df_with_links], ignore_index=True)

    df_with_links.to_csv("regatta_data.csv", index=False, mode="w")
    return print(df_with_links.head())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process HTML file and extract table with links."
    )
    parser.add_argument(
        "--year",
        nargs="+",
        type=int,
        help="List of years",
        default=[datetime.now().year],
    )
    args = parser.parse_args()
    main(args.year)
