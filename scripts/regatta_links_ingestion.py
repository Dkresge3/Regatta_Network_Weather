import pandas as pd
from bs4 import BeautifulSoup
import argparse

# from postgres_connection import get_postgres_connection
import logging
from datetime import datetime
import requests
import json
import os
from postgres_connection import get_postgres_connection
import pydantic
import time
import random
import uuid

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def raw_database_links() -> pd.DataFrame:
    try:
        post_gres_conn = get_postgres_connection("interactive", "dv")
        logging.debug("Connection to postgres successful")
        cursor = post_gres_conn.cursor()
        query = """ SELECT "Regatta_results_link" FROM regatta_network_raw.regatta_links_raw """
        cursor.execute(query)
        result_links = cursor.fetchall()
        cursor.close()
        post_gres_conn.close()
        df = pd.DataFrame(result_links, columns=["Regatta_results_link"])
        return df
    except Exception as e:
        raise Exception("Connection failed") from e


def extract_regatta_results_page(url: str) -> bytes:
    try:
        response = requests.get(url)
        logging.debug(f"Status code returned: {response.status_code}")
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        # Save response content as a file
        return response.content
    except requests.exceptions.RequestException as e:
        logging.error(f"Error extracting regatta links from: {url}. Error: {e}")


def html_table_to_dataframe_with_links(html_content: bytes) -> pd.DataFrame:

    df_site_table = pd.DataFrame()
    logging.debug("Parsing HTML content")
    tables = pd.read_html(html_content)
    # Assuming the first table is the one you want
    df = tables[1]
    df = df.drop(index=0).reset_index(drop=True)
    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    # # Find the table rows
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
    links_df = links_df.iloc[1:, :].reset_index(drop=True)

    combined_df = df.join(links_df, rsuffix="_link")

    combined_df = combined_df.drop(columns=["2", "3", "0_link", "3_link"])
    new_columns = [
        "Regatta_listed_date",
        "Regatta_listed_name",
        "Regatta_event_link",
        "Regatta_results_link",
    ]
    combined_df.columns = new_columns
    combined_df = combined_df.dropna(how="all")
    return combined_df


def upload_to_db(df: pd.DataFrame) -> None:
    try:
        post_gres_conn = get_postgres_connection("interactive", "dv")
        logging.debug("Connection to postgres successful")
        cursor = post_gres_conn.cursor()
        rows_uploaded = (
            0  # Initialize a variable to keep track of the number of rows uploaded
        )
        for index, row in df.iterrows():
            cursor.execute(
                """INSERT INTO regatta_network_raw.regatta_links_raw ("Regatta_listed_date", "Regatta_listed_name", "Regatta_event_link", "Regatta_results_link", "Ingestion_id", "Ingestion_ts") VALUES (%s, %s, %s, %s, %s, %s)""",
                (
                    row["Regatta_listed_date"],
                    row["Regatta_listed_name"],
                    row["Regatta_event_link"],
                    row["Regatta_results_link"],
                    str(uuid.uuid4()),
                    datetime.now(),
                ),
            )
            rows_uploaded += 1  # Increment the count for each row uploaded
        post_gres_conn.commit()
        cursor.close()
        post_gres_conn.close()
        logging.info(
            f"Uploaded {rows_uploaded} rows to regatta_links_raw"
        )  # Log the number of rows uploaded
    except Exception as e:
        raise Exception("Connection failed") from e


def main(year: list[int] = datetime.now().year):
    combined_df = pd.DataFrame()
    for y in year:
        base_url = f"https://www.regattanetwork.com/html/results.php?year={y}"
        logging.debug(f"Scraping {y} regatta network data {base_url}")
        df_with_links = html_table_to_dataframe_with_links(
            extract_regatta_results_page(base_url)
        )
        # Sleep for 2-6 seconds
        sleep_time = random.uniform(2, 6)
        time.sleep(sleep_time)
        combined_df = pd.concat([combined_df, df_with_links]).reset_index(drop=True)
        current_ingested = raw_database_links().reset_index(drop=True)
        merged_df = combined_df.merge(
            current_ingested, on="Regatta_results_link", how="left", indicator=True
        )
        new_df = merged_df[merged_df["_merge"] == "left_only"].reset_index(drop=True)
        new_df = new_df.drop(columns=["_merge"])
        new_df.to_csv("new_regattas.csv", index=False)
        upload_to_db(new_df)


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
