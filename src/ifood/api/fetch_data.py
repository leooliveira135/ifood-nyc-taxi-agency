import logging
import re
import requests
import time
from bs4 import BeautifulSoup

def fetch_data_from_source(endpoint: str, filename: str, filter_date: str) -> list:
    """
        Fetch data from the specified endpoint and filter by filename and date.
        Args:
            endpoint (str): The URL to fetch data from.
            filename (str): The name of the file section to search for.
            filter_date (str): The date string to filter the results.
        Returns:
            list: A list of URLs matching the criteria.
    """
    response = requests.get(endpoint)
    time.sleep(10)
    if response.status_code != 200:
        logging.error(f"Failed to access the endpoint: {response.text}")
        raise ValueError(f"Failed to retrieve page, status code: {response.status_code}")
    else:
        logging.info("Page retrieved successfully")
        soup = BeautifulSoup(response.text, 'html.parser')
        search_links = re.compile(re.escape(filename), re.I)
        el = soup.find(string=search_links)
        if not el:
            raise ValueError(f"Could not find the {filename} section")
        else:
            matches = []
            output_files = []
            for a in soup.find_all('a', href=True):
                if search_links.search(a.get_text(" ", strip=True)):
                    matches.append(a)
            for match in matches:
                if match['href'].find(filter_date) != -1:
                    logging.info(f"Matched link: {match['href']}")
                    output_files.append(match['href'])
    
    return output_files