from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import requests
import time


def extract_basic_info_from_GO(url, cycle_number):
    """
    Extract data from a SINGLE jwst GO cycle page.
    ** Specifically, this function extracts NIRSpec/MOS information from the tables on the page **

    e.g. extract_data_from_cycle_GO("Cycle 1","https://www.stsci.edu/jwst/science-execution/approved-programs/general-observers/cycle-1-go")

    Args:
        url (str): URL of the page to extract data from
        cycle_number (str): Cycle number of the proposals
    Returns:
        data (list): List of lists containing the extracted data
        headers (list): List of headers
    """
    print(f"Extracting info for {cycle_number}...")

    # Request the page
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Parse the page content
    soup = BeautifulSoup(response.content, 'html.parser')

    # Parse the content to extract proposals from all tables
    tables = soup.find_all('table')  # Find all tables
    data = []
    headers = None

    # Find all titles (topics) corresponding to tables
    topics = soup.find_all('span', class_='accordion__title-text')
    for topic, table in zip(topics, tables):
        rows = table.find_all('tr')
        if not rows:
            continue  # Skip empty tables

        # Assuming first row is headers
        current_headers = [header.get_text(strip=True) for header in rows[0].find_all('th')]
        
        if headers is None:  # Set headers only once
            headers = current_headers
        
        topic_text = topic.get_text(strip=True)

        for row in rows[1:]:
            columns = row.find_all('td')
            if len(columns) != len(headers):
                continue  # Skip rows that do not match the header length

            if "Instrument/ Mode" not in headers:
                instrument_mode = columns[headers.index("Instrument/Mode")].get_text(strip=True).replace('\n', '').replace('\r', '').replace(' ', '')
            else:
                instrument_mode = columns[headers.index("Instrument/ Mode")].get_text(strip=True).replace('\n', '').replace('\r', '').replace(' ', '')
            if "NIRSpec/MOS" in instrument_mode:
                row_data = [col.get_text(strip=True) for col in columns]
                row_data.append(topic_text)  # Add the topic text to the row data
                row_data.append(cycle_number)  # Add the cycle number to the row data
                data.append(row_data)

    return data, headers




def get_observation_status(proposal_id, retries=3):
    """
    Get the jwst observation status for a given Proposal ID
    ** Specifically, this function fetches the NIRSpec/MOS data from the tables on the page **

    Args:
        proposal_id (str): JWST Proposal ID to fetch data for
        retries (int): Number of retries in case of connection errors
    """
    print(f"Fetching status for Proposal ID: {proposal_id}")

    url = f"https://www.stsci.edu/cgi-bin/get-visit-status?id={proposal_id}&markupFormat=html&observatory=JWST&pi=1"
    session = requests.Session()
    attempt = 0
    while attempt < retries:
        try:
            response = session.get(url, timeout=60)
            response.raise_for_status()  # Raise an error for bad status codes
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all tables with observation status
            tables = soup.find_all("table")
            if not tables:
                print(f"No tables found for Proposal ID {proposal_id}")
                return [], []  # Return two empty lists if no tables are found
            
            all_status_data = []
            all_headers = []  # Initialize as a list to maintain order
            
            for table in tables:
                # Get all rows in the table
                rows = table.find_all('tr')
                
                # Parse the table headers (first row)
                headers = [header.get_text(strip=True) for header in rows[0].find_all('td')]
                if headers:  # Ensure headers are not empty
                    all_headers.extend([header for header in headers if header not in all_headers])  # Add only new headers
                    print(f"Headers: {headers}")  # Debug: Output the headers
                
                    # Locate the "Template" column index
                    template_index = headers.index("Template") if "Template" in headers else -1
                    
                    # Parse the rows in the table (skip header)
                    rows = rows[1:]  # Skip the header row
                    print(f"Number of rows found: {len(rows)}")  # Debug: Output the number of rows found
                    
                    for row in rows:
                        columns = row.find_all('td')
                        
                        if len(columns) != len(headers):
                            continue  # Skip rows that don't match the header length
                        
                        row_data = [col.get_text(strip=True) for col in columns]
                        print(f"Row data: {row_data}")  # Debug: Output the data in this row
                        
                        # Check if the "Template" matches "NIRSpec MultiObject Spectroscopy"
                        if template_index != -1 and "NIRSpec MultiObject Spectroscopy" in row_data[template_index]:
                            row_dict = dict(zip(headers, row_data))  # Map header to row data
                            all_status_data.append(row_dict)
            
            return all_status_data, all_headers
        
        except (requests.ConnectionError, requests.Timeout) as e:
            print(f"Connection error for Proposal ID {proposal_id}: {e}. Retrying...")
            attempt += 1
            time.sleep(5)  # Wait before retrying
    
    print(f"Failed to fetch data for Proposal ID {proposal_id} after {retries} retries.")
    return [], []  # Return two empty lists in case of failure

def check_csv(basic_info_file,status_file):
    """
    Check if all the proposals in the basic_info_file have been checked for status.
    """
    df1 = pd.read_csv(basic_info_file)
    df2 = pd.read_csv(status_file)
    df1id = df1['ID'].tolist()
    df2id = df2['ID'].tolist()
    if set(df1id).issubset(set(df2id)):
        print('All checked')
    else:
        print('Not all checked, missing:')
        print(set(df1id)-set(df2id))


def extract_basic_info_from_GTO(url):
    """
    Extract data from a SINGLE jwst GTO page 
    ** Specifically, this function extracts NIRSpec/MOS information from the tables on the page **
    """
    print("Extracting info for GTO...")

    response = requests.get(url)
    response.raise_for_status() 
    soup = BeautifulSoup(response.content, 'html.parser')

    tables = soup.find_all('table')  # Find all tables
    data = []
    headers = None

    for table in tables:
        rows = table.find_all('tr')
        if not rows:
            continue  # Skip empty tables

        current_headers = [header.get_text(strip=True) for header in rows[0].find_all('th')]
        
        if headers is None:  # Set headers only once
            headers = current_headers
        
        for row in rows[1:]:
            columns = row.find_all('td')
            if len(columns) != len(headers):
                continue  # Skip rows that do not match the header length

            if "Instrument/Mode" in headers:
                instrument_mode = columns[headers.index("Instrument/Mode")].get_text(strip=True)
            elif "Instrument/ Mode" in headers:
                instrument_mode = columns[headers.index("Instrument/ Mode")].get_text(strip=True)
            else:
                continue

            if "NIRSpec/MOS" in instrument_mode:
                row_data = [col.get_text(strip=True) for col in columns]

                # Programs with "AR" icon have components that have no exclusive access period
                # and can be used as a basis for GO Archival  Research (AR) Proposals.
                if "AR?" in headers: 
                    ar_index = headers.index("AR?")
                    if columns[ar_index].find('img'): 
                        row_data[ar_index] = "AR"
                
                data.append(row_data)

    return data, headers



def extract_basic_info_from_DDT(url):
    """
    Extract data from a SINGLE jwst DDT page 
    ** Specifically, this function extracts NIRSpec information from the tables on the page **
    """
    print("Extracting info for DDT...")

    response = requests.get(url)
    response.raise_for_status() 
    soup = BeautifulSoup(response.content, 'html.parser')

    tables = soup.find_all('table')  # Find all tables
    data = []
    headers = None

    for table in tables:
        rows = table.find_all('tr')
        if not rows:
            continue  # Skip empty tables

        current_headers = [header.get_text(strip=True) for header in rows[0].find_all('th')]
        
        if headers is None:  # Set headers only once
            headers = current_headers
        
        for row in rows[1:]:
            columns = row.find_all('td')
            if len(columns) != len(headers):
                continue  # Skip rows that do not match the header length

            if "Instruments" in headers:
                instrument_mode = columns[headers.index("Instruments")].get_text(strip=True)
            else:
                continue

            if "NIRSpec" in instrument_mode:
                row_data = [col.get_text(strip=True) for col in columns]
                data.append(row_data)

    return data, headers