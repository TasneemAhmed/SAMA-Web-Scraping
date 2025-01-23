import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, unquote
import logging

"""
We configure logging using basicConfig() to set the logging level to INFO. 
This means that only messages with severity level INFO and higher will be logged.
"""
logging.basicConfig(level=logging.INFO)

def download_sama_xlsx_file(save_directory, archive_directory):
    """
    Download an Excel file (.xlsx) from the SAMA Monthly Statistics page in current working directory if doesn't exist in Archive directory

    """
    archive_found = False # change to true if file exist in Archive directory
    # Ensure the archive directory exists
    if not os.path.exists(archive_directory):
        os.makedirs(archive_directory)
        logging.info(f"Created archive directory: {archive_directory}")
    try:
        # URL of the SAMA Monthly Statistics page
        url = "https://www.sama.gov.sa/ar-sa/EconomicReports/Pages/MonthlyStatistics.aspx"
        """
        User-Agent: describe the client/ machine which connect with the server
        """
        #headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.61 Safari/537.36'}
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}
        # Send a GET request to the URL
        response = requests.get(url, headers=headers)
    
        # Raise an HTTPError for bad status codes
        response.raise_for_status()

        # Parse HTML
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all links on the page
        links = soup.find_all('a', href=True)

        # Filter links to find the one with .xlsx extension
        xlsx_link = None
        for link in links:
            href = link.get('href')
            if href and href.endswith('.xlsx'):
                xlsx_link = href
                break

        if not xlsx_link:
            raise ValueError("No .xlsx file found on the page.")

        # Create the full URL of the Excel file
        base_url = urlparse(url)
        file_url = urljoin(f"{base_url.scheme}://{base_url.netloc}", xlsx_link)

        # Parse the file name from the URL
        parsed_url = urlparse(file_url)
        file_name = os.path.basename(parsed_url.path)

        # Decode URL-encoded file name if necessary
        file_name = unquote(file_name)

        # Define the local file path and archive file path
     
        local_file_path = os.path.join(save_directory, file_name)
        archive_file_path = os.path.join(archive_directory, file_name) 

        # Check if file already exists in archive
        if os.path.exists(archive_file_path):
            logging.info(f"File already exists in archive. Skipping download.")
            archive_found = True
        
        elif archive_found==False: 
            logging.info("The file not in Archive, Start Downloading...")
            # Download the file inside the current working directory
            response = requests.get(file_url, headers=headers)
            if response.status_code == 200:
                with open(local_file_path, 'wb') as file:
                    file.write(response.content)

                logging.info(f"File downloaded successfully as: {local_file_path}")        

    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading file: {e}")
    except ValueError as ve:
        logging.error(ve)

#save in current working directory
save_directory = os.getcwd()
archive_directory = os.path.join(save_directory, 'Archive') # Join the current working directory with the subdirectory 'Archive'

# Call the function:
downloaded_file_name = download_sama_xlsx_file(save_directory, archive_directory)
if downloaded_file_name:
    print(f"Downloaded file name: {downloaded_file_name}")
