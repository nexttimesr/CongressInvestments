from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeResult
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import pandas as pd
import requests
import traceback
import os

load_dotenv()

endpoint = os.getenv('ENDPOINT')
key = AzureKeyCredential(os.getenv('KEY'))
document_intelligence_client = DocumentIntelligenceClient(endpoint, key)

# Initialize the WebDriver
session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('https://', adapter)

cService = webdriver.ChromeService(executable_path='/Users/sirui/Downloads/chromedriver-mac-x64/chromedriver')
driver = webdriver.Chrome(service = cService)

base_url = "https://disclosures-clerk.house.gov/"  # Replace with the actual base URL

# Navigate to the form page
driver.get(f"{base_url}FinancialDisclosure#Search")

# Wait for the token and form to load
wait = WebDriverWait(driver, 10)
token_element = wait.until(EC.presence_of_element_located((By.NAME, '__RequestVerificationToken')))
token = token_element.get_attribute('value')

# Set up the form fields
filing_year_element = driver.find_element(By.ID, 'FilingYear')
filing_year_element.send_keys('2024')

# Submit the form
submit_button = driver.find_element(By.XPATH, "//button[@title='Search']")
submit_button.click()

# Wait for the results table to load
wait.until(EC.presence_of_element_located((By.ID, 'DataTables_Table_0')))

# Scrape the data from the table
data = []
page_number = 1
while True:
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    table = soup.find('table', id='DataTables_Table_0')
    rows = table.find('tbody').find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        name = cols[0].text.strip()
        office = cols[1].text.strip()
        filing_year = cols[2].text.strip()
        filing_type = cols[3].text.strip()
        pdf_link = base_url + cols[0].find('a')['href']
        response = requests.get(pdf_link)

        pdf_name = f'/tmp/{name}.pdf'
        with open(pdf_name, 'wb') as f:
            f.write(response.content)

        with open(pdf_name, "rb") as f:
            poller = document_intelligence_client.begin_analyze_document(
                "prebuilt-layout", analyze_request=f, content_type="application/octet-stream"
            )
        result: AnalyzeResult = poller.result()
        for i, table in enumerate(result.tables):
            # Extract the column headers
            columns = [cell['content'] for cell in table['cells'] if cell['rowIndex'] == 0]

            # Initialize a list to hold the rows
            rows = [[None] * len(columns) for _ in range(table['rowCount'] - 1)]

            # Fill the rows with cell data
            for cell in table['cells']:
                if cell['rowIndex'] > 0:
                    rows[cell['rowIndex'] - 1][cell['columnIndex']] = cell['content']

            # Create a DataFrame for the current table and add it to the list
            df = pd.DataFrame(rows, columns=columns)
            data.append(df)

    try:
        page_number += 1
        next_page_button = wait.until(EC.element_to_be_clickable((By.XPATH, f"//a[@data-dt-idx='{page_number}']")))
        next_page_button.click()
        wait.until(EC.presence_of_element_located((By.ID, 'DataTables_Table_0')))
    except Exception as e:
        print("Break!!!")
        print(f"Exception Type: {type(e).__name__}")
        print(f"Exception Message: {str(e)}")
        print("Stack Trace:")
        print(traceback.format_exc())
        break


# Concatenate all DataFrames
final_df = pd.concat(data, ignore_index=True)

# Save to CSV
final_df.to_csv('combined_financial_disclosures_2024.csv', index=False, escapechar='\\')
print("Data saved to combined_financial_disclosures_2024.csv")