import requests
from bs4 import BeautifulSoup
import pandas as pd

# URL of the website
url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"

# Send a GET request to the URL
response = requests.get(url)

# Create a BeautifulSoup object to parse the HTML content
soup = BeautifulSoup(response.content, "html.parser")

# Find all the table rows in the directory listing
rows = soup.find_all("tr")

# Find the corresponding file
desired_date = "2022-02-07 14:03"
desired_file = None

for row in rows:
    columns = row.find_all("td")
    if len(columns) >= 2:
        file_name = columns[0].find("a").text.strip()
        last_modified = columns[1].text.strip()

        if desired_date in last_modified:
            desired_file = file_name
            break

# Check if the desired file is found
if desired_file:
    # Build the URL for the file
    file_url = url + desired_file

    # Download the file
    response = requests.get(file_url)
    file_path = desired_file

    # Write the file locally
    with open(file_path, "wb") as file:
        file.write(response.content)

    # Open the file with Pandas
    df = pd.read_csv(file_path)

    # Find records with the highest HourlyDryBulbTemperature
    max_temperature = df["HourlyDryBulbTemperature"].max()
    max_temperature_records = df[df["HourlyDryBulbTemperature"] == max_temperature]

    # Print the records with the highest temperature
    print("Records with the highest HourlyDryBulbTemperature:")
    print(max_temperature_records)
else:
    print("No file found for the specified date.")
