
import os
import asyncio
import aiohttp
import zipfile

DOWNLOAD_DIR = "downloads"

async def download_file(session, uri):
    async with session.get(uri) as response:
        filename = os.path.basename(uri)
        file_path = os.path.join(DOWNLOAD_DIR, filename)
        with open(file_path, "wb") as file:
            while True:
                chunk = await response.content.read(1024)
                if not chunk:
                    break
                file.write(chunk)
        print(f"Downloaded: {filename}")
        await extract_csv(file_path)
        os.remove(file_path)  # Remove the zip file after extraction

async def extract_csv(file_path):
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            csv_files = [name for name in zip_ref.namelist() if name.endswith('.csv')]
            for csv_file in csv_files:
                zip_ref.extract(csv_file, DOWNLOAD_DIR)
        print(f"Extracted: {file_path}")
    except zipfile.BadZipFile:
        print(f"Skipping extraction: {file_path} is not a valid zip file")

async def download_files_async(download_uris):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for uri in download_uris:
            tasks.append(download_file(session, uri))
        await asyncio.gather(*tasks)

def async_main():
    download_uris = [
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
    ]

    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(download_files_async(download_uris))
    loop.run_until_complete(future)

if __name__ == "__main__":
    async_main()

    