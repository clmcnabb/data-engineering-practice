import asyncio
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import aiohttp
import requests

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


async def download_and_extract_async(
    session: aiohttp.ClientSession, uri: str, data_dir: Path
):
    filename = uri.split("/")[-1]
    zip_path = data_dir / filename
    try:
        async with session.get(uri) as response:
            if response.status == 200:
                with open(zip_path, "wb") as f:
                    f.write(await response.read())
            else:
                print(f"Failed to download {uri}")
                return
    except Exception as e:
        print(f"Error downloading {uri}: {e}")
        return

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(data_dir)
    zip_path.unlink()
    print(f"Downloaded and extracted {filename}")


def download_and_extract_sync(uri: str, data_dir: Path):
    filename = uri.split("/")[-1]
    zip_path = data_dir / filename

    response = requests.get(uri)
    if response.status_code == 200:
        with open(zip_path, "wb") as f:
            f.write(response.content)
    else:
        print(f"Failed to download {uri}")
        return

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(data_dir)
    zip_path.unlink()
    print(f"Downloaded and extracted {filename}")


async def main():
    data_dir = Path.cwd() / "downloads"
    data_dir.mkdir(exist_ok=True)
    async with aiohttp.ClientSession() as session:
        tasks = [
            download_and_extract_async(session, uri, data_dir) for uri in download_uris
        ]
        await asyncio.gather(*tasks)


def main_threadpool():
    data_dir = Path.cwd() / "downloads"
    data_dir.mkdir(exist_ok=True)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(download_and_extract_sync, uri, data_dir)
            for uri in download_uris
        ]
        for future in as_completed(futures):
            future.result()


if __name__ == "__main__":
    asyncio.run(main())
    main_threadpool()
