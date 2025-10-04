"""Downloading B3 historical data."""
import os  # noqa: I001
import requests
import urllib3
from zipfile import ZipFile
from tqdm import tqdm

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def download_b3_series(download_url, output_dir):
    """Downloads a ZIP file from the specified URL,
    extracts its contents to the given output directory, and
    removes the ZIP file.

    Args:
        download_url (str): The URL to download the ZIP file from.
        output_dir (str): The directory to save the extracted files.

    Raises:
        Exception: If any error occurs during download, extraction, or file
        operations.

    Note:
        SSL verification is disabled for the download request.
    """  # noqa: D205
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        zip_file_path = os.path.join(output_dir, "data_raw.zip")

        response = requests.get(
            download_url,
            stream=True,
            timeout=10,
            verify=False  # noqa: S501
        )

        response.raise_for_status()

        with open(zip_file_path, "wb") as zip_file:
            for chunk in response.iter_content(chunk_size=8192):
                zip_file.write(chunk)

        with ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)

        os.remove(zip_file_path)
    except Exception as e:
        print(f"Error while processing download: {e}")


if __name__ == "__main__":
    start_year = 2010
    end_year = 2025

    for y in tqdm(range(start_year, end_year + 1)):
        b3_url = ("https://bvmf.bmfbovespa.com.br/" +
                  "InstDados/SerHist/COTAHIST_A{year}.ZIP".format(year=y))
        output_directory = "01__dataprep/data_raw"
        download_b3_series(b3_url, output_directory)
