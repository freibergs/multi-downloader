import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading
import time
import logging
from urllib3.exceptions import ProtocolError, IncompleteRead

DOWNLOAD_DIR = "downloads"
TEMP_DIR = "temp"
RETRY_DELAY = 5
MAX_RETRIES = 10
MAX_WORKERS = 5
LOG_FILE = "download.log"
SAFE_URL = "https://www.google.com"
URLS = [
    "https://storage.googleapis.com/public_test_access_ae/output_20sec.mp4",
    "https://storage.googleapis.com/public_test_access_ae/output_30sec.mp4",
    "https://storage.googleapis.com/public_test_access_ae/output_40sec.mp4",
    "https://storage.googleapis.com/public_test_access_ae/output_50sec.mp4",
    "https://storage.googleapis.com/public_test_access_ae/output_60sec.mp4",
]

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

class FileDownloader:
    def __init__(self, download_dir=DOWNLOAD_DIR, temp_dir=TEMP_DIR, retry_delay=RETRY_DELAY, max_retries=MAX_RETRIES):
        self.download_dir = download_dir
        self.temp_dir = temp_dir
        self.retry_delay = retry_delay
        self.max_retries = max_retries

        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        self.progress_lock = threading.Lock()

    def _log(self, message, level=logging.INFO):
        logging.log(level, message)

    def _get_file_size(self, url):
        try:
            response = requests.head(url, timeout=10)
            response.raise_for_status()
            return int(response.headers.get("Content-Length", 0))
        except requests.RequestException as e:
            self._log(f"Failed to get file size for {url}: {e}", logging.WARNING)
            return 0

    def _get_existing_download_size(self, filename, in_temp=True):
        path = os.path.join(self.temp_dir if in_temp else self.download_dir, filename)
        return os.path.getsize(path) if os.path.exists(path) else 0

    def _check_internet(self):
        try:
            requests.get(SAFE_URL, timeout=5)
            return True
        except requests.ConnectionError:
            return False

    def download_file(self, url, progress_bars):
        filename = os.path.basename(url)
        temp_path = os.path.join(self.temp_dir, filename)
        final_path = os.path.join(self.download_dir, filename)
        
        total_size = self._get_file_size(url)
        start_bytes = self._get_existing_download_size(filename, in_temp=True)
        
        self._log(f"Starting/resuming download: {filename} (size: {total_size} bytes, already downloaded: {start_bytes} bytes)")

        if os.path.exists(final_path) and self._get_existing_download_size(filename, in_temp=False) == total_size:
            self._log(f"File already downloaded: {filename}")
            with self.progress_lock:
                progress_bars[filename].n = total_size
                progress_bars[filename].refresh()
            return
        
        headers = {"Range": f"bytes={start_bytes}-"} if start_bytes > 0 else {}
        retries = 0

        while retries <= self.max_retries:
            try:
                with requests.get(url, headers=headers, stream=True, timeout=30) as response:
                    response.raise_for_status()
                    if start_bytes > 0 and response.status_code != 206:
                        self._log(f"Server does not support resume for {filename}. Restarting download.")
                        start_bytes = 0
                        headers = {}

                    progress_bar = progress_bars[filename]
                    mode = "ab" if start_bytes > 0 else "wb"
                    with open(temp_path, mode) as file:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                file.write(chunk)
                                start_bytes += len(chunk)
                                with self.progress_lock:
                                    progress_bar.update(len(chunk))
                    
                    os.replace(temp_path, final_path)
                    self._log(f"Download completed: {filename}")
                    with self.progress_lock:
                        progress_bar.n = total_size
                        progress_bar.refresh()
                    return
            except (requests.ConnectionError, ProtocolError, IncompleteRead):
                self._log(f"Connection lost during download: {filename}. Retrying ({retries}/{self.max_retries})...", logging.WARNING)
                while not self._check_internet():
                    self._log("No internet connection. Retrying in 5 seconds.", logging.WARNING)
                    time.sleep(self.retry_delay)
                start_bytes = self._get_existing_download_size(filename, in_temp=True)
                headers = {"Range": f"bytes={start_bytes}-"}
            except requests.RequestException as e:
                retries += 1
                self._log(f"Error downloading {filename}: {e}. Retrying ({retries}/{self.max_retries})...", logging.ERROR)
                time.sleep(self.retry_delay)
        
        self._log(f"Exceeded maximum retries for {filename}. Download failed.", logging.ERROR)

    def download_files(self, urls, max_workers=MAX_WORKERS):
        file_sizes = {os.path.basename(url): self._get_file_size(url) for url in urls}
        
        progress_bars = {
            os.path.basename(url): tqdm(
                total=file_sizes[os.path.basename(url)],
                unit='B', 
                unit_scale=True, 
                desc=os.path.basename(url),
                position=i,
                leave=True,
                initial=self._get_existing_download_size(
                    os.path.basename(url),
                    in_temp=True
                )
            ) 
            for i, url in enumerate(urls)
        }
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.download_file, url, progress_bars): url
                for url in urls
            }
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self._log(f"Error in worker thread: {e}", logging.ERROR)
                    time.sleep(self.retry_delay)
        
        for bar in progress_bars.values():
            bar.close()


def main():
    downloader = FileDownloader()
    downloader.download_files(URLS)

if __name__ == "__main__":
    main()