import json
import yt_dlp
import requests
from bs4 import BeautifulSoup
import sys
import os
import logging
from tqdm import tqdm
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import getpass
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(filename='downloader.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
MAX_RETRIES = 3
BACKOFF_FACTOR = 0.3
REQUEST_TIMEOUT = 10  # seconds
CHUNK_SIZE = 1024  # For large downloads


def create_session():
    """Creates a requests session with retry logic and backoff."""
    session = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_json(url, session):
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }
    try:
        with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as response:
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            data = soup.find(type="application/json")
            if data:
                for child in data.children:
                    return json.loads(child.string)
            logging.error(f"No JSON data found at URL: {url}")
    except (HTTPError, ConnectionError, Timeout) as e:
        logging.error(f"Network error while retrieving JSON from {url}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while retrieving JSON from {url}: {e}")
    return None


def get_course(json_data):
    try:
        course = {}
        chapters = json_data['props']['pageProps']['course']['chapters']
        for chapter in chapters:
            chapter_title = sanitize_filename(chapter['title'])
            course[chapter_title] = {}
            for video in chapter['contents']:
                if video['__typename'] == 'Video':
                    video_title = sanitize_filename(video['title'])
                    course[chapter_title][video_title] = video['id']
        return course
    except KeyError as e:
        logging.error(f"KeyError while parsing course JSON: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while parsing course data: {e}")
    return None


def sanitize_filename(filename):
    """Sanitize filenames by replacing special characters."""
    return "".join([c if c.isalnum() or c in (' ', '.', '_') else '_' for c in filename])


def get_token(username, password, session):
    """Authenticate and get token from the submeta.io API."""
    url = "https://b.submeta.io/api"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0",
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Origin": "https://submeta.io",
        "Referer": "https://submeta.io/",
        "Accept-Encoding": "gzip, deflate, br"
    }

    payload = {
        "operationName": "Login",
        "variables": {
            "input": {
                "username": username,
                "password": password
            }
        },
        "query": """
        mutation Login($input: LoginInput!) {
          login(input: $input) {
            token
            user {
              id
              name
              username
              email
            }
            errors {
              key
              message
            }
          }
        }
        """
    }

    try:
        with session.post(url, headers=headers, data=json.dumps(payload), timeout=REQUEST_TIMEOUT) as response:
            response.raise_for_status()
            data = response.json()
            token = data["data"]["login"].get("token")
            if token:
                logging.info("Login successful!")
                print("Login successful! Token obtained")
                return token
            logging.error(f"Login failed. Response: {data}")
    except (HTTPError, ConnectionError, Timeout) as e:
        logging.error(f"Network error during login: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during login: {e}")
    return None


def downloader(course, args, token, session):
    url_prefix = "https://customer-3j2pofw9vdbl9sfy.cloudflarestream.com/"
    url_suffix = "/manifest/video.mpd"
    url_api = "https://b.submeta.io/api"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }

    download_path = args[2] if len(args) == 3 else 'submeta-downloads'
    os.makedirs(download_path, exist_ok=True)

    for chapter in tqdm(course, desc="Chapters"):
        chapter_index = list(course).index(chapter) + 1
        chapter_title = sanitize_filename(chapter)
        chapter_path = os.path.join(download_path, f'{chapter_index}. {chapter_title}')
        os.makedirs(chapter_path, exist_ok=True)

        for video in tqdm(course[chapter], desc=f"Downloading videos from {chapter}", leave=False):
            video_index = list(course[chapter]).index(video) + 1
            video_title = sanitize_filename(video)
            filename = f'{video_index}. {video_title}'
            filepath = os.path.join(chapter_path, f'{filename}.%(ext)s')

            payload = {
                "operationName": "GetVideoForWatchAuth",
                "variables": {"id": course[chapter][video], "isStandalone": False},
                "query": """query GetVideoForWatchAuth($id: ID!, $isStandalone: Boolean) {
                               result: getVideoForWatchAuth(id: $id, isStandalone: $isStandalone) {
                                 video {
                                   ...VideoForWatchAuthData
                                   __typename
                                 }
                                 isAuthorized
                                 errors {
                                   ...ErrorsFields
                                   __typename
                                 }
                                 __typename
                               }
                            }
                            fragment VideoForWatchAuthData on Video {
                              id
                              videoRef
                              token
                              __typename
                            }
                            fragment ErrorsFields on ErrorOutput {
                              key
                              message
                              __typename
                            }
                            """
            }

            try:
                with session.post(url_api, json=payload, headers=headers, timeout=REQUEST_TIMEOUT) as response:
                    response.raise_for_status()
                    data = response.json()
                    video_token = data['data']['result']['video']['token']
                    download_url = f"{url_prefix}{video_token}{url_suffix}"

                    ydl_opts = {
                        'extract_flat': 'discard_in_playlist',
                        'fragment_retries': 10,
                        'http_headers': {'Referer': 'https://submeta.io'},
                        'external_downloader': 'aria2c',
                        'ignoreerrors': True,
                        'outtmpl': filepath,
                        'retries': 10,
                        'quiet': True
                    }

                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        ydl.download([download_url])
                    logging.info(f"Successfully downloaded: {filename}")
            except (HTTPError, ConnectionError, Timeout) as e:
                logging.error(f"Network error while downloading {filename}: {e}")
            except Exception as e:
                logging.error(f"Failed to download {filename}: {e}")


def main(args):
    if len(args) not in [2, 3]:
        print('usage: submeta-dl.py <URL> <download path(optional)>')
        return -1

    session = create_session()

    json_data = get_json(args[1], session)
    if not json_data:
        print("Failed to retrieve JSON data. Check the URL or logs for more information.")
        return -1

    course = get_course(json_data)
    if not course:
        print("Failed to parse course data. Check logs for more information.")
        return -1

    username = input("Enter username: ")
    password = getpass.getpass("Enter password: ")  # Use getpass to hide password input
    token = get_token(username, password, session)
    if not token:
        print("Failed to login. Check credentials or logs for more information.")
        return -1

    downloader(course, args, token, session)
    print("Download complete!")


if __name__ == "__main__":
    main(sys.argv)
