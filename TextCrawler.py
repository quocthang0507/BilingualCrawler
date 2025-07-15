import os
import requests
from bs4 import BeautifulSoup
from langdetect import detect
from tqdm import tqdm
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

# Constants
TIMEOUT = 30
MAX_WORKERS = 5
LANG_VI = 'vi'
LANG_EN = 'en'


@lru_cache(maxsize=1000)
def detect_language(text: str) -> str:
    """Cache language detection results to avoid repeated processing"""
    try:
        return detect(text)
    except:
        return ''


def create_session() -> requests.Session:
    """Create a reusable session with common settings"""
    session = requests.Session()
    session.timeout = TIMEOUT
    return session


def fetch_page(url: str, session: requests.Session) -> Tuple[str, BeautifulSoup]:
    """Fetch and parse a webpage"""
    response = session.get(url)
    response.raise_for_status()
    return response.text, BeautifulSoup(response.text, 'html.parser')


def clean_text(text: str) -> str:
    """Remove special whitespace characters"""
    return text.translate(str.maketrans('', '', '\xa0\u200b'))


def process_paragraph_pair(p1: BeautifulSoup, p2: BeautifulSoup) -> Tuple[str, str]:
    """Process a pair of paragraphs and return a Vietnamese-English pair if valid"""
    text1 = clean_text(p1.get_text(strip=True))
    text2 = clean_text(p2.get_text(strip=True))

    if not (text1 and text2):
        return None

    lang1 = detect_language(text1)
    lang2 = detect_language(text2)

    if lang1 == LANG_VI and lang2 == LANG_EN:
        return (text1, text2)
    elif lang1 == LANG_EN and lang2 == LANG_VI:
        return (text2, text1)
    return None


def get_bilingual_text(url: str, session: requests.Session) -> List[Tuple[str, str]]:
    """Extract bilingual text pairs from a webpage"""
    try:
        _, soup = fetch_page(url, session)
        content_div = soup.find("div", class_="entry-content")

        if not content_div:
            return []

        paragraphs = content_div.find_all("p")
        bilingual_pairs = []

        for i in range(len(paragraphs) - 1):
            result = process_paragraph_pair(paragraphs[i], paragraphs[i + 1])
            if result:
                bilingual_pairs.append(result)
                i += 1  # Skip next paragraph as it's part of current pair

        return bilingual_pairs

    except requests.RequestException:
        return []


def process_url(url: str, session: requests.Session) -> List[Tuple[str, str]]:
    """Process a single URL and extract bilingual pairs"""
    return get_bilingual_text(url, session)


def main():
    data_folder = os.path.join(os.getcwd(), 'data')
    urls_file = os.path.join(data_folder, 'sitemap.txt')
    output_file = os.path.join(data_folder, 'bilingual_text.txt')

    # Read URLs
    with open(urls_file, 'r', encoding='utf8') as f:
        urls = [line.strip() for line in f if line.strip()]

    # Process URLs in parallel
    session = create_session()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_url, url, session) for url in urls]

        with open(output_file, 'w', encoding='utf8') as writer:
            for future in tqdm(futures, desc="Processing URLs"):
                pairs = future.result()
                for vi, en in pairs:
                    writer.write(f"* {vi}\n+ {en}\n")


if __name__ == '__main__':
    main()
