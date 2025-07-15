import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlsplit, urlparse
import os
from typing import Set, Optional
import logging
from collections import deque
import async_timeout

# Constants
MAX_DEPTH = 3  # Maximum recursion depth
MAX_URLS_PER_DEPTH = 50  # Maximum URLs to process at each depth
MAX_CONCURRENT_REQUESTS = 20
REQUEST_TIMEOUT = 30
VALID_EXTENSIONS = {'.htm', '.html', ''}


class RecursiveWebCrawler:
    def __init__(self, start_url: str, output_file: str):
        self.start_url = start_url
        self.output_file = output_file
        self.visited_urls: Set[str] = set()
        self.host_name = self.get_host_name(start_url)
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def get_host_name(url: str) -> str:
        return "{0.scheme}://{0.netloc}".format(urlsplit(url))

    @staticmethod
    def is_valid_url(url: str) -> bool:
        try:
            result = urlparse(url)
            return bool(result.scheme and result.netloc)
        except:
            return False

    def should_process_url(self, url: str) -> bool:
        if not url:
            return False

        # Check if URL belongs to the same domain
        if not url.startswith(self.host_name):
            return False

        # Check file extension
        _, ext = os.path.splitext(urlparse(url).path)
        return ext.lower() in VALID_EXTENSIONS

    async def fetch_url(self, url: str, session: aiohttp.ClientSession) -> Optional[str]:
        try:
            async with self.semaphore, async_timeout.timeout(REQUEST_TIMEOUT):
                async with session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    self.logger.warning(f"Failed to fetch {url}, status: {response.status}")
                    return None
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {str(e)}")
            return None

    async def extract_urls(self, html_content: str, base_url: str) -> Set[str]:
        urls = set()
        soup = BeautifulSoup(html_content, 'html.parser')

        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(base_url, href)

            if self.should_process_url(full_url) and full_url not in self.visited_urls:
                urls.add(full_url)

        return urls

    async def process_url(self, url: str, depth: int, session: aiohttp.ClientSession) -> Set[str]:
        if depth > MAX_DEPTH or url in self.visited_urls:
            return set()

        self.visited_urls.add(url)
        self.logger.info(f"Processing {url} at depth {depth}")

        html_content = await self.fetch_url(url, session)
        if not html_content:
            return set()

        return await self.extract_urls(html_content, url)

    async def crawl_recursive(self):
        async with aiohttp.ClientSession() as session:
            # Use deque for BFS approach
            url_queue = deque([(self.start_url, 1)])  # (url, depth)

            while url_queue:
                current_depth_urls = []

                # Get all URLs at current depth
                while url_queue and len(current_depth_urls) < MAX_URLS_PER_DEPTH:
                    url, depth = url_queue.popleft()
                    if url not in self.visited_urls:
                        current_depth_urls.append((url, depth))

                if not current_depth_urls:
                    continue

                # Process current depth URLs concurrently
                tasks = [self.process_url(url, depth, session) for url, depth in current_depth_urls]
                results = await asyncio.gather(*tasks)

                # Add new URLs to queue for next depth
                for new_urls in results:
                    for new_url in new_urls:
                        if new_url not in self.visited_urls:
                            url_queue.append((new_url, depth + 1))

                # Save progress after each depth
                self.save_progress()

                self.logger.info(f"Depth {depth}: Processed {len(current_depth_urls)} URLs, "
                                 f"Total URLs: {len(self.visited_urls)}")

    def save_progress(self):
        try:
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            with open(self.output_file, 'w', encoding='utf8') as f:
                for url in sorted(self.visited_urls):
                    f.write(f"{url}\n")
        except IOError as e:
            self.logger.error(f"Error saving progress: {str(e)}")


async def main():
    start_url = "https://vietanhsongngu.com"  # Replace with your domain
    output_file = os.path.join("data", "sitemap.txt")

    crawler = RecursiveWebCrawler(start_url, output_file)
    await crawler.crawl_recursive()


if __name__ == '__main__':
    asyncio.run(main())
