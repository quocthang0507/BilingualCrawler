import requests
import os
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlsplit, urlparse
from termcolor import cprint

MAX_URLS = 10000  # Constants should be uppercase
urls_origin = ['https://vietanhsongngu.com/']


def print_red(x: str): return cprint(x, 'red')


def print_blue(x): return cprint(x, 'blue')


def is_valid_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False


def get_host_name(url: str) -> str:
    return "{0.scheme}://{0.netloc}/".format(urlsplit(url))


def get_beautifulsoup_from_url(url: str) -> BeautifulSoup | None:
    if not is_valid_url(url):
        return None

    session = requests.Session()
    try:
        with session.get(url, timeout=60) as response:
            response.raise_for_status()  # Raises HTTPError for bad responses
            return BeautifulSoup(response.text, 'html.parser')
    except requests.RequestException as e:
        print_red(f"Request error: {str(e)}")
        return None
    finally:
        session.close()


def get_urls_from_url(url: str, ext: str = '.htm') -> list[str]:
    urls = set()  # Using a set for faster duplicate checking
    start_time = time.time()

    host_name = get_host_name(url)
    soup = get_beautifulsoup_from_url(url)

    if soup is None:
        end_time = time.time()
        print_red(
            f'Đã có lỗi khi truy cập địa chỉ {url} này, trong {round(end_time - start_time, 2)} giây')
        return []

    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.endswith(ext) and host_name == get_host_name(href):
            urls.add(href)

    end_time = time.time()
    print_blue(
        f'Đã lấy được {len(urls)} URL, trong {round(end_time - start_time, 2)} giây')
    return list(urls)


def crawl_site():
    visited_urls = set()
    data_folder = os.path.join(os.path.dirname(__file__), 'data')
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        
    output_file = os.path.join(data_folder, 'sitemap.txt')
    
    try:
        with open(output_file, 'w', encoding='utf8') as f:
            # Process initial URL
            if not urls_origin:
                print_red("No origin URL provided")
                return
                
            urls_to_process = set(get_urls_from_url(urls_origin[0]))
            
            while urls_to_process:
                current_url = urls_to_process.pop()
                
                if current_url in visited_urls:
                    continue
                    
                if not is_valid_url(current_url):
                    continue
                    
                visited_urls.add(current_url)
                f.write(current_url + '\n')
                
                try:
                    new_urls = get_urls_from_url(current_url)
                    urls_to_process.update(new_urls)
                except Exception as e:
                    print_red(f"Error crawling {current_url}: {e}")
                    
        print_blue(f'Total URLs: {len(visited_urls)}')
                    
    except IOError as e:
        print_red(f"Error writing to file: {e}")

if __name__ == '__main__':
    crawl_site()