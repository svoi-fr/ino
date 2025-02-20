from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from contextlib import contextmanager
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import regex as re
import requests
from concurrent.futures import ThreadPoolExecutor
import logging
import time
from trafilatura import extract
import hashlib
import random
from tools import simple_tool_call

def hash_string(text):
    return hashlib.md5(text.encode()).hexdigest()

@contextmanager
def create_driver():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-software-rasterizer')
    # options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    options.add_argument('--window-size=1920,1080')
    
    # Performance optimizations
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument('--disable-dev-tools')
    options.add_argument('--disable-3d-apis')
    options.add_argument('--disable-bundled-ppapi-flash')
    options.add_argument('--disable-javascript-harmony-shipping')
    options.add_argument('--disable-smooth-scrolling')
    options.add_argument('--disable-sync')
    
    # Block media and other resource types
    prefs = {
        'profile.managed_default_content_settings.images': 2,
        'profile.default_content_settings.media_stream': 2,
        'profile.default_content_settings.plugins': 2,
        'profile.default_content_settings.popups': 2,
        'profile.default_content_settings.notifications': 2
    }
    options.add_experimental_option('prefs', prefs)
    
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(20)  # Shorter timeout
    try:
        yield driver
    finally:
        driver.quit()

def is_valid_url(url, domain, url_pattern_skip, url_pattern_include):
    """Validate URL against patterns and domain"""
    if not url or not url.startswith('http'):
        return False
    
    if urlparse(url).netloc != domain:
        return False
        
    if url_pattern_skip and any(re.search(pattern, url) for pattern in url_pattern_skip):
        return False
        
    if url_pattern_include and not any(re.search(pattern, url) for pattern in url_pattern_include):
        return False
        
    return True

def check_url(url):
    """Quick check if URL is accessible"""
    try:
        res = requests.head(url, timeout=5)
        return res.ok, res.headers.get('content-type', '')
    except:
        return False, None

def crawl(starting_page, max_pages=100, callback=None, callback_pdf=None, queue=None, 
          url_pattern_skip=None, url_pattern_include=None, max_workers=5):
    
    print(f"Starting crawl from {starting_page}")
    # Initialize patterns
    if isinstance(url_pattern_skip, str):
        url_pattern_skip = [url_pattern_skip]
    if isinstance(url_pattern_include, str):
        url_pattern_include = [url_pattern_include]
    
    domain = urlparse(starting_page).netloc
    base = f"{urlparse(starting_page).scheme}://{domain}"
    queue = queue or set([starting_page])
    processed = set()
    skip_hash = set()
    parsed = set()
    page_count = 0
    
    with create_driver() as driver:
        while queue and len(parsed) <= max_pages:
            # Process multiple URLs concurrently
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                urls_to_process = list(queue)[:max_workers]
                queue.difference_update(urls_to_process)
                
                future_to_url = {
                    executor.submit(check_url, url): url 
                    for url in urls_to_process
                }
                
                for future in future_to_url:
                    url = future_to_url[future]
                    try:
                        is_ok, content_type = future.result()
                        if not is_ok:
                            print(f"Failed to fetch {url}")
                            processed.add(url)
                            continue
                            
                        if content_type.startswith('text/html') or content_type.startswith('text/plain'):
                            def update_queue():
                                html = driver.page_source
                                current_url = driver.current_url
                                processed.add(current_url)
                                # Extract new URLs
                                soup = BeautifulSoup(html, 'html.parser')
                                new_urls = set()
                                for a in soup.find_all('a', href=True):
                                    href = a['href'].strip()
                                    # print(href)
                                    # Skip JavaScript links and search related URLs
                                    if '#' in href or any(x in href.lower() for x in ['?s=', 'search?', 'recherche?']):
                                        continue
                                    if href.startswith('/') or href.startswith('./') or href.startswith('../'):
                                        href = urljoin(base, href)
                                    if urlparse(href).netloc == domain and not any(re.search(pattern, href) for pattern in url_pattern_skip):
                                        new_urls.add(href)
                                    # if is_valid_url(href, domain, url_pattern_skip, url_pattern_include):
                                    #     new_urls.add(href)
                                
                                queue.update(new_urls - processed)
                            try:
                                print(f"Processing {url}")
                                driver.get(url)
                                def page_has_content(driver):
                                    body = driver.find_element(By.TAG_NAME, "body")
                                    h1 = driver.find_elements(By.TAG_NAME, "h1")
                                    h2 = driver.find_elements(By.TAG_NAME, "h2")
                                    # print("============ CHK =============")
                                    text = body.text.strip()
                                    text_hash = hash_string(text)
                                    if text_hash in skip_hash:
                                        raise TimeoutException("Timeout hash found")
                                    return (len(h1) > 0 or 
                                            len(h2) > 0 or 
                                            len(body.text.strip()) > 500)
                                
                                WebDriverWait(driver, 10).until(page_has_content)
                                # time.sleep(10)
                                
                                html = driver.page_source
                                current_url = driver.current_url
                                
                                if callback and current_url not in processed and current_url not in parsed:
                                    callback(current_url, html)
                                    processed.add(current_url)
                                    parsed.add(current_url)

                                update_queue()
                                
                                
                            except TimeoutException:
                                print(f"Timed out waiting for page: {url}")
                                body = driver.find_element(By.TAG_NAME, "body")
                                text = body.text.strip()
                                text_hash = hash_string(text)
                                skip_hash.add(text_hash)
                                update_queue()
                                
                        elif content_type.startswith('application/pdf') and callback_pdf:
                            callback_pdf(url)
                        else:
                            print(f"Skipping {url} with content type {content_type}")
                        processed.add(url)
                        page_count += 1
                        
                    except Exception as e:
                        processed.add(url)
                        print(f"Error processing {url}: {str(e)}")
                        
    print(f"Processed {page_count} pages")


def get_sitemap_url(url: str) -> str:
    domain = urlparse(url).netloc
    common_paths = [
        f"https://{domain}/sitemap.xml",
        f"https://{domain}/api/sitemap.xml",
        f"https://{domain}/sitemap-index.xml",
        f"https://{domain}/sitemap_index.xml",
        f"https://{domain}/wp-sitemap.xml",
        f"https://{domain}/sitemaps.xml",
        f"https://{domain}/sitemap/sitemap.xml",
    ]
    for path in common_paths:
        try:
            head = requests.head(path, timeout=60)
            if head.status_code == 200:
                response = requests.get(path, timeout=60)
                # try to parse the sitemap
                soup = BeautifulSoup(response.content, "xml")
                if soup.find("sitemap"):
                    return path
        except Exception as e:
            continue
    return None

def parse_sitemap_links(sitemap_url: str, limit = None) -> set:
    try:
        response = requests.get(sitemap_url, timeout=(30, 30))
        soup = BeautifulSoup(response.content, "xml")
        entries = set()

        # Handle sitemap index
        sitemaps = soup.find_all("sitemap")

        if sitemaps:
            for sitemap in sitemaps:
                loc = sitemap.find("loc")
                if loc and loc.text:
                    sub_url = loc.text.strip()
                    sub_entries = parse_sitemap_links(sub_url)
                    entries.update(sub_entries)
                    if limit and len(entries) >= limit:
                        break

        for url_elem in soup.find_all("url"):
            loc = url_elem.find("loc")
            if loc and loc.text:
                url = loc.text.strip()
                entries.add(url)
                if limit and len(entries) >= limit:
                    break

        return entries
    except Exception as e:
        print(f"Error parsing sitemap: {str(e)}")
        return []

def get_url_sample(url, num_samples=5):
    links = set()
    sitemap_url = get_sitemap_url(url)
    print(f"Found sitemap: {sitemap_url}")
    if sitemap_url:
        parsed = parse_sitemap_links(sitemap_url, limit=max(num_samples, 500))
        print(f"Found {len(parsed)} URLs in sitemap")
        if parsed:
            links = set(random.sample(list(parsed), num_samples))
    
    def crawl_links(url, html):
        links.add(url)

    crawl(url, max_pages=max(num_samples, 50), callback=crawl_links)
    print(f"Found {len(links)} URLs in total")
    return random.sample(list(links), min(len(links), num_samples))
    
def language_filter(urls):
    if len(urls) > 100:
        urls = random.sample(list(urls), 100)
    url_sample = '\n'.join(urls)
    print(url_sample)

    language_patterns = simple_tool_call(
        url_sample,
        "Identify multilingual pattern in url's for page classification.",
        multilingual=("Return true if url's follow an evident multilingual pattern, false otherwise. Patterns list must be empty for unilingual websites.", False),
        patterns=[
            {
                "code": "2-letter language code e.g. en, fr, de, etc",
                "pattern": "Regex url pattern for language - e.g. .*/en/.*, .*en-US.*, .*?lang=en$, etc",
            }
        ]
    )
    if language_patterns["multilingual"]:
        print("Multilingual website")
        print(language_patterns)
        patterns = {p["code"].lower(): p["pattern"] for p in language_patterns["patterns"]}
        if 'en' in patterns:
            print("English pattern found: " + patterns['en'])
            return patterns['en']
    return None


def parse(url, html):
    soup = BeautifulSoup(html, 'html.parser')
    title = soup.title.string if soup.title else "No title"
    content = extract(html)
    print(f"URL: {url}")
    print(f"Title: {title}")
    print(f"Content: {content[:500]}")
    # print(soup.text)
    # print(soup.prettify())

if __name__ == "__main__":
    # url = "https://watizat.org"
    # url = "https://integreat.app"
    url = "https://italy.refugee.info"
    # url = "https://refugies.info"
    # url = "https://qx1.org"
    # url = "https://gisti.org"
    # crawl(url, max_pages=500, callback=parse)
    # sample = get_url_sample(url, num_samples=50)
    # pattern = language_filter(sample)

    print(urlparse(url).netloc)

    # crawl(url, max_pages=500, callback=parse, url_pattern_include="en-us")