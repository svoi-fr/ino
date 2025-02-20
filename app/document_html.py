from typing import Optional, List, Set
from bs4 import BeautifulSoup
import html2text
import re
from trafilatura import extract
from datetime import datetime
import langdetect
from urllib.parse import urljoin, urlparse
from document import DocumentData, clean_text, text_to_markdown

def parse_html_document(html: str, url: str) -> DocumentData:
    doc = DocumentData(url=url)
    
    # Extract text content
    doc.text = extract(html, url, output_format='txt')
    if not doc.text:
        raise ValueError("Failed to extract text content")
    
    # Extract markdown with metadata
    content = extract(
        html,
        url,
        output_format='markdown',
        include_links=True,
        include_formatting=True,
        with_metadata=True
    )
    
    # Split metadata from content if present
    if content and '---' in content:
        _, meta_section, content = content.split('---', 2)
        parse_metadata(doc, meta_section)
        doc.md_content = content.strip()
    else:
        doc.md_content = content
    
    # Generate raw markdown
    doc.md_raw = html_to_raw_markdown(html)
    
    # Extract all links
    doc.links = extract_links(html, url)
    
    doc.language = langdetect.detect(doc.text[:1000])
    return doc

def extract_links(html: str, base_url: str) -> Set[str]:
    """
    Extract all valid URLs from the HTML content.
    
    Args:
        html: The HTML content to parse
        base_url: The base URL of the page for resolving relative URLs
    
    Returns:
        Set of unique, absolute URLs found in the HTML
    """
    try:
        soup = BeautifulSoup(html, 'html.parser')
        links = set()
        
        # Process all anchor tags
        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            link_text = a.get_text().lower()
            
            if not should_remove_link(href, link_text):
                # Convert relative URLs to absolute
                absolute_url = make_absolute_url(href, base_url)
                if absolute_url:
                    links.add(absolute_url)
        
        # Also process other tags that might contain URLs
        for tag in soup.find_all(['iframe', 'frame', 'link'], src=True):
            src = tag.get('src', '').strip()
            if src:
                absolute_url = make_absolute_url(src, base_url)
                if absolute_url:
                    links.add(absolute_url)
                    
        return links
        
    except Exception as e:
        print(f"Error extracting links: {str(e)}")
        return set()

def make_absolute_url(url: str, base_url: str) -> Optional[str]:
    """
    Convert a URL to absolute form and validate it.
    
    Args:
        url: The URL to process (can be relative or absolute)
        base_url: The base URL to use for relative URLs
    
    Returns:
        The absolute URL if valid, None otherwise
    """
    try:
        # Handle fragment-only URLs
        if url.startswith('#'):
            return None
            
        # Convert to absolute URL
        absolute = urljoin(base_url, url)
        
        # Parse and validate
        parsed = urlparse(absolute)
        if not parsed.scheme or not parsed.netloc:
            return None
            
        # Ensure proper scheme
        if parsed.scheme not in ('http', 'https'):
            return None
            
        return absolute
        
    except Exception:
        return None

def parse_metadata(doc: DocumentData, meta_section: str):
    print(meta_section)
    current_key = None
    
    for line in meta_section.strip().split('\n'):
        line = line.strip()
        if not line:
            continue
            
        # Check if this is a new key
        if ':' in line and not line.startswith(' '):
            key, value = line.split(':', 1)
            current_key = key.strip().lower()
            value = value.strip()
            if current_key in ('title', 'author', 'description', 'sitename', 'hostname'):
                setattr(doc, current_key, value)
            if current_key in ('date',):
                setattr(doc, current_key, datetime.strptime(value, '%Y-%m-%d'))

def should_remove_link(href: str, link_text: str) -> bool:
    """
    Determine if a link should be removed from processing.
    Only filters out javascript links, malformed URLs, and cookie-related content.
    Allows relative links to be processed.
    """
    return (
        href.startswith('javascript:') or
        href.startswith('<') or
        href.endswith('>') or
        any(term in href.lower() or term in link_text 
            for term in ['cookies', 'javascript'])
    )

def html_to_raw_markdown(html: str) -> Optional[str]:
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script, style elements and images
        for element in soup(["script", "style", "img"]):
            element.decompose()
            
        # Configure html2text
        h = html2text.HTML2Text()
        h.ignore_links = False
        h.ignore_images = True
        h.ignore_emphasis = False
        h.body_width = 0  # No wrapping
        h.protect_links = True
        h.unicode_snob = True
        h.skip_internal_links = True
        h.inline_links = True
        
        # Convert to markdown
        markdown = h.handle(str(soup))
        
        # Just clean up excessive newlines
        markdown = re.sub(r'\n{3,}', '\n\n', markdown)
        
        return markdown.strip()
        
    except Exception as e:
        print(f"Error converting to raw markdown: {str(e)}")
        return None

def clean_markdown(markdown: str) -> str:
    # Remove excessive newlines
    markdown = re.sub(r'\n{3,}', '\n\n', markdown)
    
    # Remove excessive spaces
    markdown = re.sub(r' {3,}', ' ', markdown)
    
    # Remove lines with just spaces
    markdown = re.sub(r'\n +\n', '\n\n', markdown)
    
    return markdown.strip()