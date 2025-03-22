import requests
import trafilatura
from lxml import html, etree
import urllib.parse
from urllib.parse import urlparse
from bs4 import BeautifulSoup, Tag, NavigableString
import re
import difflib
from inscriptis import get_text
from inscriptis.model.config import ParserConfig


# List of URLs to test
urls = [
    'https://qx1.org/lieu/ecm-boutik/',
    'https://qx1.org/lieu/armee-du-salut-paroisse-de-saint-mauront/',
    'https://qx1.org/lieu/rusf-reseau-universite-sans-frontieres/',
]

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Expanded list of tags to remove
def clean_html(html_content):
    """Clean HTML by removing specified tags while preserving important content."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Enhanced list of tags to remove
    remove_tags = [
        "script", "img", "style", "svg", "path", "option", "button", "iframe", 
        "link", "input", "select", "form", "fieldset", "label", "noscript",
        "meta", "head", "canvas", "audio", "video", "source", "track", "map",
        "area", "object", "param", "embed", "picture", "nav", "template"
    ]
    
    # Additional attributes to remove that might contain noise
    remove_attrs = ['onclick', 'onload', 'onmouseover', 'data-src', 'data-lazy', 
                    'style', 'class', 'id', 'aria-*', 'data-*']
    
    # Remove tags
    pattern = re.compile(r'\b(' + '|'.join(remove_tags) + r')\b')
    for tag in soup.find_all(pattern):
        tag.decompose()
    
    class_pattens = ['cookie', 'banner', 'popup', 'modal', 'ad', 'mapbox']

    pattern = re.compile(r'\b(' + '|'.join(class_pattens) + r')\b')
    for tag in soup.find_all(class_=pattern):
        tag.decompose()
    
    # Remove specific attributes from remaining tags
    for tag in soup.find_all(True):  # Find all remaining tags
        for attr in list(tag.attrs):
            # Handle wildcards like data-* and aria-*
            if any(attr == wild.replace('*', '') or 
                  (wild.endswith('*') and attr.startswith(wild[:-1])) 
                  for wild in remove_attrs):
                del tag.attrs[attr]
    
    # Remove empty tags (no text and no children with text)
    for tag in soup.find_all():
        if not tag.get_text(strip=True) and tag.name not in ['br', 'hr']:
            tag.decompose()
    
    return soup.prettify()

def extract_with_trafilatura(html_content, url):
    """Extract content using trafilatura."""
    downloaded = trafilatura.extract(
        html_content,
        url=url,
        include_comments=False,
        include_tables=True,
        include_images=False,
        include_links=True,
        output_format='xml'
    )
    return downloaded

def preserve_important_content(original_html, trafilatura_output):
    """
    Compare original (pre-cleaned) HTML with trafilatura output to identify
    and preserve important content that might have been dropped.
    """
    # Parse both documents
    orig_soup = BeautifulSoup(original_html, 'html.parser')
    traf_soup = BeautifulSoup(trafilatura_output, 'xml')
    traf_text = traf_soup.get_text()
    
    # Find important content in original that might be missing in trafilatura output
    important_elements = []
    
    # Helper function to get substantial parent container
    def get_substantial_parent(element, max_levels=2):
        """
        Get a meaningful parent container that likely contains related content.
        Traverses up the DOM tree looking for container elements with multiple children
        or specific container classes.
        """
        # List of class names that typically indicate content containers
        container_classes = ['container', 'section', 'box', 'card', 'panel', 'widget', 
                           'article', 'content', 'info', 'details', 'group', 'aside']
        
        current = element
        for _ in range(max_levels):
            if not current.parent or current.parent.name == 'body':
                return current
                
            parent = current.parent
            
            # Check if this parent has a relevant container class
            has_container_class = False
            if 'class' in parent.attrs:
                parent_classes = parent['class'] if isinstance(parent['class'], list) else [parent['class']]
                has_container_class = any(container in ' '.join(parent_classes).lower() 
                                         for container in container_classes)
            
            # Check if parent contains multiple meaningful children
            children_count = len([c for c in parent.children 
                                if isinstance(c, Tag) and c.name not in ['br', 'hr']])
            
            # If this parent has multiple children or a container class, consider it substantial
            if children_count >= 2 or has_container_class:
                current = parent
                # If this parent has a header/title element as one of its children,
                # it's likely a complete content block
                if any(c.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6'] 
                      for c in parent.find_all(recursive=False)):
                    return current
            else:
                # Keep going up if this parent doesn't seem substantial enough
                current = parent
                
        return current  # Return the highest level we reached
    
    # Helper function to check if content is already in trafilatura output
    def is_content_in_trafilatura(content):
        """Check if content or similar content exists in trafilatura output."""
        if not content:
            return True
            
        # Direct text match
        if content in traf_text:
            return True
            
        # Try fuzzy matching for similar content
        # Break down content into sentences and check each
        sentences = re.split(r'[.!?]\s+', content)
        for sentence in sentences:
            if len(sentence) > 15 and sentence in traf_text:
                return True
                
        return False
    
    # 1. Look for map links (Google Maps, OpenStreetMap, etc.)
    map_links = orig_soup.find_all('a', href=lambda h: h and any(term in h.lower() for term in 
                                ['maps.google', 'google.com/maps', 'openstreetmap', 
                                 'goo.gl/maps', 'maps.app', '/maps/']))
    
    for link in map_links:
        if not is_content_in_trafilatura(link.get('href')):
            # Get a substantial parent that likely contains related address info
            container = get_substantial_parent(link)
            
            important_elements.append({
                'type': 'map',
                'content': str(container),
                'text': container.get_text(strip=True),
                'url': link.get('href')
            })
    
    # 2. Look for address information
    # First check for explicit address elements
    address_elements = orig_soup.find_all(['address'])
    for elem in address_elements:
        text_content = elem.get_text(strip=True)
        if text_content and not is_content_in_trafilatura(text_content):
            container = get_substantial_parent(elem)
            important_elements.append({
                'type': 'address',
                'content': str(container),
                'text': container.get_text(strip=True)
            })
    
    # Then look for elements with address-related classes or IDs
    address_containers = orig_soup.find_all(['div', 'section', 'article', 'aside'], 
                                           class_=lambda c: c and any(term in (c.lower() if c else '') 
                                                                    for term in ['address', 'contact', 'location', 'adresse']))
    
    for container in address_containers:
        text_content = container.get_text(strip=True)
        if text_content and not is_content_in_trafilatura(text_content):
            important_elements.append({
                'type': 'address',
                'content': str(container),
                'text': text_content
            })
            
    # Look for elements with address in the ID
    address_by_id = orig_soup.find_all(id=lambda i: i and any(term in (i.lower() if i else '') 
                                                           for term in ['address', 'contact', 'location', 'adresse']))
    for elem in address_by_id:
        text_content = elem.get_text(strip=True)
        if text_content and not is_content_in_trafilatura(text_content):
            container = get_substantial_parent(elem)
            important_elements.append({
                'type': 'address',
                'content': str(container),
                'text': container.get_text(strip=True)
            })
            
    # 3. Look for headers that might indicate address sections (like "Our Location", "Find Us", etc.)
    location_headers = orig_soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'], 
                                        string=lambda s: s and any(term in s.lower() 
                                                                for term in ['address', 'location', 'contact', 
                                                                           'find us', 'where to find', 'adresse', 
                                                                           'où nous trouver']))
    for header in location_headers:
        # Get the container that includes this header and its associated content
        container = get_substantial_parent(header)
        text_content = container.get_text(strip=True)
        
        if text_content and not is_content_in_trafilatura(text_content):
            important_elements.append({
                'type': 'address',
                'content': str(container),
                'text': text_content
            })
    
    # 4. Look for tel: links
    phone_links = orig_soup.find_all('a', href=lambda h: h and h.startswith('tel:'))
    for link in phone_links:
        if not is_content_in_trafilatura(link.get('href')):
            container = get_substantial_parent(link)
            important_elements.append({
                'type': 'phone',
                'content': str(container),
                'text': container.get_text(strip=True),
                'url': link.get('href')
            })
    
    # 5. Look for mailto: links
    email_links = orig_soup.find_all('a', href=lambda h: h and h.startswith('mailto:'))
    for link in email_links:
        if not is_content_in_trafilatura(link.get('href')):
            container = get_substantial_parent(link)
            important_elements.append({
                'type': 'email',
                'content': str(container),
                'text': container.get_text(strip=True),
                'url': link.get('href')
            })
    
    # 6. Look for phone patterns in text even without tel: links
    phone_pattern = re.compile(r'(\+\d{1,3}[ -]?)?\(?\d{3}\)?[ -]?\d{3}[ -]?\d{4}')
    for tag in orig_soup.find_all(string=phone_pattern):
        if tag and not is_content_in_trafilatura(tag.strip()):
            container = get_substantial_parent(tag.parent)
            important_elements.append({
                'type': 'phone',
                'content': str(container),
                'text': container.get_text(strip=True)
            })
    
    # 7. Look for email patterns in text even without mailto: links
    email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    for tag in orig_soup.find_all(string=email_pattern):
        if tag and not is_content_in_trafilatura(tag.strip()):
            container = get_substantial_parent(tag.parent)
            important_elements.append({
                'type': 'email',
                'content': str(container),
                'text': container.get_text(strip=True)
            })
    
    # 8. Look for operating hours
    # First, try to find containers explicitly about hours
    hours_elements = orig_soup.find_all(['div', 'section', 'article', 'aside'], 
                                      class_=lambda c: c and any(term in (c.lower() if c else '') 
                                                               for term in ['hours', 'schedule', 'opening', 
                                                                          'horaire', 'opening-hours', 
                                                                          'business-hours']))
    for elem in hours_elements:
        text_content = elem.get_text(strip=True)
        if text_content and not is_content_in_trafilatura(text_content):
            important_elements.append({
                'type': 'hours',
                'content': str(elem),
                'text': text_content
            })
    
    # Find headings about hours
    hours_headers = orig_soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'], 
                                     string=lambda s: s and any(term in s.lower() 
                                                             for term in ['hours', 'schedule', 'opening', 
                                                                        'horaire', 'heures d\'ouverture', 
                                                                        'business hours']))
    for header in hours_headers:
        container = get_substantial_parent(header)
        text_content = container.get_text(strip=True)
        
        if text_content and not is_content_in_trafilatura(text_content):
            important_elements.append({
                'type': 'hours',
                'content': str(container),
                'text': text_content
            })
    
    # 9. Look for elements with time patterns (e.g., 9:00-17:00, 9h-17h)
    time_pattern = re.compile(r'(\d{1,2}[h:]\d{2})\s*-\s*(\d{1,2}[h:]\d{2})')
    time_elements = orig_soup.find_all(string=time_pattern)
    for elem in time_elements:
        if elem and not is_content_in_trafilatura(elem.strip()):
            # Check if this is likely part of operating hours (look for days of week nearby)
            days_pattern = re.compile(r'(lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche|monday|tuesday|wednesday|thursday|friday|saturday|sunday)', re.IGNORECASE)
            container = get_substantial_parent(elem.parent)
            context = container.get_text()
            
            if days_pattern.search(context):
                important_elements.append({
                    'type': 'hours',
                    'content': str(container),
                    'text': container.get_text(strip=True)
                })
    
    # Remove duplicates
    seen_texts = set()
    unique_elements = []
    for item in important_elements:
        content = item.get('content', '')
        if content and content not in seen_texts:
            seen_texts.add(content)
            unique_elements.append(item)
    
    return unique_elements

def merge_content(trafilatura_output, preserved_content):
    """Merge the trafilatura output with preserved content."""
    if not preserved_content:
        return trafilatura_output
    
    soup = BeautifulSoup(trafilatura_output, 'xml')
    
    # Create a new section for preserved content
    preserved_section = soup.new_tag('div')
    preserved_section['class'] = 'preserved-content'
    
    # Group preserved content by type
    grouped_content = {}
    for item in preserved_content:
        item_type = item['type']
        if item_type not in grouped_content:
            grouped_content[item_type] = []
        grouped_content[item_type].append(item)
    
    # For each content type, create a section
    for content_type, items in grouped_content.items():
        # Create section for this content type
        type_section = soup.new_tag('div')
        type_section['class'] = f'preserved-{content_type}'
        
        # Add heading for this section
        heading = soup.new_tag('h3')
        heading.string = {
            'address': 'Location Information',
            'map': 'Map Links',
            'phone': 'Phone Contacts',
            'email': 'Email Contacts',
            'hours': 'Opening Hours',
        }.get(content_type, f'Additional {content_type.capitalize()} Information')
        
        type_section.append(heading)
        
        # Add all items of this type
        for item in items:
            item_container = soup.new_tag('div')
            item_container['class'] = f'{content_type}-item'
            
            # If there's a URL (for maps, tel:, mailto:), add it as a link
            if 'url' in item:
                url_tag = soup.new_tag('a')
                url_tag['href'] = item['url']
                url_display = {
                    'map': 'View on Map',
                    'phone': item['url'].replace('tel:', ''),
                    'email': item['url'].replace('mailto:', '')
                }.get(content_type, item['url'])
                
                url_tag.string = url_display
                item_container.append(url_tag)
                item_container.append(soup.new_tag('br'))
            
            # Parse the HTML content of the item
            content_html = BeautifulSoup(item['content'], 'html.parser')
            
            # Recursively copy the entire structure
            def copy_element(src, dest):
                # Skip script, style and empty elements
                if src.name in ['script', 'style'] or (src.name and not src.get_text(strip=True)):
                    return
                
                # If it's a string node (NavigableString)
                if isinstance(src, NavigableString):
                    if str(src).strip():  # Only copy non-whitespace strings
                        dest.append(str(src))
                    return
                
                # Create a new tag with the same name
                if src.name:
                    new_tag = soup.new_tag(src.name)
                    
                    # Copy attributes
                    for attr, value in src.attrs.items():
                        new_tag[attr] = value
                    
                    # Add this tag to the destination
                    dest.append(new_tag)
                    
                    # Copy all child nodes recursively
                    for child in src.children:
                        copy_element(child, new_tag)
            
            # Copy the content into our item container
            for child in content_html.children:
                copy_element(child, item_container)
            
            type_section.append(item_container)
        
        preserved_section.append(type_section)
    
    # Find the right place to insert the preserved content
    body = soup.find('body') or soup.find('text') or soup.find('doc')
    if body:
        body.append(preserved_section)
    else:
        soup.append(preserved_section)
    
    return str(soup)

def main():
    """Main function to process all URLs."""
    for url in urls:
        print(f"\n{'='*80}")
        print(f"Processing {url}")
        print(f"{'='*80}")
        
        # 1. Get original HTML
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise error for bad status codes
            original_html = response.text
        except Exception as e:
            print(f"Error fetching URL: {e}")
            continue
        
        # 2. Pre-clean HTML
        cleaned_html = clean_html(original_html)
        print("\nPre-cleaned HTML (sample):")
        print(cleaned_html[:500] + "..." if len(cleaned_html) > 500 else cleaned_html)
        
        # 3. Process with trafilatura
        trafilatura_output = extract_with_trafilatura(cleaned_html, url)
        print("\nTrafilatura output (sample):")
        if trafilatura_output:
            print(trafilatura_output[:500] + "..." if len(trafilatura_output) > 500 else trafilatura_output)
        else:
            print("No content extracted by trafilatura")
            # Try with original HTML as fallback
            print("Trying with original HTML as fallback...")
            trafilatura_output = extract_with_trafilatura(original_html, url)
            if not trafilatura_output:
                print("Still no content extracted. Skipping URL.")
                continue
        
        # 4. Find and preserve important content that trafilatura might have dropped
        preserved_content = preserve_important_content(cleaned_html, trafilatura_output)

        
        # Print detailed info about preserved content
        print(f"\nFound {len(preserved_content)} elements that trafilatura might have dropped:")
        for i, item in enumerate(preserved_content, 1):
            print(f"  {i}. {item['type'].upper()}: {item.get('text', '')[:80]}..." if len(item.get('text', '')) > 80 else item.get('text', ''))
            if 'url' in item:
                print(f"     URL: {item['url']}")
        
        # 5. Merge trafilatura output with preserved content
        final_output = merge_content(trafilatura_output, preserved_content)
        print("\nFinal output (sample):")
        print(final_output)
        # conf = ParserConfig(display_images=False, display_links=True, display_anchors=False)
        # text = get_text(final_output)
        # print(text)
        # print(final_output[:50000] + "..." if len(final_output) > 500 else final_output)
        
        # Save the results to files
        domain = urlparse(url).netloc
        path = urlparse(url).path.strip('/').replace('/', '_')
        filename_base = f"{domain}_{path}" if path else domain
        
        try:
            # Create a 'results' directory if it doesn't exist
            import os
            if not os.path.exists('results'):
                os.makedirs('results')
                
            # Save all processing stages
            with open(f"results/{filename_base}_original.html", "w", encoding='utf-8') as f:
                f.write(original_html)
            with open(f"results/{filename_base}_cleaned.html", "w", encoding='utf-8') as f:
                f.write(cleaned_html)
            with open(f"results/{filename_base}_trafilatura.xml", "w", encoding='utf-8') as f:
                f.write(trafilatura_output)
            with open(f"results/{filename_base}_final.xml", "w", encoding='utf-8') as f:
                f.write(final_output)
                
            print(f"\nResults saved to 'results/{filename_base}_*.html/xml' files")
            
        except Exception as e:
            print(f"Error saving results: {e}")
            
        print(f"\nCompleted processing {url}")
        print(f"{'='*80}")

if __name__ == "__main__":
    main()