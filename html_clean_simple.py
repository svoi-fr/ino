import requests
import trafilatura
from lxml import html, etree
import urllib.parse
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import re
import difflib
from inscriptis import get_text
from inscriptis.model.config import ParserConfig

def pre_parse(html_content):
    """
    Pre-process HTML by removing specified tags while preserving important content.
    
    Args:
        html_content (str): The original HTML content
        
    Returns:
        str: Cleaned HTML
    """
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

def trafilatura_parse(html_content, url):
    """
    Parse HTML content using trafilatura in both XML and HTML formats.
    
    Args:
        html_content (str): HTML content to parse
        url (str): URL of the HTML content
        
    Returns:
        tuple: (trafilatura_xml, trafilatura_html, metadata)
            - trafilatura_xml (str): Content in XML format
            - trafilatura_html (str): Content in HTML format
            - metadata (dict): Metadata extracted by trafilatura
    """
    # Extract content in XML format
    trafilatura_xml = trafilatura.extract(
        html_content,
        url=url,
        include_comments=False,
        include_tables=True,
        include_images=False,
        include_links=True,
        output_format='xml'
    )
    
    # Extract content in HTML format
    trafilatura_html = trafilatura.extract(
        html_content,
        url=url,
        include_comments=False,
        include_tables=True,
        include_images=False,
        include_links=True,
        output_format='html'
    )
    
    metadata = trafilatura.metadata.extract_metadata(
        html_content,
        default_url=url,
        extensive=False
    )
    # Convert metadata object to dictionary
    metadata_dict = {}
    for key, value in metadata.as_dict().items():
        if value:
            metadata_dict[key] = value
    
    return trafilatura_xml, trafilatura_html, metadata_dict

def extract_structured_data(original_html, trafilatura_xml):
    """
    Extract structured data from HTML content using our custom extractor.
    
    Args:
        original_html (str): Original HTML content
        trafilatura_xml (str): Content extracted by trafilatura
        
    Returns:
        dict: Structured data dictionary
    """
    # Find preserved content that trafilatura might have missed
    preserved_elements = preserve_important_content(original_html, trafilatura_xml)
    
    # Extract structured data
    contact_info = extract_location_info(preserved_elements)
    hours_info = extract_hours_info(preserved_elements)
    
    # Organize structured data
    structured_data = {
        'contact': contact_info,
        'hours': hours_info,
        'preserved_elements': preserved_elements
    }
    
    return structured_data

def preserve_important_content(original_html, trafilatura_output):
    """
    Compare original HTML with trafilatura output to identify
    and preserve important content that might have been dropped.
    """
    # Parse both documents
    orig_soup = BeautifulSoup(original_html, 'html.parser')
    traf_soup = BeautifulSoup(trafilatura_output, 'xml')
    traf_text = traf_soup.get_text()
    
    # Find important content in original that might be missing in trafilatura output
    important_elements = []
    
    # Helper function to get parent containers with context
    def get_parent_with_context(element, max_levels=3):
        """Get parent element with meaningful context for the given element."""
        current = element
        for _ in range(max_levels):
            if current.parent and current.parent.name != 'body' and len(current.parent.get_text(strip=True)) < 500:
                current = current.parent
                # If parent has multiple children and seems to be a container, use it
                if len(list(current.children)) >= 3:
                    return current
            else:
                break
        return element
    
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
    
    # 1. Look for address information
    address_elements = orig_soup.find_all(['address', 'div', 'p'], class_=lambda c: c and any(term in (c.lower() if c else '') for term in ['address', 'contact', 'location']))
    for elem in address_elements:
        text_content = elem.get_text(strip=True)
        if text_content and not is_content_in_trafilatura(text_content):
            parent = get_parent_with_context(elem)
            important_elements.append({
                'type': 'address',
                'content': str(parent),
                'text': parent.get_text(strip=True)
            })
    
    # 2. Look for map links (Google Maps, OpenStreetMap, etc.)
    map_links = orig_soup.find_all('a', href=lambda h: h and any(term in h.lower() for term in ['maps.google', 'openstreetmap', 'goo.gl/maps', 'maps.app', '/maps/']))
    for link in map_links:
        if not is_content_in_trafilatura(link.get('href')):
            parent = get_parent_with_context(link)
            important_elements.append({
                'type': 'map',
                'content': str(parent),
                'text': parent.get_text(strip=True),
                'url': link.get('href')
            })
    
    # 3. Look for tel: and mailto: links
    contact_links = orig_soup.find_all('a', href=lambda h: h and (h.startswith('tel:') or h.startswith('mailto:')))
    for link in contact_links:
        if not is_content_in_trafilatura(link.get('href')):
            parent = get_parent_with_context(link)
            link_type = 'phone' if link.get('href', '').startswith('tel:') else 'email'
            important_elements.append({
                'type': link_type,
                'content': str(parent),
                'text': parent.get_text(strip=True),
                'url': link.get('href')
            })
    
    # 4. Look for phone patterns in text even without tel: links
    phone_pattern = re.compile(r'(\+\d{1,3}[ -]?)?\(?\d{3}\)?[ -]?\d{3}[ -]?\d{4}')
    for tag in orig_soup.find_all(string=phone_pattern):
        if tag and not is_content_in_trafilatura(tag.strip()):
            parent = get_parent_with_context(tag.parent)
            important_elements.append({
                'type': 'phone',
                'content': str(parent),
                'text': parent.get_text(strip=True)
            })
    
    # 5. Look for email patterns in text even without mailto: links
    email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    for tag in orig_soup.find_all(string=email_pattern):
        if tag and not is_content_in_trafilatura(tag.strip()):
            parent = get_parent_with_context(tag.parent)
            important_elements.append({
                'type': 'email',
                'content': str(parent),
                'text': parent.get_text(strip=True)
            })
    
    # 6. Look for operating hours
    hours_elements = orig_soup.find_all(['div', 'span', 'p'], class_=lambda c: c and any(term in (c.lower() if c else '') for term in ['hours', 'schedule', 'opening', 'horaire']))
    for elem in hours_elements:
        text_content = elem.get_text(strip=True)
        if text_content and not is_content_in_trafilatura(text_content):
            parent = get_parent_with_context(elem)
            important_elements.append({
                'type': 'hours',
                'content': str(parent),
                'text': parent.get_text(strip=True)
            })
    
    # 7. Look for elements with time patterns (e.g., 9:00-17:00, 9h-17h)
    time_pattern = re.compile(r'(\d{1,2}[h:]\d{2})\s*-\s*(\d{1,2}[h:]\d{2})')
    time_elements = orig_soup.find_all(string=time_pattern)
    for elem in time_elements:
        if elem and not is_content_in_trafilatura(elem.strip()):
            # Check if this is likely part of operating hours (look for days of week nearby)
            days_pattern = re.compile(r'(lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche|monday|tuesday|wednesday|thursday|friday|saturday|sunday)', re.IGNORECASE)
            parent = elem.parent
            context = parent.get_text()
            if days_pattern.search(context):
                container = get_parent_with_context(parent)
                important_elements.append({
                    'type': 'hours',
                    'content': str(container),
                    'text': container.get_text(strip=True)
                })
    
    # Remove duplicates based on text content
    seen_texts = set()
    unique_elements = []
    for item in important_elements:
        text = item.get('text', '')
        if text and text not in seen_texts:
            seen_texts.add(text)
            unique_elements.append(item)
    
    return unique_elements

def extract_location_info(preserved_content):
    """
    Extract and deduplicate location information from preserved content.
    Returns a dictionary with address, map_url, phone, and email.
    """
    location_info = {
        'address': None,
        'map_url': None,
        'phone': None,
        'email': None
    }
    
    # Extract address
    address_items = [item for item in preserved_content if item['type'] == 'address']
    if address_items:
        # Get the longest address text as it's likely the most complete
        address_text = max([item.get('text', '').strip() for item in address_items], key=len)
        # Clean up the address text
        address_text = re.sub(r'\s+', ' ', address_text)
        location_info['address'] = address_text
    
    # Extract map URL
    map_items = [item for item in preserved_content if item['type'] == 'map']
    if map_items:
        # Look for URLs containing latitude and longitude
        latlong_pattern = re.compile(r'(?:loc:|place\/|@)(-?\d+\.\d+),?\s*(-?\d+\.\d+)')
        for item in map_items:
            url = item.get('url', '')
            if url and latlong_pattern.search(url):
                location_info['map_url'] = url
                break
    
    # Extract phone
    phone_items = [item for item in preserved_content if item['type'] == 'phone']
    if phone_items:
        # Extract phone numbers
        phone_pattern = re.compile(r'(\+\d{1,3}[ -]?\d{1,3}[ -]?\d{1,3}[ -]?\d{1,4}|\d{2}[ -]?\d{2}[ -]?\d{2}[ -]?\d{2}[ -]?\d{2})')
        phones = []
        for item in phone_items:
            text = item.get('text', '')
            url = item.get('url', '').replace('tel:', '')
            
            # Check text for phone numbers
            if text:
                matches = phone_pattern.findall(text)
                phones.extend(matches)
            
            # Add URL if it's a phone number
            if url and phone_pattern.match(url):
                phones.append(url)
        
        # Deduplicate and get the most complete phone number
        if phones:
            # Normalize phone numbers for comparison
            normalized_phones = [''.join(filter(str.isdigit, phone)) for phone in phones]
            # Get the longest phone number as the canonical one
            longest_index = normalized_phones.index(max(normalized_phones, key=len))
            location_info['phone'] = phones[longest_index]
    
    # Extract email
    email_items = [item for item in preserved_content if item['type'] == 'email']
    if email_items:
        # Extract email addresses
        email_pattern = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}')
        emails = []
        for item in email_items:
            text = item.get('text', '')
            url = item.get('url', '').replace('mailto:', '')
            
            # Check text for email
            if text:
                matches = email_pattern.findall(text)
                emails.extend(matches)
            
            # Add URL if it's an email
            if url and email_pattern.match(url):
                emails.append(url)
        
        # Deduplicate emails
        if emails:
            location_info['email'] = emails[0]  # Just take the first email
    
    return location_info

def extract_hours_info(preserved_content):
    """
    Extract and organize hours information from preserved content.
    Returns a dictionary mapping days to lists of time ranges.
    """
    hours_info = {}
    
    # Find all hours items
    hours_items = [item for item in preserved_content if item['type'] == 'hours']
    
    if not hours_items:
        return hours_info
    
    # Extract time information
    day_pattern = re.compile(r'\b(lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche|monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b', re.IGNORECASE)
    time_pattern = re.compile(r'(\d{1,2}[h:]\d{2})\s*-\s*(\d{1,2}[h:]\d{2})')
    
    for item in hours_items:
        text = item.get('text', '')
        
        # Find all day mentions
        day_matches = day_pattern.findall(text.lower())
        
        # Find all time ranges
        time_matches = time_pattern.findall(text)
        
        # If we have both days and times, associate them
        if day_matches and time_matches:
            for day in day_matches:
                normalized_day = day.lower()
                # Map French day names to English if needed
                day_mapping = {
                    'lundi': 'monday',
                    'mardi': 'tuesday',
                    'mercredi': 'wednesday',
                    'jeudi': 'thursday',
                    'vendredi': 'friday',
                    'samedi': 'saturday',
                    'dimanche': 'sunday'
                }
                normalized_day = day_mapping.get(normalized_day, normalized_day)
                
                # Capitalize the first letter
                display_day = normalized_day[0].upper() + normalized_day[1:]
                
                # Add to hours_info
                if display_day not in hours_info:
                    hours_info[display_day] = []
                
                for time_range in time_matches:
                    start, end = time_range
                    time_str = f"{start} - {end}"
                    if time_str not in hours_info[display_day]:
                        hours_info[display_day].append(time_str)
    
    return hours_info

def merge_structured_data(content, structured_data, format='xml'):
    """
    Merge structured data with content.
    
    Args:
        content (str): Content to merge with (XML or HTML)
        structured_data (dict): Structured data to merge
        format (str): Format of the content ('xml' or 'html')
        
    Returns:
        str: Content with structured data merged in
    """
    if not structured_data:
        return content
    
    # Parse content based on format
    if format.lower() == 'xml':
        soup = BeautifulSoup(content, 'xml')
    else:
        soup = BeautifulSoup(content, 'html.parser')
    
    # Find main content section
    if format.lower() == 'xml':
        main_section = soup.find('main')
        if not main_section:
            main_section = soup.find('body') or soup.find('text')
            if not main_section:
                main_section = soup.new_tag('main')
                soup.append(main_section)
    else:
        main_section = soup.find('body')
        if not main_section:
            main_section = soup.new_tag('body')
            soup.append(main_section)
    
    # Add contact information
    contact_info = structured_data.get('contact', {})
    if any(contact_info.values()):
        # Add contact header
        header = soup.new_tag('p')
        header.string = "Contact Information"
        main_section.append(header)
        
        # Add address
        if contact_info.get('address'):
            p = soup.new_tag('p')
            p.string = contact_info['address']
            main_section.append(p)
        
        # Add map link
        if contact_info.get('map_url'):
            p = soup.new_tag('p')
            p.string = f"Map: {contact_info['map_url']}"
            main_section.append(p)
        
        # Add phone
        if contact_info.get('phone'):
            p = soup.new_tag('p')
            p.string = f"Phone: {contact_info['phone']}"
            main_section.append(p)
        
        # Add email
        if contact_info.get('email'):
            p = soup.new_tag('p')
            p.string = f"Email: {contact_info['email']}"
            main_section.append(p)
    
    # Add hours information
    hours_info = structured_data.get('hours', {})
    if hours_info:
        # Add hours header
        header = soup.new_tag('p')
        header.string = "Opening Hours"
        main_section.append(header)
        
        for day, times in hours_info.items():
            p = soup.new_tag('p')
            p.string = f"{day}: {', '.join(times)}"
            main_section.append(p)
    
    return str(soup)

def process_html(url, html_content=None, headers=None):
    """
    Process HTML content from a URL or directly provided content.
    
    This function implements the complete processing pipeline:
    1. Pre-process original HTML
    2. Parse with trafilatura (XML, HTML, metadata)
    3. Extract structured data
    4. Merge structured data with content
    
    Args:
        url (str): URL of the HTML content
        html_content (str, optional): HTML content to process. If None, fetches from URL
        headers (dict, optional): Headers for HTTP request if fetching from URL
        
    Returns:
        dict: Dictionary containing all processed data
    """
    if headers is None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    # Fetch HTML content if not provided
    if html_content is None:
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            html_content = response.text
        except Exception as e:
            return {
                'error': f"Error fetching URL: {e}",
                'success': False
            }
    
    # 1. Pre-process original HTML
    cleaned_html = pre_parse(html_content)
    
    # 2. Parse with trafilatura
    trafilatura_xml, trafilatura_html, metadata = trafilatura_parse(html_content, url)
    
    # If still no content, return error
    if not trafilatura_xml:
        return {
            'error': "Failed to extract content with trafilatura",
            'success': False
        }
    
    # 3. Extract structured data
    structured_data = extract_structured_data(html_content, trafilatura_xml)
    
    # 4. Merge structured data with content
    final_xml = merge_structured_data(trafilatura_xml, structured_data, format='xml')
    final_html = merge_structured_data(trafilatura_html, structured_data, format='html') if trafilatura_html else None
    # soup = BeautifulSoup(final_html, 'html.parser')
    # final_html = soup.prettify()
    
    # Return all processed data
    result = {
        'success': True,
        'cleaned_html': cleaned_html,
        'trafilatura_xml': trafilatura_xml,
        'trafilatura_html': trafilatura_html,
        'metadata': metadata,
        'structured_data': structured_data,
        'final_xml': final_xml,
        'final_html': final_html
    }
    
    return result

def main():
    """
    Simple demonstration of the full processing pipeline.
    """
    # Example URLs to test
    urls = [
        'https://qx1.org/lieu/ecm-boutik/',
        'https://qx1.org/lieu/armee-du-salut-paroisse-de-saint-mauront/',
        'https://qx1.org/lieu/rusf-reseau-universite-sans-frontieres/',
        'https://refugies.info/dispositif/603fc01d7e319900146336a5'
    ]
    
    for url in urls:
        print(f"\n{'='*80}")
        print(f"Processing {url}")
        print(f"{'='*80}")
        
        # Process the URL
        result = process_html(url)
        
        if result['success']:
            # Print summary of results
            print("\nProcessing successful!")
            print(f"Metadata: {result['metadata']}")
            print(f"HTML content:")
            print(result['final_html'])
            print(f"Structured data:")
            print(f"  Contact: {result['structured_data']['contact']}")
            print(f"  Hours: {result['structured_data']['hours']}")
            print(f"  Preserved elements: {len(result['structured_data']['preserved_elements'])}")
            
            # Save results to files (optional)
            # ... (save code would go here) ...
        else:
            print(f"Error: {result['error']}")
        
        print(f"{'='*80}")

if __name__ == "__main__":
    main()