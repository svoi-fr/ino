from typing import Optional, BinaryIO, Union, List
from datetime import datetime
from urllib.parse import urlparse
import langdetect
import re
from io import BytesIO
import fitz  # PyMuPDF
from bs4 import BeautifulSoup
from document import DocumentData, clean_text
from document_html import html_to_raw_markdown
from pdftitle import get_title_from_io

def is_likely_header(text: str, prev_text: Optional[str] = None) -> bool:
    """Check if text looks like a header."""
    if re.match(r'^(?:[A-Z0-9][.])+\s', text):
        return True
    if len(text) < 100 and text.isupper():
        return True
    if prev_text and prev_text.strip().endswith(':'):
        return False
    return False

def parse_pdf_document(pdf_bytes: Union[bytes, BinaryIO], url: Optional[str] = None) -> DocumentData:
    doc = DocumentData(url=url)
    pdf_doc = None
    
    try:
        if isinstance(pdf_bytes, bytes):
            pdf_doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        else:
            pdf_doc = fitz.open(stream=pdf_bytes.read(), filetype="pdf")
        
        # First analyze document structure and get text blocks for title extraction
        text_blocks = []
        font_sizes = []
        
        for page in pdf_doc:
            blocks = page.get_text("dict", sort=True)["blocks"]
            for block in blocks:
                if "lines" not in block:
                    continue
                    
                block_text = ""
                block_sizes = []
                is_bold = False
                
                for line in block["lines"]:
                    if "spans" not in line:
                        continue
                    for span in line["spans"]:
                        if span["text"].strip():
                            block_text += span["text"].strip() + " "
                            block_sizes.append(span["size"])
                            if span["flags"] & 2**4:  # Bold flag
                                is_bold = True
                
                if block_text.strip():
                    avg_size = sum(block_sizes) / len(block_sizes) if block_sizes else 0
                    text_blocks.append({
                        'text': block_text.strip(),
                        'avg_size': avg_size,
                        'is_bold': is_bold,
                        'len': len(block_text.strip()),
                        'bbox': block.get('bbox')
                    })
                    font_sizes.extend(block_sizes)
        
        # Get normal font size for title extraction
        normal_size = float(max(set(font_sizes), key=font_sizes.count)) if font_sizes else 12.0
        
        # Extract title first
        doc.title = extract_title(text_blocks, pdf_doc.metadata, normal_size, pdf_bytes)
        
        # Get HTML content with proper structure
        html_parts = []
        for page in pdf_doc:
            blocks = page.get_text("dict", sort=True)["blocks"]
            for block in blocks:
                if "lines" not in block:
                    continue
                    
                text = ""
                is_bold = False
                block_sizes = []
                
                for line in block["lines"]:
                    line_text = ""
                    for span in line["spans"]:
                        if span["flags"] & 2**4:  # Bold flag
                            is_bold = True
                        line_text += span["text"] + " "
                        block_sizes.append(span["size"])
                    text += line_text.strip() + "\n"
                
                # Clean up multiple newlines and trailing spaces
                text = re.sub(r'\s*\n\s*', ' ', text)
                text = text.strip()
                
                if not text:
                    continue
                    
                # Calculate size ratio
                avg_size = sum(block_sizes) / len(block_sizes) if block_sizes else 0
                size_ratio = avg_size / normal_size if normal_size else 1
                    
                # Handle bullet points
                if text.startswith('•') or text.startswith('-'):
                    text = text[1:].strip()  # Just remove the bullet point
                # Handle headers (using size ratio and bold)
                elif size_ratio > 1.3 or (is_bold and size_ratio > 1.2):
                    text = f"<h2>{text}</h2>"
                # Handle bold text
                elif is_bold:
                    text = f"<b>{text}</b>"
                
                html_parts.append(text)
        
        full_html = "\n".join(html_parts)
        doc.html_raw = full_html
        
        # Store raw text
        doc.text = BeautifulSoup(full_html, 'html.parser').get_text()
        
        if not doc.text:
            raise ValueError("Failed to extract text content from PDF")
        
        extract_pdf_metadata(doc, pdf_doc)
        
        # Convert HTML to markdown while preserving structure
        doc.md_content = html_to_raw_markdown(full_html)
        doc.md_raw = doc.md_content
        
        if url:
            doc.url = url
            url_parsed = urlparse(url)
            doc.hostname = url_parsed.netloc
            doc.sitename = url_parsed.netloc.split('.')[-2]
            
        doc.language = langdetect.detect(doc.text[:1000])
        
    finally:
        if pdf_doc:
            pdf_doc.close()
    
    return doc

def extract_pdf_text(pdf_doc: fitz.Document) -> str:
    """Extract text from PDF with smarter header detection and better formatting preservation."""
    html_parts = []
    last_block_bottom = None
    
    # First pass: analyze document structure
    font_sizes = []
    text_blocks = []  # Store all text blocks for analysis
    
    for page in pdf_doc:
        blocks = page.get_text("dict", sort=True)["blocks"]
        for block in blocks:
            if "lines" not in block:
                continue
                
            block_text = ""
            block_sizes = []
            is_bold = False
            
            # Collect all text from spans before processing
            spans_text = []
            for line in block["lines"]:
                if "spans" not in line:
                    continue
                    
                line_text = ""
                for span in line["spans"]:
                    if span["text"].strip():
                        line_text += span["text"].strip() + " "
                        block_sizes.append(span["size"])
                        if span["flags"] & 2**4:  # Bold flag
                            is_bold = True
                spans_text.append(line_text)
            
            # Join lines and handle hyphenation
            joined_text = ""
            for i, line_text in enumerate(spans_text):
                if i < len(spans_text) - 1:
                    # If line ends with hyphen and next line starts with letter
                    if line_text.strip().endswith('-') and spans_text[i+1].strip() and spans_text[i+1].strip()[0].isalpha():
                        # Join hyphenated word
                        joined_text += line_text.strip()[:-1]  # Remove hyphen
                    else:
                        joined_text += line_text + " "
                else:
                    joined_text += line_text
            
            block_text = joined_text.strip()
            
            if block_text:
                avg_size = sum(block_sizes) / len(block_sizes) if block_sizes else 0
                text_blocks.append({
                    'text': block_text,
                    'avg_size': avg_size,
                    'is_bold': is_bold,
                    'len': len(block_text),
                    'bbox': block.get('bbox')
                })
                font_sizes.extend(block_sizes)
    
    if not font_sizes or not text_blocks:
        return ""
    
    # Analyze font sizes
    font_sizes.sort()
    normal_size = float(max(set(font_sizes), key=font_sizes.count))
    
    # Process blocks with context awareness
    prev_text = None
    for i, block in enumerate(text_blocks):
        text = block['text']
        size_ratio = block['avg_size'] / normal_size if normal_size else 1
        
        # Determine if this block should be a header
        is_header = False
        header_level = 0
        
        if is_likely_header(text, prev_text):
            is_header = True
            # Determine header level based on size and context
            if size_ratio > 1.3 or block['is_bold']:
                header_level = 1
            else:
                header_level = 2
        elif size_ratio > 1.4 and block['is_bold']:
            is_header = True
            header_level = 1
        elif size_ratio > 1.2 and block['is_bold']:
            is_header = True
            header_level = 2
        
        # Format the block
        if is_header:
            html_parts.append(f"<h{header_level}>{text}</h{header_level}>")
        else:
            # Handle bullet points
            if text.startswith('•') or text.startswith('-'):
                text = text[1:].strip()  # Just remove the bullet point
            else:
                text = f"<p>{text}</p>"
            html_parts.append(text)
        
        prev_text = text
        
        # Add spacing between blocks based on vertical position
        if i < len(text_blocks) - 1:
            current_bottom = block['bbox'][3]
            next_top = text_blocks[i + 1]['bbox'][1]
            if next_top - current_bottom > normal_size * 1.5:
                html_parts.append("<p></p>")
    
    # Deduplicate content while preserving order
    seen_content = set()
    unique_parts = []
    
    for part in html_parts:
        # Clean up the text content for comparison
        clean_content = re.sub(r'<[^>]+>', '', part).strip()
        clean_content = re.sub(r'\s+', ' ', clean_content)
        
        # Skip empty parts
        if not clean_content:
            continue
            
        # Check for footer/header patterns
        is_footer = False
        footer_patterns = [
            r'^.*(?:all rights reserved|tous droits réservés).*$',
            r'^(?:https?://)?(?:www\.)?[a-zA-Z0-9-]+\.[a-zA-Z]{2,}/?$',  # Standalone URLs
            r'^.*watizat\.org.*$'  # Specific to your case
        ]
        
        for pattern in footer_patterns:
            if re.match(pattern, clean_content, re.IGNORECASE):
                is_footer = True
                break
        
        if is_footer:
            continue
            
        # Skip standalone page numbers
        if re.match(r'^\d+$', clean_content):
            continue
            
        # Skip standalone dates (assuming various formats)
        if re.match(r'^(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{4}$', clean_content, re.I):
            continue
        
        # Only add if we haven't seen this content before
        if clean_content not in seen_content:
            seen_content.add(clean_content)
            unique_parts.append(part)
    
    html = "\n".join(unique_parts)
    
    # Post-process the HTML
    # Wrap consecutive li elements in ul tags
    html = re.sub(r'(?:<li>(?:(?!</li>).)*</li>)+', lambda m: f"<ul>{m.group(0)}</ul>", html)
    
    # Clean up multiple consecutive newlines
    html = re.sub(r'\n{3,}', '\n\n', html)
    
    return clean_text(html)

def extract_pdf_metadata(doc: DocumentData, pdf_doc: fitz.Document):
    """Extract metadata from PDF document."""
    metadata = pdf_doc.metadata
    
    metadata_mapping = {
        'title': 'title',
        'author': 'author',
        'subject': 'description',
        'keywords': 'keywords',
        'creator': 'creator',
        'producer': 'producer'
    }
    
    for pdf_key, meta_key in metadata_mapping.items():
        value = metadata.get(pdf_key)
        if value:
            if meta_key in ('title', 'author', 'description'):
                setattr(doc, meta_key, value)
    
    # Handle dates
    if 'creationDate' in metadata:
        try:
            date_str = metadata['creationDate']
            if date_str.startswith('D:'):
                date_str = date_str[2:]
            doc.date = datetime.strptime(date_str[:14], '%Y%m%d%H%M%S')
        except (ValueError, IndexError):
            pass
    elif 'modDate' in metadata:
        try:
            date_str = metadata['modDate']
            if date_str.startswith('D:'):
                date_str = date_str[2:]
            doc.date = datetime.strptime(date_str[:14], '%Y%m%d%H%M%S')
        except (ValueError, IndexError):
            pass

def clean_text(text: str) -> str:
    """Clean extracted PDF text by handling hyphenation and other issues"""
    # Handle hyphenated words at line breaks
    text = re.sub(r'(\w+)-\n(\w+)', r'\1\2', text)
    
    # Remove excessive newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    # Clean up spaces
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def extract_title(text_blocks: List[dict], metadata: dict, normal_size: float, pdf_bytes: Union[bytes, BinaryIO]) -> str:
    """Extract title using multiple strategies"""
    # Strategy 1: Use PDF metadata if available and valid
    if metadata.get('title') and len(metadata['title']) > 3:
        return metadata['title'][:255]
    
    # Strategy 2: Try pdftitle library
    try:
        if isinstance(pdf_bytes, bytes):
            pdf_io = BytesIO(pdf_bytes)
        else:
            pdf_io = pdf_bytes
        title = get_title_from_io(pdf_io)
        if title and len(title) > 3:
            return title[:255]
    except:
        pass
    
    # Strategy 3: Look for the largest text in the first few blocks
    first_blocks = text_blocks[:5]
    if first_blocks:
        title_candidates = sorted(
            first_blocks,
            key=lambda b: (b['avg_size'], -b['bbox'][1] if b['bbox'] else 0),
            reverse=True
        )
        
        for block in title_candidates:
            text = block['text'].strip()
            if 10 < len(text) < 200 and not any(p in text.lower() for p in ['tous droits', 'copyright']):
                return text[:255]
    
    return ""

def blocks_to_markdown(text_blocks: List[dict], normal_size: float) -> str:
    markdown_parts = []
    prev_text = ""
    
    for i, block in enumerate(text_blocks):
        text = block['text'].strip()
        if not text:
            continue
            
        # Calculate size ratio compared to normal text
        size_ratio = block['avg_size'] / normal_size if normal_size > 0 else 1
        is_header = False
        header_level = 0
        
        if is_likely_header(text, prev_text):
            is_header = True
            # Determine header level based on size and context
            if size_ratio > 1.3 or block['is_bold']:
                header_level = 1
            else:
                header_level = 2
        elif size_ratio > 1.4 and block['is_bold']:
            is_header = True
            header_level = 1
        elif size_ratio > 1.2 and block['is_bold']:
            is_header = True
            header_level = 2
        
        # Format the block
        if is_header:
            markdown_parts.append(f"{'#' * header_level} {text}")
        else:
            # Handle bullet points
            if text.startswith('•') or text.startswith('-'):
                markdown_parts.append(f"- {text[1:].strip()}")
            else:
                markdown_parts.append(text)
        
        prev_text = text
        
        # Add spacing between blocks based on vertical position
        if i < len(text_blocks) - 1:
            current_bottom = block['bbox'][3]
            next_top = text_blocks[i + 1]['bbox'][1]
            if next_top - current_bottom > normal_size * 1.5:
                markdown_parts.append("")
    
    return "\n".join(markdown_parts)

def clean_raw_html(html: str) -> str:
    """Clean up raw PDF HTML output."""
    # Remove inline styles and positioning
    html = re.sub(r'\s*style="[^"]*"', '', html)
    html = re.sub(r'\s*top:\d+\.?\d*pt;', '', html)
    html = re.sub(r'\s*left:\d+\.?\d*pt;', '', html)
    
    # Normalize font tags
    html = re.sub(r'<span[^>]*font-family:[^>]*>', '<span>', html)
    html = re.sub(r'<span[^>]*>', '<span>', html)
    
    # Join fragmented paragraphs
    html = re.sub(r'</p>\s*<p[^>]*>', ' ', html)
    
    # Clean up whitespace
    html = re.sub(r'\s+', ' ', html)
    html = re.sub(r'>\s+<', '><', html)
    
    return html.strip()