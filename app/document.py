# document.py
from typing import Optional, Dict, List, Any, BinaryIO, Union
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import urlparse
import langdetect
import re
from bs4 import BeautifulSoup

@dataclass
class DocumentData:
    url: Optional[str] = None
    title: Optional[str] = None
    text: Optional[str] = None
    md_content: Optional[str] = None
    links: List[str] = field(default_factory=list)
    hostname: Optional[str] = None
    sitename: Optional[str] = None
    language: Optional[str] = None
    author: Optional[str] = None
    description: Optional[str] = None
    date: Optional[datetime] = None
    md_raw: Optional[str] = None
    html_raw: Optional[str] = None

    def is_valid(self) -> bool:
        """Check if document has valid content"""
        return bool(self.text and len(self.text.strip()) > 0)

def clean_text(text: str) -> str:
    if not text:
        return ""
        
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove form feed characters
    text = text.replace('\f', '\n\n')
    
    # Clean up newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    return text.strip()

def text_to_markdown(text: str) -> str:
    if not text:
        return ""
        
    # Split into lines
    lines = text.split('\n')
    
    # Process each line
    markdown_lines = []
    in_paragraph = False
    
    for line in lines:
        line = line.strip()
        
        if not line:
            if in_paragraph:
                markdown_lines.append('')
                in_paragraph = False
            continue
            
        # Detect potential headers
        if line.isupper() and len(line) > 3:
            if in_paragraph:
                markdown_lines.append('')
                in_paragraph = False
            markdown_lines.append(f'## {line.title()}')
            continue
        
        # Regular text
        if in_paragraph:
            markdown_lines.append(line)
        else:
            in_paragraph = True
            markdown_lines.append(line)
    
    return '\n'.join(markdown_lines)
