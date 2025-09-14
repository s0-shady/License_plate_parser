#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parser for data from tablica-rejestracyjna.pl
Project for analyzing "bad behavior density" in Poland :)
"""

import requests
import re
import time
import psycopg2
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import logging
import sys
import random
from typing import List, Dict, Optional

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class LicensePlateParser:
    def __init__(self):
        self.base_url = "https://tablica-rejestracyjna.pl"
        self.comments_url = f"{self.base_url}/komentarze"
        self.session = requests.Session()
        
        # Headers to look like a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pl,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
        # Database configuration
        self.db_config = {
            'host': 'localhost',
            'database': 'license_plate_reports_db',
            'user': 'license_plate_analyzer',
            'password': 'secure_password_123'
        }
        
        # Parsing statistics
        self.start_time = None
        self.processed_pages = 0
        self.total_pages = 85519
        self.total_records = 0
        self.error_count = 0
        self.debug_mode = False
        
    def connect_db(self):
        """Establish database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = True
            return conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def extract_region_code(self, plate_number: str) -> str:
        """Extract regional part from license plate"""
        plate_clean = re.sub(r'\s+', ' ', plate_number.strip().upper())
        
        # Multiple patterns for different plate formats
        patterns = [
            r'^([A-Z]{2,3})\s+\d',      # WA 1234, WWL 1234
            r'^([A-Z]{2,3})\d',         # WA1234, WWL1234  
            r'^([A-Z]{2,3})',           # Fallback: just letters
        ]
        
        for pattern in patterns:
            match = re.match(pattern, plate_clean)
            if match:
                return match.group(1)
        
        # Last resort: first 2-3 letters
        letters_only = re.match(r'^[A-Z]+', plate_clean)
        if letters_only:
            letters = letters_only.group(0)
            return letters[:3] if len(letters) >= 3 else letters[:2]
        
        return 'UNK'
    
    def parse_datetime(self, date_str: str) -> Optional[datetime]:
        """Parse date from various formats"""
        date_str = date_str.strip()
        
        date_formats = [
            '%Y-%m-%d %H:%M:%S',
            '%d.%m.%Y %H:%M:%S',
            '%d-%m-%Y %H:%M:%S',
            '%Y-%m-%d',
            '%d.%m.%Y',
            '%d-%m-%Y',
            '%d/%m/%Y %H:%M:%S',
            '%d/%m/%Y'
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        logger.warning(f"Cannot parse date: {date_str}")
        return None
    
    def extract_comments_from_page(self, page_content: str) -> List[Dict]:
        """Extract comments from HTML page with multiple parsing strategies"""
        soup = BeautifulSoup(page_content, 'html.parser')
        comments = []
        
        # Debug mode - save page content
        if self.debug_mode:
            with open('debug_page.html', 'w', encoding='utf-8') as f:
                f.write(page_content)
            logger.info("DEBUG: Saved page content to debug_page.html")
        
        text_content = soup.get_text()
        
        if self.debug_mode:
            logger.info(f"DEBUG: Page content length: {len(text_content)} chars")
            logger.info(f"DEBUG: First 500 chars: {text_content[:500]}")
        
        # Multiple parsing strategies
        strategies = [
            self._parse_strategy_1,
            self._parse_strategy_2,
            self._parse_strategy_3
        ]
        
        for i, strategy in enumerate(strategies):
            try:
                comments = strategy(text_content)
                if comments:
                    logger.info(f"Strategy {i+1} found {len(comments)} comments")
                    break
                else:
                    logger.info(f"Strategy {i+1} found 0 comments")
            except Exception as e:
                logger.warning(f"Strategy {i+1} failed: {e}")
        
        return comments
    
    def _parse_strategy_1(self, text_content: str) -> List[Dict]:
        """Original parsing strategy with improved pattern"""
        comments = []
        
        # Look for pattern: PLATE · DATE CONTENT
        pattern = r'([A-Z]{1,4}\s*\d{1,5}[A-Z]*)\s*[·•]\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s*([^\n]+?)(?=\n|$)'
        
        matches = re.finditer(pattern, text_content, re.MULTILINE)
        
        for match in matches:
            plate = match.group(1).strip()
            date_str = match.group(2).strip()
            comment_text = match.group(3).strip()
            
            # Extract author from comment
            parts = comment_text.split(' ', 3)
            author = parts[0] if parts else "Anonymous"
            comment_clean = ' '.join(parts[1:]) if len(parts) > 1 else comment_text
            
            parsed_date = self.parse_datetime(date_str)
            if parsed_date:
                comments.append({
                    'plate': plate,
                    'date': parsed_date,
                    'author': author[:255],
                    'comment': comment_clean[:1000] if comment_clean else ""
                })
        
        return comments
    
    def _parse_strategy_2(self, text_content: str) -> List[Dict]:
        """Alternative parsing strategy - look for license plates first"""
        comments = []
        
        # Find all potential license plates
        plate_pattern = r'([A-Z]{2,3}\s*\d{1,5}[A-Z]*)'
        plates = re.finditer(plate_pattern, text_content)
        
        for plate_match in plates:
            plate = plate_match.group(1).strip()
            start_pos = plate_match.end()
            
            # Look for date after the plate (within next 100 characters)
            text_after = text_content[start_pos:start_pos+200]
            date_match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', text_after)
            
            if date_match:
                date_str = date_match.group(1)
                comment_start = start_pos + date_match.end()
                comment_end = text_content.find('\n', comment_start, comment_start + 500)
                if comment_end == -1:
                    comment_end = comment_start + 500
                
                comment_text = text_content[comment_start:comment_end].strip()
                
                # Extract author and comment
                words = comment_text.split()
                author = words[0] if words else "Anonymous"
                comment_clean = ' '.join(words[1:]) if len(words) > 1 else ""
                
                parsed_date = self.parse_datetime(date_str)
                if parsed_date:
                    comments.append({
                        'plate': plate,
                        'date': parsed_date,
                        'author': author[:255],
                        'comment': comment_clean[:1000]
                    })
        
        return comments
    
    def _parse_strategy_3(self, text_content: str) -> List[Dict]:
        """HTML-based parsing strategy"""
        comments = []
        
        # Try to parse from HTML structure
        soup = BeautifulSoup(text_content, 'html.parser')
        
        # Look for common HTML patterns
        potential_comments = soup.find_all(['div', 'p', 'span'], string=re.compile(r'[A-Z]{2,3}\s*\d'))
        
        for element in potential_comments[:50]:  # Limit to avoid too much processing
            text = element.get_text().strip()
            
            # Try to extract plate and date
            plate_match = re.search(r'([A-Z]{2,3}\s*\d{1,5}[A-Z]*)', text)
            date_match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', text)
            
            if plate_match and date_match:
                plate = plate_match.group(1)
                date_str = date_match.group(1)
                
                # Extract comment (text after date)
                comment_text = text[date_match.end():].strip()
                words = comment_text.split()
                author = words[0] if words else "Anonymous"
                comment_clean = ' '.join(words[1:]) if len(words) > 1 else ""
                
                parsed_date = self.parse_datetime(date_str)
                if parsed_date:
                    comments.append({
                        'plate': plate,
                        'date': parsed_date,
                        'author': author[:255],
                        'comment': comment_clean[:1000]
                    })
        
        return comments
    
    def save_comments_to_db(self, comments: List[Dict]) -> int:
        """Save comments to database"""
        if not comments:
            return 0
        
        conn = self.connect_db()
        saved_count = 0
        
        try:
            cursor = conn.cursor()
            
            for comment in comments:
                try:
                    region_code = self.extract_region_code(comment['plate'])
                    
                    cursor.execute("""
                        INSERT INTO reports 
                        (license_plate, region_code, report_date, comment_text, author_name)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        comment['plate'],
                        region_code,
                        comment['date'],
                        comment['comment'],
                        comment['author']
                    ))
                    saved_count += 1
                    
                except Exception as e:
                    logger.warning(f"Error saving comment {comment['plate']}: {e}")
                    self.error_count += 1
                    
        except Exception as e:
            logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()
            
        return saved_count
    
    def fetch_page(self, page_num: int) -> Optional[str]:
        """Fetch specific page"""
        if page_num == 1:
            url = self.comments_url
        else:
            url = f"{self.comments_url}?p={page_num}"
        
        try:
            # Random delay
            time.sleep(random.uniform(1.0, 3.0))
            
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            logger.info(f"Fetched page {page_num}, size: {len(response.text)} chars")
            return response.text
            
        except requests.RequestException as e:
            logger.error(f"Error fetching page {page_num}: {e}")
            self.error_count += 1
            return None
    
    def calculate_eta(self) -> str:
        """Calculate estimated time"""
        if self.processed_pages == 0 or not self.start_time:
            return "unknown"
        
        elapsed_time = time.time() - self.start_time
        avg_time_per_page = elapsed_time / self.processed_pages
        remaining_pages = self.total_pages - self.processed_pages
        eta_seconds = remaining_pages * avg_time_per_page
        
        eta_delta = timedelta(seconds=int(eta_seconds))
        return str(eta_delta)
    
    def print_progress(self):
        """Display progress"""
        if self.processed_pages % 5 == 0 or self.processed_pages <= 5:
            progress_pct = (self.processed_pages / self.total_pages) * 100
            eta = self.calculate_eta()
            
            logger.info(f"Processed: {self.processed_pages}/{self.total_pages} "
                       f"({progress_pct:.2f}%) | "
                       f"Records: {self.total_records} | "
                       f"Errors: {self.error_count} | "
                       f"ETA: {eta}")
    
    def run_parser(self, start_page: int = 1, end_page: Optional[int] = None, debug: bool = False):
        """Main parser function"""
        self.start_time = time.time()
        self.debug_mode = debug
        
        if end_page is None:
            end_page = self.total_pages
        
        logger.info(f"Starting parsing of pages {start_page}-{end_page}")
        if debug:
            logger.info("DEBUG MODE ENABLED - will save debug info")
        
        for page_num in range(start_page, end_page + 1):
            try:
                page_content = self.fetch_page(page_num)
                if page_content is None:
                    continue
                
                comments = self.extract_comments_from_page(page_content)
                saved_count = self.save_comments_to_db(comments)
                
                self.processed_pages += 1
                self.total_records += saved_count
                
                logger.info(f"Page {page_num}: Found {len(comments)} comments, saved {saved_count}")
                
                self.print_progress()
                
                if saved_count == 0 and page_num > 5:
                    logger.warning(f"Page {page_num} contains no data")
                
            except KeyboardInterrupt:
                logger.info("Interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error processing page {page_num}: {e}")
                self.error_count += 1
                continue
        
        elapsed_time = time.time() - self.start_time
        logger.info(f"\n=== PARSING SUMMARY ===")
        logger.info(f"Processed pages: {self.processed_pages}")
        logger.info(f"Saved records: {self.total_records}")
        logger.info(f"Errors: {self.error_count}")
        logger.info(f"Duration: {timedelta(seconds=int(elapsed_time))}")
        logger.info(f"Average per page: {elapsed_time/self.processed_pages:.2f}s")

def main():
    """Main function"""
    parser = LicensePlateParser()
    
    debug_mode = False
    start_page = 1
    end_page = None
    
    # Parse command line arguments
    if len(sys.argv) >= 2:
        if sys.argv[1] == '--debug':
            debug_mode = True
            start_page = int(sys.argv[2]) if len(sys.argv) >= 3 else 1
            end_page = int(sys.argv[3]) if len(sys.argv) >= 4 else 1
        else:
            start_page = int(sys.argv[1])
            end_page = int(sys.argv[2]) if len(sys.argv) >= 3 else None
    else:
        print("Parser for tablica-rejestracyjna.pl")
        print("Usage:")
        print("  python3 license_plate_parser.py [start_page] [end_page]")
        print("  python3 license_plate_parser.py --debug [page_num]")
        print("  python3 license_plate_parser.py 1 85519  # Full parsing")
        print()
        
        response = input("Run test with first 3 pages? (y/N): ")
        if response.lower() != 'y':
            sys.exit(0)
            
        start_page = 1
        end_page = 3
        debug_mode = True
    
    try:
        parser.run_parser(start_page, end_page, debug_mode)
    except Exception as e:
        logger.error(f"Critical parser error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
