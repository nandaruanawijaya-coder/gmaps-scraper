"""
Search engine for Google Maps
"""
import re
import time
import random
from typing import List, Set, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

from models.place import SearchTask, Place
from config.settings import ScraperConfig
from core.driver_manager import DriverManager
from utils.extractors import (
    extract_coordinates_from_link, 
    parse_address, 
    clean_text,
    parse_rating,
    parse_reviews_count
)


class MapsSearchEngine:
    """Handles searching and extracting data from Google Maps"""
    
    def __init__(self, driver_manager: DriverManager, config: ScraperConfig):
        self.driver_manager = driver_manager
        self.config = config
        self.driver = driver_manager.driver
        self.seen_links: Set[str] = set()
        self.seen_names: Set[str] = set()
    
    def search(self, task: SearchTask) -> List[Place]:
        """
        Execute a search task and return found places
        
        Args:
            task: SearchTask to execute
            
        Returns:
            List of Place objects
        """
        print(f"Searching: {task}")
        
        places = []
        query = task.get_query()
        
        # Perform search with retry
        if not self._perform_search(query):
            print(f"Failed to perform search for: {query}")
            return places
        
        # Wait for results to load
        time.sleep(self.config.scroll_pause_time)
        
        # Scroll to load more results
        place_hrefs = self._scroll_and_collect_elements(task.max_results)
        
        print(f"Found {len(place_hrefs)} unique place hrefs")
        
        # Extract details from each place by finding fresh element for each href
        seen_urls_this_task = set()  # Track URLs for safety
        
        for idx, href in enumerate(place_hrefs):
            try:
                # Find FRESH element by href each time
                place = self._extract_place_details_by_href(href, task, idx, len(place_hrefs))
                
                if place and self._is_valid_place(place):
                    # Double-check for duplicate URL (shouldn't happen but safety check)
                    if place.google_maps_link in seen_urls_this_task:
                        print(f"  [{idx+1}/{len(place_hrefs)}] ⚠️  DUPLICATE URL: {place.name}")
                        continue
                    
                    # Valid new place!
                    seen_urls_this_task.add(place.google_maps_link)
                    places.append(place)
                    print(f"  [{idx+1}/{len(place_hrefs)}] ✓ {place.name}")
                
                # Random delay
                time.sleep(random.uniform(self.config.min_delay, self.config.max_delay))
                
            except Exception as e:
                print(f"  [{idx+1}/{len(place_hrefs)}] Error: {e}")
                continue
        
        print(f"Collected {len(places)} places for: {task}")
        return places
    
    def _perform_search(self, query: str, max_retries: int = 3) -> bool:
        """Perform search with retry"""
        for attempt in range(max_retries):
            try:
                print(f"  Search attempt {attempt + 1}/{max_retries}")
                
                if not self.driver_manager.reset_to_maps_home():
                    continue
                
                search_box = WebDriverWait(self.driver, self.config.element_wait_timeout).until(
                    EC.presence_of_element_located((
                        By.CSS_SELECTOR,
                        'input[aria-label="Search Google Maps"], input[id="searchboxinput"]'
                    ))
                )
                
                search_box.clear()
                time.sleep(0.5)
                search_box.send_keys(query)
                time.sleep(1)
                search_box.send_keys(Keys.RETURN)
                time.sleep(3)
                
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]'))
                    )
                    return True
                except TimeoutException:
                    print(f"  Results timeout")
                    
            except Exception as e:
                print(f"  Search failed: {e}")
                time.sleep(2)
        
        return False
    
    def _scroll_and_collect_elements(self, max_results: int) -> List:
        """Scroll and collect unique place HREFS (not elements) by tracking unique hrefs"""
        
        try:
            results_panel = None
            for selector in ['div[role="feed"]', 'div.m6QErb']:
                try:
                    results_panel = self.driver.find_element(By.CSS_SELECTOR, selector)
                    print(f"  Found panel: {selector}")
                    break
                except:
                    continue
            
            if not results_panel:
                return []
            
            scroll_attempts = 0
            no_change_count = 0
            seen_hrefs = []  # Use LIST to preserve order, not set
            
            while len(seen_hrefs) < max_results and scroll_attempts < self.config.max_scroll_attempts:
                # Get current elements
                current_elements = []
                for selector in ['a.hfpxzc', 'div.Nv2PK']:
                    try:
                        current_elements = results_panel.find_elements(By.CSS_SELECTOR, selector)
                        if len(current_elements) > 0:
                            break
                    except:
                        continue
                
                # Check each element for uniqueness
                new_count = 0
                for idx, elem in enumerate(current_elements):
                    try:
                        href = elem.get_attribute('href')
                        if href and href not in seen_hrefs:
                            seen_hrefs.append(href)  # Preserve order
                            new_count += 1
                    except:
                        continue
                
                # Check if we got new unique elements
                if new_count == 0:
                    no_change_count += 1
                else:
                    no_change_count = 0
                
                # Stop if no new unique results after multiple scrolls
                if no_change_count >= 3:
                    print(f"  No new unique results after {no_change_count} scrolls")
                    break
                
                # Scroll down
                self.driver.execute_script(
                    "arguments[0].scrollTop = arguments[0].scrollHeight",
                    results_panel
                )
                
                time.sleep(self.config.scroll_pause_time)
                scroll_attempts += 1
                print(f"  Scroll {scroll_attempts}: {len(seen_hrefs)} unique hrefs")
            
            # Return list of hrefs (NOT elements!)
            final_hrefs = seen_hrefs[:max_results]
            print(f"  Collected {len(final_hrefs)} unique hrefs")
            return final_hrefs
            
        except Exception as e:
            print(f"Scroll error: {e}")
            return []
    
    def _extract_place_details_by_href(self, href: str, task: SearchTask, idx: int, total: int) -> Optional[Place]:
        """Extract place details by finding fresh element with this href and clicking it"""
        try:
            # Find results panel
            results_panel = None
            for selector in ['div[role="feed"]', 'div.m6QErb']:
                try:
                    results_panel = self.driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except:
                    continue
            
            if not results_panel:
                print(f"  [{idx+1}/{total}] ❌ Can't find results panel")
                return None
            
            # Find FRESH element with this href
            element = None
            try:
                # Try to find by exact href
                elements = results_panel.find_elements(By.CSS_SELECTOR, 'a.hfpxzc')
                for elem in elements:
                    try:
                        elem_href = elem.get_attribute('href')
                        if elem_href == href:
                            element = elem
                            break
                    except:
                        continue
            except Exception as e:
                print(f"  [{idx+1}/{total}] ❌ Error finding element: {e}")
                return None
            
            if not element:
                print(f"  [{idx+1}/{total}] ❌ Element not found for href")
                return None
            
            # Scroll into view
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                time.sleep(0.5)
            except:
                pass
            
            # Click
            try:
                self.driver.execute_script("arguments[0].click();", element)
            except Exception as e:
                print(f"  [{idx+1}/{total}] ❌ Click failed: {e}")
                return None
            
            # Wait for details panel
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.m6QErb[aria-label]'))
                )
            except TimeoutException:
                pass
            
            # Wait for content to render
            time.sleep(4)
            
            # Extract name first to see what we actually got
            name = self._extract_text('h1.DUwDvf, h1.fontHeadlineLarge, h1')
            
            if not name or name in ['Hasil', 'Results', '']:
                print(f"  [{idx+1}/{total}] ❌ Could not extract name")
                return None
            
            # Extract category
            category = self._extract_text('button.DkEaL, div.LBgpqf button, button[jsaction*="category"]')
            
            # Extract address
            address = self._extract_text('button[data-item-id="address"], div.rogA2c, button[aria-label*="Address"]')
            
            # Try aria-label if address not found
            if not address:
                try:
                    addr_elems = self.driver.find_elements(By.CSS_SELECTOR, 'button[data-item-id*="address"]')
                    for elem in addr_elems:
                        aria = elem.get_attribute('aria-label')
                        if aria and ':' in aria:
                            address = aria.split(':', 1)[1].strip()
                            break
                except:
                    pass
            
            subdistrict, district, city, province, zip_code = parse_address(address) if address else (None, None, None, None, None)
            
            # Get coordinates and link from current URL (where we actually are)
            link = self.driver.current_url
            latitude, longitude = extract_coordinates_from_link(link)
            
            # Extract rating - try multiple methods
            rating = None
            rating_text = None
            
            # Method 1: Get from parent container (gets full "4.5" text)
            try:
                rating_container = self.driver.find_element(By.CSS_SELECTOR, 'div.F7nice')
                full_text = rating_container.text
                # Extract rating from text like "4.5" or "4.5 (123 reviews)"
                match = re.search(r'(\d+[.,]\d+)', full_text)
                if match:
                    rating_text = match.group(1).replace(',', '.')
            except:
                pass
            
            # Method 2: Standard selectors (fallback)
            if not rating_text:
                rating_text = self._extract_text('div.F7nice span[aria-hidden="true"], span.ceNzKf[aria-hidden="true"]')
            
            rating = parse_rating(rating_text)
            
            # Extract reviews count
            reviews_count = None
            reviews_text = None
            
            try:
                rating_section = self.driver.find_element(By.CSS_SELECTOR, 'div.F7nice')
                full_text = rating_section.text
                match = re.search(r'\(([0-9.,\s]+)\)', full_text)
                if match:
                    reviews_text = match.group(1)
            except:
                pass
            
            reviews_count = parse_reviews_count(reviews_text)
            
            # Extract phone
            phone = self._extract_text('button[data-item-id*="phone"]')
            if not phone:
                try:
                    phone_elem = self.driver.find_element(By.CSS_SELECTOR, 'button[data-item-id*="phone"]')
                    aria = phone_elem.get_attribute('aria-label')
                    if aria and ':' in aria:
                        phone = aria.split(':', 1)[1].strip()
                except:
                    pass
            
            # Extract website
            website = self._extract_attribute('a[data-item-id="authority"]', 'href')
            
            # Extract opening hours
            opening_hours = self._extract_opening_hours()
            
            # Extract stars
            stars = self._extract_star_distribution()
            
            # Create Place
            place = Place(
                name=clean_text(name),
                category=clean_text(category),
                address=clean_text(address),
                subdistrict=clean_text(subdistrict),
                district=clean_text(district),
                city=clean_text(city),
                province=clean_text(province),
                zip_code=clean_text(zip_code),
                latitude=latitude,
                longitude=longitude,
                rating=rating,
                reviews_count=reviews_count,
                phone=clean_text(phone),
                website=clean_text(website),
                google_maps_link=link,
                opening_hours=clean_text(opening_hours),
                star_1=stars.get(1),
                star_2=stars.get(2),
                star_3=stars.get(3),
                star_4=stars.get(4),
                star_5=stars.get(5),
                search_keyword=task.keyword,
                search_location=task.location
            )
            
            return place
            
        except Exception as e:
            print(f"  [{idx+1}/{total}] ❌ Extract error: {e}")
            return None
    
    def _extract_text(self, selector: str) -> Optional[str]:
        """Extract text trying multiple selectors"""
        selectors = [s.strip() for s in selector.split(',')]
        
        for sel in selectors:
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, sel)
                text = element.text
                if text and text.strip():
                    return text.strip()
            except:
                continue
        
        return None
    
    def _extract_attribute(self, selector: str, attribute: str) -> Optional[str]:
        """Extract attribute from element"""
        try:
            element = self.driver.find_element(By.CSS_SELECTOR, selector)
            return element.get_attribute(attribute)
        except:
            return None
    
    def _extract_opening_hours(self) -> Optional[str]:
        """Extract opening hours"""
        try:
            hours = self.driver.find_element(By.CSS_SELECTOR, 'button[data-item-id*="oh"]')
            return hours.text
        except:
            return None
    
    def _extract_star_distribution(self) -> dict:
        """Extract star distribution"""
        stars = {1: None, 2: None, 3: None, 4: None, 5: None}
        
        try:
            bars = self.driver.find_elements(By.CSS_SELECTOR, 'tr.BHOKXe')
            
            for idx, bar in enumerate(bars[:5]):
                try:
                    import re
                    match = re.search(r'(\d+)', bar.text)
                    if match:
                        stars[5 - idx] = int(match.group(1))
                except:
                    continue
                    
        except:
            pass
        
        return stars
    
    def _is_valid_place(self, place: Place) -> bool:
        """Check if place is valid"""
        return (
            place.name is not None and 
            place.google_maps_link is not None and
            len(place.name) > 0
        )