"""
Search engine for Google Maps
"""
import time
import random
from typing import List, Set, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

from ..models.place import SearchTask, Place
from ..config.settings import ScraperConfig
from ..core.driver_manager import DriverManager
from ..utils.extractors import (
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
        place_elements = self._scroll_and_collect_elements(task.max_results)
        
        print(f"Found {len(place_elements)} place elements")
        
        # Extract details from each place
        for idx, element in enumerate(place_elements):
            try:
                place = self._extract_place_details(element, task)
                
                if place and self._is_valid_place(place):
                    # Check for duplicates
                    if place.google_maps_link not in self.seen_links:
                        self.seen_links.add(place.google_maps_link)
                        places.append(place)
                        print(f"  [{idx+1}/{len(place_elements)}] ✓ {place.name}")
                    else:
                        print(f"  [{idx+1}/{len(place_elements)}] ✗ Duplicate: {place.name}")
                
                # Random delay to appear human-like
                time.sleep(random.uniform(self.config.min_delay, self.config.max_delay))
                
            except Exception as e:
                print(f"  [{idx+1}/{len(place_elements)}] Error extracting place: {e}")
                continue
        
        print(f"Collected {len(places)} unique places for: {task}")
        return places
    
    def _perform_search(self, query: str, max_retries: int = 3) -> bool:
        """Perform search with retry mechanism"""
        for attempt in range(max_retries):
            try:
                print(f"  Search attempt {attempt + 1}/{max_retries}")
                
                # Reset to home
                if not self.driver_manager.reset_to_maps_home():
                    continue
                
                # Find search box
                search_box = WebDriverWait(self.driver, self.config.element_wait_timeout).until(
                    EC.presence_of_element_located((
                        By.CSS_SELECTOR,
                        'input[aria-label="Search Google Maps"], input[id="searchboxinput"]'
                    ))
                )
                
                # Enter query
                search_box.clear()
                time.sleep(0.5)
                search_box.send_keys(query)
                time.sleep(1)
                search_box.send_keys(Keys.RETURN)
                
                # Wait for results
                time.sleep(3)
                
                # Check if results loaded
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((
                            By.CSS_SELECTOR,
                            'div[role="feed"], div.m6QErb'
                        ))
                    )
                    return True
                except TimeoutException:
                    print(f"  Results not loaded on attempt {attempt + 1}")
                    
            except Exception as e:
                print(f"  Search attempt {attempt + 1} failed: {e}")
                time.sleep(2)
        
        return False
    
    def _scroll_and_collect_elements(self, max_results: int) -> List:
        """Scroll results panel and collect place elements"""
        place_elements = []
        
        try:
            # Try multiple selectors for the results panel
            results_panel = None
            panel_selectors = [
                'div[role="feed"]',
                'div.m6QErb',
                'div[aria-label*="Results"]',
                'div.section-layout',
                '[role="main"] div[tabindex]'
            ]
            
            for selector in panel_selectors:
                try:
                    results_panel = self.driver.find_element(By.CSS_SELECTOR, selector)
                    print(f"  Found results panel with selector: {selector}")
                    break
                except:
                    continue
            
            if not results_panel:
                print("  Could not find results panel")
                # Try to get all link elements as fallback
                try:
                    place_elements = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/maps/place/"]')
                    print(f"  Found {len(place_elements)} place links as fallback")
                    return place_elements[:max_results]
                except:
                    return []
            
            last_height = 0
            scroll_attempts = 0
            no_change_count = 0
            
            while len(place_elements) < max_results and scroll_attempts < self.config.max_scroll_attempts:
                # Try multiple selectors for place elements
                current_elements = []
                element_selectors = [
                    'a[href*="/maps/place/"]',
                    'div.Nv2PK',
                    'a.hfpxzc',
                    'div[role="article"]',
                    'div[jsaction*="mouseover"]'
                ]
                
                for selector in element_selectors:
                    try:
                        found = results_panel.find_elements(By.CSS_SELECTOR, selector)
                        if len(found) > len(current_elements):
                            current_elements = found
                    except:
                        continue
                
                if len(current_elements) > len(place_elements):
                    place_elements = current_elements
                    no_change_count = 0
                    print(f"  Scrolled {scroll_attempts} times, found {len(place_elements)} elements")
                else:
                    no_change_count += 1
                
                # Stop if no new elements after multiple scrolls
                if no_change_count >= 3:
                    print(f"  No new results after {no_change_count} scrolls")
                    break
                
                # Scroll down
                self.driver.execute_script(
                    "arguments[0].scrollTop = arguments[0].scrollHeight",
                    results_panel
                )
                
                time.sleep(self.config.scroll_pause_time)
                scroll_attempts += 1
                
                # Check if reached end
                current_height = self.driver.execute_script(
                    "return arguments[0].scrollHeight",
                    results_panel
                )
                
                if current_height == last_height:
                    no_change_count += 1
                
                last_height = current_height
            
        except Exception as e:
            print(f"Error during scrolling: {e}")
            import traceback
            traceback.print_exc()
        
        return place_elements[:max_results]
    
    def _extract_place_details(self, element, task: SearchTask) -> Optional[Place]:
        """Extract detailed information from a place element"""
        try:
            # Click on the element to open details
            try:
                self.driver.execute_script("arguments[0].click();", element)
            except:
                # Try regular click as fallback
                element.click()
            
            time.sleep(2)
            
            # Extract basic info with multiple selector attempts
            name = self._extract_text_multi([
                'h1.DUwDvf',
                'h1.fontHeadlineLarge',
                'h1[class*="title"]',
                'h1'
            ])
            
            if not name:
                print("    Warning: Could not extract name")
                return None
            
            category = self._extract_text_multi([
                'button.DkEaL',
                'button[jsaction*="category"]',
                'div.LBgpqf button'
            ])
            
            # Extract address with multiple attempts
            address = self._extract_text_multi([
                'button[data-item-id="address"]',
                'div.rogA2c',
                'button[data-tooltip*="Copy address"]',
                'div[aria-label*="Address"]'
            ])
            
            district, city, province, zip_code = parse_address(address) if address else (None, None, None, None)
            
            # Extract link (from current URL)
            link = self.driver.current_url
            
            # Extract coordinates
            latitude, longitude = extract_coordinates_from_link(link)
            
            # Extract rating and reviews with multiple attempts
            rating_text = self._extract_text_multi([
                'div.F7nice span[aria-hidden="true"]',
                'span.ceNzKf',
                'div[jsaction*="rating"] span'
            ])
            rating = parse_rating(rating_text)
            
            reviews_text = self._extract_text_multi([
                'div.F7nice span[aria-label*="reviews"]',
                'button[aria-label*="reviews"]',
                'span[aria-label*="reviews"]'
            ])
            reviews_count = parse_reviews_count(reviews_text)
            
            # Extract contact info with multiple attempts
            phone = self._extract_text_multi([
                'button[data-item-id="phone:tel:"]',
                'button[data-tooltip*="phone"]',
                'button[aria-label*="Phone"]',
                'a[href^="tel:"]'
            ])
            
            website = self._extract_attribute('a[data-item-id="authority"]', 'href')
            if not website:
                website = self._extract_attribute('a[aria-label*="Website"]', 'href')
            
            # Extract opening hours
            opening_hours = self._extract_opening_hours()
            
            # Extract star distribution
            stars = self._extract_star_distribution()
            
            # Create Place object
            place = Place(
                name=clean_text(name),
                category=clean_text(category),
                address=clean_text(address),
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
            print(f"Error extracting place details: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _extract_text_multi(self, selectors: list) -> Optional[str]:
        """Try multiple selectors to extract text"""
        for selector in selectors:
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, selector)
                text = element.text
                if text:
                    return text
            except:
                continue
        return None
    
    def _extract_text(self, selector: str) -> Optional[str]:
        """Extract text from element by CSS selector"""
        try:
            element = self.driver.find_element(By.CSS_SELECTOR, selector)
            return element.text
        except:
            return None
    
    def _extract_attribute(self, selector: str, attribute: str) -> Optional[str]:
        """Extract attribute from element"""
        try:
            element = self.driver.find_element(By.CSS_SELECTOR, selector)
            return element.get_attribute(attribute)
        except:
            return None
    
    def _extract_opening_hours(self) -> Optional[str]:
        """Extract opening hours information"""
        try:
            # Try to find opening hours button and click it
            hours_button = self.driver.find_element(
                By.CSS_SELECTOR,
                'button[data-item-id*="oh"], button[aria-label*="hours"]'
            )
            
            # Get text without clicking (to avoid expanding)
            return hours_button.text
        except:
            return None
    
    def _extract_star_distribution(self) -> dict:
        """Extract star rating distribution (1-5 stars)"""
        stars = {1: None, 2: None, 3: None, 4: None, 5: None}
        
        try:
            # Find all rating bars
            rating_bars = self.driver.find_elements(
                By.CSS_SELECTOR,
                'tr.BHOKXe, table.RURq4 tr'
            )
            
            for idx, bar in enumerate(rating_bars[:5]):
                try:
                    # Extract the count
                    count_text = bar.text
                    # Try to extract number from text
                    import re
                    match = re.search(r'(\d+)', count_text)
                    if match:
                        stars[5 - idx] = int(match.group(1))
                except:
                    continue
                    
        except:
            pass
        
        return stars
    
    def _is_valid_place(self, place: Place) -> bool:
        """Check if place has minimum required information"""
        return (
            place.name is not None and 
            place.google_maps_link is not None and
            len(place.name) > 0
        )