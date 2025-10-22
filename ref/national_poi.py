# Perlu ditambah scroll sampe end of page

import time
import re
import pandas as pd
import json
import signal
import os
import sys
import random
import math
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import threading
from queue import Queue
import psutil

if __name__ == '__main__':
    mp.set_start_method('spawn', force=True)
    
# Global data dictionary to track progress
global_data = {
    'names': [],
    'categories': [],
    'addresses': [],
    'provinces': [],
    'links': [],
    'ratings': [],
    'reviews_count': [],
    'latitudes': [],
    'longitudes': [],
    'zip_codes': [],
    'cities': [],
    'districts': [],
    'subdistricts': [],
    'phones': [],
    'websites': [],
    'open_hours': [],
    'stars': [],
    'num_reviews': [],
    'latest_reviews': [],
    'processed_elements': set(),
    'seen_name_pairs': set(),
    'search_query': "",
    'coordinates_searched': set(),
    'target_area': "",
    'last_coords': None,
    'poi_index': 0,
    'poi_categories': [

        # Infrastructure & Transportation
        "Toll gate", "Gerbang tol", "Bus station", "Train station", "Airport",
        "Subway station", "LRT station", "MRT station", "TransJakarta stop", "Terminal bus",
        "Parking lot", "Gas station", "SPBU", "Electric charging station", "Port", "Harbor",
        "Logistics center", "Distribution center", "Trucking company", "Freight terminal",
        
        # Residential Areas
        "Housing complex", "Housing estate", "Perumahan", "Gated community",
        "Housing authority", "Apartment building", "Residential area",
        "Real estate developer", "Rumah susun", "Condominium", "Townhouse",
        "Cluster housing", "Property agent", "Real estate office", "Serviced apartment",
        "Student housing", "Senior living", "Affordable housing",
        
        # Food & Beverage
        "Restaurant", "Cafe", "Coffee shop", "Food court", "Food producer",
        "Seafood restaurant", "Chicken restaurant", "Dessert shop", "Bakery",
        "Fast food restaurant", "Food stall", "Warung", "Rumah makan",
        "Street food", "Food truck", "Catering service", "Ice cream shop",
        "Juice bar", "Tea house", "BBQ restaurant", "Pizza place", "Noodle shop",
        "Padang restaurant", "Sunda restaurant", "Chinese restaurant", "Japanese restaurant",
        "Korean restaurant", "Western restaurant", "Vegetarian restaurant", "Halal restaurant",
        
        # Education
        "University", "College", "School", "Elementary school", "Middle school",
        "High school", "Senior high school", "Private school", "Vocational school",
        "International school", "Distance learning center", "Religious school",
        "Boarding school", "Islamic boarding school", "Pesantren",
        "Kindergarten", "Preschool", "PAUD", "TK", "SD", "SMP", "SMA", "SMK",
        "Language school", "Training center", "Tutoring center", "Bimbel",
        "Computer course", "Driving school", "Art school", "Music school"
        
        # Business & Office
        "Office building", "Corporate office", "Business park", "Industrial area",
        "Factory", "Warehouse", "Tech park", "Coworking space", "Registry office",
        "Convention center", "Trade center", "UMKM", "Manufacturing plant",
        "Business center", "Startup incubator", "Industrial estate", "Free trade zone",
        "Export company", "Import company", "Trading company", "Consulting firm",
        "Accounting office", "Law firm", "Advertising agency", "IT company",
        
        # Healthcare
        "Hospital", "Medical center", "Clinic", "Health center", "Dentist",
        "Pharmacy", "Government hospital", "Private hospital", "Medical laboratory",
        "Puskesmas", "Posyandu", "Mental health clinic", "Rehabilitation center",
        "Optical shop", "Medical equipment", "Veterinary clinic", "Animal hospital",
        "Traditional medicine", "Herbal medicine", "Physiotherapy", "Dialysis center",
        
        # Retail & Shopping
        "Shopping mall", "Department store", "Supermarket", "Hypermarket",
        "Convenience store", "Market", "Traditional market", "Pasar", "Retail store",
        "Minimarket", "Indomaret", "Alfamart", "Electronics store", "Furniture store",
        "Clothing store", "Shoe store", "Jewelry store", "Mobile phone shop",
        "Computer store", "Bookstore", "Stationery store", "Hardware store",
        "Auto parts store", "Motorcycle dealer", "Car dealer",
        
        # Leisure & Recreation
        "Park", "Garden", "Tourist attraction", "Sports complex", "Stadium",
        "Badminton court", "Soccer field", "Public swimming pool", "Recreation area",
        "Movie theater", "Mall", "Theme park", "Zoo", "Museum", "Art gallery",
        "Bowling alley", "Billiards", "Karaoke", "Spa", "Massage", "Salon",
        "Barbershop", "Fitness center", "Gym", "Yoga studio", "Golf course",
        "Water park", "Beach", "Lake", "Camping ground",
        
        # Travel & Accommodation
        "Hotel", "Resort", "Motel", "Lodging", "Guest house", "Travel agency",
        "Tour operator", "Homestay", "Villa", "Hostel", "Budget hotel",
        "Boutique hotel", "Business hotel", "Airport hotel", "Rental car",
        "Motorcycle rental", "Bus rental", "Tour guide", "Travel insurance",
        
        # Religious Facilities
        "Mosque", "Church", "Temple", "Masjid", "Gereja", "Vihara", "Pura",
        "Surau", "Langgar", "Cathedral", "Monastery", "Convent", "Prayer hall",
        "Religious center", "Islamic center", "Christian center",
        
        # Financial Services
        "Bank branch", "ATM", "Pegadaian", "Pawnshop", "Insurance agency",
        "Money changer", "Cooperative", "Koperasi", "Credit union",
        "Microfinance", "Investment firm", "Securities company", "Fintech",
        "Mobile banking", "Digital payment", "Remittance service",
        
        # Government & Public Services
        "Government office", "Police station", "Fire station", "Post office",
        "Tax office", "Public service", "City hall", "District office", "Kantor lurah",
        "Kantor camat", "Kantor desa", "Kelurahan", "Kecamatan", "Immigration office",
        "Customs office", "Court house", "Prosecutor office", "Military base",
        "Embassy", "Consulate", "Notary public", "Land office", "BPN",

        # Utilities & Infrastructure Services
        "Water treatment plant", "Power plant", "Telecommunication tower",
        "Internet provider", "Cable TV", "Waste management", "Recycling center",
        "Water company", "Electric company", "Phone company",

        # Automotive Services
        "Car wash", "Auto repair", "Tire shop", "Car rental", "Motorcycle repair",
        "Oil change", "Car insurance", "Vehicle inspection", "Driving test center",

        # Professional Services
        "Photography studio", "Printing shop", "Copy center", "Event organizer",
        "Wedding planner", "Security service", "Cleaning service", "Delivery service",
        "Courier", "Laundry", "Dry cleaning", "Tailoring", "Repair service",

        # Agriculture & Food Production
        "Farm", "Plantation", "Fish farm", "Livestock", "Food processing",
        "Rice mill", "Feed mill", "Agricultural supply", "Fertilizer store",
        "Seed store", "Farmer market"

    ]
}

def initialize_driver(headless=False, driver_path=None, proxy=None, worker_id=None):
    """Initialize and configure the Chrome WebDriver with crash prevention"""
    chrome_options = Options()
    
    # Essential stability options
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")
    
    # Memory and performance optimization
    chrome_options.add_argument("--memory-pressure-off")
    chrome_options.add_argument("--max_old_space_size=4096")
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    
    # Prevent crashes
    chrome_options.add_argument("--disable-crash-reporter")
    chrome_options.add_argument("--disable-error-reporting")
    chrome_options.add_argument("--disable-logging")
    chrome_options.add_argument("--log-level=3")
    
    # Unique user data directory for each worker
    if worker_id is not None:
        temp_dir = os.path.join(os.getcwd(), f"temp_browser_data_worker_{worker_id}")
    else:
        temp_dir = os.path.join(os.getcwd(), f"temp_browser_data_{random.randint(1000, 9999)}")
    
    try:
        os.makedirs(temp_dir, exist_ok=True)
        chrome_options.add_argument(f"--user-data-dir={temp_dir}")
    except Exception as e:
        print(f"Warning: Could not create custom cache directory: {e}")
    
    # Set window size
    chrome_options.add_argument("--window-size=1280,800")
    
    # Other existing options
    chrome_options.add_argument("--disable-application-cache")
    chrome_options.add_argument("--disk-cache-size=0")
    chrome_options.add_argument("--media-cache-size=0")
    chrome_options.add_argument("--lang=id")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-infobars")
    
    if headless:
        chrome_options.add_argument("--headless=new")  # Use new headless mode
        chrome_options.add_argument("--disable-gpu")
        
    if proxy:
        chrome_options.add_argument(f'--proxy-server={proxy}')
    
    # Add remote debugging port to avoid conflicts
    debug_port = 9222 + (worker_id if worker_id else random.randint(1, 100))
    chrome_options.add_argument(f"--remote-debugging-port={debug_port}")
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if driver_path:
                service = Service(driver_path)
                driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                driver = webdriver.Chrome(options=chrome_options)
            
            driver.set_page_load_timeout(30)
            driver.delete_all_cookies()
            
            # Test if driver is working
            driver.get("about:blank")
            
            return driver
            
        except Exception as e:
            print(f"Worker {worker_id}: Chrome startup attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
                # Try different debug port
                debug_port = 9222 + random.randint(1, 1000)
                chrome_options.add_argument(f"--remote-debugging-port={debug_port}")
            else:
                raise Exception(f"Failed to start Chrome after {max_retries} attempts")
    
    return None

def reset_to_maps_home(driver):
    """Reset browser to Google Maps homepage and clear any selections"""
    try:
        print("Resetting to Google Maps homepage...")
        
        # Navigate to Google Maps homepage
        driver.get('https://maps.google.com')
        time.sleep(2)
        
        # Clear search box if it exists
        try:
            search_box = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'input[aria-label="Search Google Maps"], input[id="searchboxinput"]'))
            )
            search_box.clear()
            search_box.send_keys("")
            time.sleep(1)
        except TimeoutException:
            pass
        
        # Try to close any open panels or place details
        try:
            # Close place details panel
            close_buttons = driver.find_elements(By.CSS_SELECTOR, 'button[aria-label="Close"], button[aria-label="Back"], .VfPpkd-icon-LgbsSe[aria-label="Back"]')
            for button in close_buttons:
                try:
                    driver.execute_script("arguments[0].click();", button)
                    time.sleep(0.5)
                except:
                    pass
        except:
            pass
        
        # Additional cleanup - press Escape key to close any dialogs
        try:
            from selenium.webdriver.common.action_chains import ActionChains
            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(1)
        except:
            pass
        
        print("Reset to Google Maps homepage completed")
        return True
        
    except Exception as e:
        print(f"Error resetting to maps home: {e}")
        return False

def perform_search_with_retry(driver, search_query, max_retries=3):
    """Perform search with retry mechanism and proper error handling"""
    for attempt in range(max_retries):
        try:
            print(f"Search attempt {attempt + 1}/{max_retries} for: {search_query}")
            
            # Reset to home page before new search
            if not reset_to_maps_home(driver):
                print("Failed to reset to home page")
                time.sleep(2)
            
            # Wait for page to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'input[aria-label="Search Google Maps"], input[id="searchboxinput"]'))
            )
            
            # Find and clear search box
            search_box = driver.find_element(By.CSS_SELECTOR, 'input[aria-label="Search Google Maps"], input[id="searchboxinput"]')
            search_box.clear()
            time.sleep(0.5)
            
            # Type search query
            search_box.send_keys(search_query)
            time.sleep(1)
            
            # Press Enter
            search_box.send_keys(Keys.RETURN)
            
            # Wait for results
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, '//div[@role="feed"]'))
            )
            
            # Additional wait for results to load
            time.sleep(3)
            
            # Verify we have search results
            results = driver.find_elements(By.CSS_SELECTOR, 'a.hfpxzc')
            if len(results) > 0:
                print(f"Search successful! Found {len(results)} initial results")
                return True
            else:
                print("No results found, retrying...")
                
        except TimeoutException:
            print(f"Timeout on attempt {attempt + 1}")
            time.sleep(2)
        except Exception as e:
            print(f"Error on attempt {attempt + 1}: {e}")
            time.sleep(2)
    
    print(f"All {max_retries} search attempts failed for: {search_query}")
    return False

def clean_address(address):
    """Clean up address text"""
    if not address:
        return "Address not available"
    
    # Remove Google Plus codes
    address = re.sub(r'\b\w+\+\w+\b,?\s*', '', address)
    
    # Remove status and hours
    if 'Buka' in address:
        address = re.sub(r'Buka.*?(?=,|$)', '', address)
    if 'Tutup' in address:
        address = re.sub(r'Tutup.*?(?=,|$)', '', address)
    
    # Clean up
    address = address.replace('"', '').replace('Â', '').replace('î¤´', '')
    address = address.replace('â‹…', ' ').replace('·', ' ')
    address = ' '.join(address.split())
    
    # Remove multiple commas
    while ',,' in address:
        address = address.replace(',,', ',')
    
    # Final cleanup
    address = address.strip(' ,')
    parts = address.split(',')
    cleaned_parts = []
    seen = set()
    for part in parts:
        part = part.strip()
        if part and part not in seen and len(part) > 1:
            cleaned_parts.append(part)
            seen.add(part)
    
    return ', '.join(cleaned_parts)

def extract_province_from_address(address):
    """Extract province information from address with comprehensive context awareness for all of Indonesia"""
    if not address or address == "Address not available":
        return "Unknown"
    
    # Define provinces list at the beginning
    provinces = [
        "Kepulauan Bangka Belitung", "Daerah Khusus Ibukota Jakarta", "Daerah Istimewa Yogyakarta",
        "Nusa Tenggara Timur", "Nusa Tenggara Barat", "Kalimantan Tengah", "Kalimantan Selatan",
        "Kalimantan Timur", "Kalimantan Barat", "Kalimantan Utara", "Sulawesi Tenggara",
        "Sulawesi Selatan", "Sulawesi Tengah", "Sulawesi Barat", "Sulawesi Utara",
        "Sumatera Selatan", "Sumatera Barat", "Sumatera Utara", "Kepulauan Riau",
        "Papua Pegunungan", "Papua Selatan", "Papua Tengah", "Maluku Utara",
        "Jawa Tengah", "Jawa Timur", "Jawa Barat", "Gorontalo", "DKI Jakarta",
        "DI Yogyakarta", "Papua Barat",  "Lampung",  
        "Bengkulu", "Banten", "Jambi", "Maluku", "Papua", "Bali", "Riau", "Aceh"
    ]
    
    # Since Malang is in East Java, we'll set default province
    if "Malang" in address:
        return "Jawa Timur"
    
    # Create a copy of the address for special pattern handling
    address_for_pattern_check = " " + address + " "
    
    # STEP 1: Check for explicit province mentions with strong contextual indicators
    explicit_patterns = [
        r'Provinsi\s+([A-Za-z\s]+)',
        r'Prov[.]\s+([A-Za-z\s]+)',
        r'Daerah\s+Khusus\s+([A-Za-z\s]+)',
        r'Daerah\s+Istimewa\s+([A-Za-z\s]+)',
        r'D[.]I[.]\s+([A-Za-z\s]+)',
        r'DI\s+([A-Za-z\s]+)',
        r'D[.]K[.]I[.]\s+([A-Za-z\s]+)',
        r'DKI\s+([A-Za-z\s]+)',
        r'[,]\s*([A-Za-z\s]+)\s+\d{5}$'  # Province before postal code at end of address
    ]
    
    for pattern in explicit_patterns:
        match = re.search(pattern, address)
        if match:
            province_part = match.group(1).strip()
            # Check if this contains a known province
            for province in provinces:
                if province.lower() in province_part.lower():
                    return province
    
    # STEP 2: Check postal codes - a reliable indicator
    postal_match = re.search(r'\b\d{5}\b', address)
    if postal_match:
        postal_code = postal_match.group(0)
        prefix = postal_code[:2]
        
        # Expanded postal code mapping covering more of Indonesia
        postal_map = {
            # Jakarta
            '10': 'DKI Jakarta', '11': 'DKI Jakarta', '12': 'DKI Jakarta',
            '13': 'DKI Jakarta', '14': 'DKI Jakarta',
            # West Java
            '15': 'Jawa Barat', '16': 'Jawa Barat', '17': 'Jawa Barat',
            '40': 'Jawa Barat', '41': 'Jawa Barat', '42': 'Jawa Barat',
            '43': 'Jawa Barat', '44': 'Jawa Barat', '45': 'Jawa Barat',
            '46': 'Jawa Barat', '47': 'Jawa Barat',
            # Central Java
            '50': 'Jawa Tengah', '51': 'Jawa Tengah', '52': 'Jawa Tengah',
            '53': 'Jawa Tengah', '54': 'Jawa Tengah',
            # Yogyakarta
            '55': 'DI Yogyakarta',
            # East Java
            '60': 'Jawa Timur', '61': 'Jawa Timur', '62': 'Jawa Timur',
            '63': 'Jawa Timur', '64': 'Jawa Timur', '65': 'Jawa Timur',
            '66': 'Jawa Timur', '67': 'Jawa Timur', '68': 'Jawa Timur',
            '69': 'Jawa Timur',
            # Bali
            '80': 'Bali', '81': 'Bali', '82': 'Bali',
            # NTB and NTT
            '83': 'Nusa Tenggara Barat', '84': 'Nusa Tenggara Timur',
            # Papua
            '98': 'Papua', '99': 'Papua',
        }
        
        if prefix in postal_map:
            return postal_map[prefix]
    
    # For Malang area, default to East Java
    if "Malang" in address:
        return "Jawa Timur"
    
    # Sort provinces by length for the best matching (longest first)
    sorted_provinces = sorted(provinces, key=len, reverse=True)
    
    # Check province mentions at the end of the address - these are more likely to be the actual province
    for province in sorted_provinces:
        # Check if province is at the end of the address (more reliable)
        end_pattern = r',\s*' + re.escape(province) + r'(?:\s*\d{5})?$'
        if re.search(end_pattern, address, re.IGNORECASE):
            return province
    
    # Then check for province mentions elsewhere in the address with word boundaries
    for province in sorted_provinces:
        if re.search(r'\b' + re.escape(province) + r'\b', address, re.IGNORECASE):
            return province
    
    # If nothing else works, for Malang area assume East Java
    return "Jawa Timur"

def get_category(driver):
    """Extract the category of the place"""
    try:
        js_code = """
        try {
            // Different possible category selectors
            const categorySelectors = [
                'button[jsaction*="pane.rating.category"]',
                'button.DkEaL',
                'button[jsaction*="pane.herocard.category"]',
                'span.mgr77e'
            ];
            
            for (const selector of categorySelectors) {
                const elements = document.querySelectorAll(selector);
                for (let i = 0; i < elements.length; i++) {
                    const text = elements[i].textContent || elements[i].innerText;
                    if (text && text.trim().length > 1 && text.trim() !== 'Save' && 
                        text.trim() !== 'Directions' && text.trim() !== 'Website' && 
                        text.trim() !== 'Phone') {
                        return text.trim();
                    }
                }
            }
            
            return "Category not available";
        } catch (e) {
            console.error("Error finding category:", e);
            return "Category not available";
        }
        """
        
        category = driver.execute_script(js_code)
        if category and category != "Category not available":
            return category
        
        # Fallback to selenium
        category_selectors = [
            "button.DkEaL",
            "button[jsaction*='pane.rating.category']",
            "button[jsaction*='pane.herocard.category']",
            "span.mgr77e"
        ]
        
        for selector in category_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if text and len(text) > 1 and text not in ['Save', 'Directions', 'Website', 'Phone']:
                        return text
            except Exception:
                continue

    except Exception as e:
        print(f"Error extracting category: {e}")
    
    return "Category not available"

def get_rating_and_reviews(driver):
    """Extract rating and number of reviews"""
    try:
        js_code = """
        try {
            // Different possible rating selectors
            const ratingSelectors = [
                'span.fontBodyMedium div.F7nice span',
                'div.F7nice span',
                'span[aria-hidden="true"]'
            ];
            
            for (const selector of ratingSelectors) {
                const elements = document.querySelectorAll(selector);
                for (let i = 0; i < elements.length; i++) {
                    const text = elements[i].textContent || elements[i].innerText;
                    if (text && /^[1-5](\.[0-9])?$/.test(text.trim())) {
                        // If we found the rating, look for review count nearby
                        const reviewSelector = 'span.fontBodyMedium div.F7nice span:not(:first-child)';
                        const reviewElements = document.querySelectorAll(reviewSelector);
                        for (let j = 0; j < reviewElements.length; j++) {
                            const reviewText = reviewElements[j].textContent || reviewElements[j].innerText;
                            // Extract numbers from the review text
                            const reviewMatch = reviewText.match(/([\d,]+)/);
                            if (reviewMatch) {
                                return {
                                    rating: parseFloat(text.trim()),
                                    reviews: reviewMatch[1].replace(',', '')
                                };
                            }
                        }
                        return {
                            rating: parseFloat(text.trim()),
                            reviews: "0"
                        };
                    }
                }
            }
            
            return {
                rating: 0,
                reviews: "0"
            };
        } catch (e) {
            console.error("Error finding rating and reviews:", e);
            return {
                rating: 0,
                reviews: "0"
            };
        }
        """
        
        result = driver.execute_script(js_code)
        return result.get('rating', 0), result.get('reviews', "0")
    
    except Exception as e:
        print(f"Error extracting rating and reviews: {e}")
        return 0, "0"

def extract_coordinates_from_url(url):
    """Extract latitude and longitude from Google Maps URL"""
    try:
        # Pattern to match coordinates in Google Maps URLs
        lat_pattern = r'!3d(-?\d+\.\d+)'
        lng_pattern = r'!4d(-?\d+\.\d+)'
        
        lat_match = re.search(lat_pattern, url)
        lng_match = re.search(lng_pattern, url)
        
        if lat_match and lng_match:
            return float(lat_match.group(1)), float(lng_match.group(1))
        
        # Alternative pattern for @coordinates
        coord_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', url)
        if coord_match:
            return float(coord_match.group(1)), float(coord_match.group(2))
            
    except Exception as e:
        print(f"Error extracting coordinates: {e}")
    
    return None, None

def extract_location_details(address):
    """Extract ZIP code, city, district, and subdistrict from address"""
    if not address or address == "Address not available":
        return None, None, None, None
    
    # Extract ZIP code
    zip_match = re.search(r'\b(\d{5})\b', address)
    zip_code = zip_match.group(1) if zip_match else None
    
    # Split address into parts
    parts = [part.strip() for part in address.split(',')]
    
    # Initialize variables
    city = None
    district = None
    subdistrict = None
    
    # Indonesian city keywords
    city_keywords = ['Kota', 'Kabupaten', 'Jakarta', 'Surabaya', 'Bandung', 'Malang', 'Yogyakarta']
    
    # Look for city, district, subdistrict
    for i, part in enumerate(parts):
        part_lower = part.lower()
        
        # Check for city
        if any(keyword.lower() in part_lower for keyword in city_keywords):
            city = part
        
        # Check for Kecamatan (district)
        if 'kec' in part_lower or 'kecamatan' in part_lower:
            district = part.replace('Kec.', '').replace('Kecamatan', '').strip()
        
        # Check for Kelurahan/Desa (subdistrict)
        if any(keyword in part_lower for keyword in ['kel', 'kelurahan', 'desa']):
            subdistrict = part.replace('Kel.', '').replace('Kelurahan', '').replace('Desa', '').strip()
    
    # If city not found explicitly, try to infer from context
    if not city and len(parts) > 1:
        # Often the city is in the last parts
        for part in reversed(parts[-3:]):
            if len(part) > 3 and not any(char.isdigit() for char in part):
                city = part
                break
    
    return zip_code, city, district, subdistrict

def get_phone_number(driver):
    """Extract phone number from Google Maps"""
    try:
        js_code = """
        try {
            const phoneSelectors = [
                'button[data-item-id="phone:tel:"]',
                'button[aria-label*="phone"]',
                'button[aria-label*="Phone"]',
                'button[aria-label*="Telepon"]',
                'span[aria-label*="Phone"]'
            ];
            
            for (const selector of phoneSelectors) {
                const elements = document.querySelectorAll(selector);
                for (let i = 0; i < elements.length; i++) {
                    const text = elements[i].textContent || elements[i].innerText;
                    const ariaLabel = elements[i].getAttribute('aria-label') || '';
                    
                    // Look for phone patterns
                    const phonePattern = /(\+62|0)[0-9\s\-\(\)]{8,}/;
                    if (phonePattern.test(text)) {
                        return text.trim();
                    }
                    if (phonePattern.test(ariaLabel)) {
                        const match = ariaLabel.match(phonePattern);
                        return match ? match[0].trim() : null;
                    }
                }
            }
            return null;
        } catch (e) {
            console.error("Error finding phone:", e);
            return null;
        }
        """
        
        phone = driver.execute_script(js_code)
        return phone if phone else "Phone not available"
        
    except Exception as e:
        print(f"Error extracting phone: {e}")
        return "Phone not available"

def get_website(driver):
    """Extract website URL from Google Maps"""
    try:
        js_code = """
        try {
            const websiteSelectors = [
                'a[data-item-id="authority"]',
                'a[aria-label*="Website"]',
                'a[aria-label*="website"]',
                'a[href^="http"]:not([href*="maps.google"])',
                'button[data-item-id="authority"]'
            ];
            
            for (const selector of websiteSelectors) {
                const elements = document.querySelectorAll(selector);
                for (let i = 0; i < elements.length; i++) {
                    const href = elements[i].getAttribute('href');
                    const ariaLabel = elements[i].getAttribute('aria-label') || '';
                    
                    if (href && href.startsWith('http') && !href.includes('maps.google')) {
                        return href;
                    }
                    
                    if (ariaLabel.toLowerCase().includes('website')) {
                        const text = elements[i].textContent || elements[i].innerText;
                        if (text && (text.includes('http') || text.includes('www'))) {
                            return text.trim();
                        }
                    }
                }
            }
            return null;
        } catch (e) {
            console.error("Error finding website:", e);
            return null;
        }
        """
        
        website = driver.execute_script(js_code)
        return website if website else "Website not available"
        
    except Exception as e:
        print(f"Error extracting website: {e}")
        return "Website not available"

def get_open_hours(driver):
    """Extract opening hours from Google Maps"""
    try:
        js_code = """
        try {
            const hourSelectors = [
                'div[aria-label*="Hours"]',
                'div[aria-label*="hours"]',
                'div[data-item-id="oh"]',
                'button[aria-label*="Hours"]'
            ];
            
            for (const selector of hourSelectors) {
                const elements = document.querySelectorAll(selector);
                for (let i = 0; i < elements.length; i++) {
                    const text = elements[i].textContent || elements[i].innerText;
                    const ariaLabel = elements[i].getAttribute('aria-label') || '';
                    
                    if (text && (text.includes(':') || text.toLowerCase().includes('open') || text.toLowerCase().includes('close'))) {
                        return text.trim();
                    }
                    
                    if (ariaLabel && ariaLabel.toLowerCase().includes('hours')) {
                        return ariaLabel;
                    }
                }
            }
            
            // Look for hours in table format
            const tables = document.querySelectorAll('table');
            for (let table of tables) {
                const text = table.textContent || table.innerText;
                // Fixed regex - escaped the dash characters
                if (text.includes(':') && (text.includes('AM') || text.includes('PM') || text.includes('\\u2013') || text.includes('-'))) {
                    return text.trim().replace(/\\n/g, '; ');
                }
            }
            
            return null;
        } catch (e) {
            console.error("Error finding hours:", e);
            return null;
        }
        """
        
        hours = driver.execute_script(js_code)
        return hours if hours else "Hours not available"
        
    except Exception as e:
        print(f"Error extracting hours: {e}")
        return "Hours not available"

def get_detailed_rating_info(driver):
    """Extract detailed rating information including stars and review count"""
    try:
        js_code = """
        try {
            const ratingElements = document.querySelectorAll('span[aria-hidden="true"]');
            let rating = null;
            let reviewCount = null;
            
            for (let element of ratingElements) {
                const text = element.textContent || element.innerText;
                
                // Check for rating (1.0 - 5.0)
                if (/^[1-5](\.[0-9])?$/.test(text.trim())) {
                    rating = parseFloat(text.trim());
                }
                
                // Check for review count
                const reviewMatch = text.match(/([0-9,]+)\s*(review|ulasan)/i);
                if (reviewMatch) {
                    reviewCount = reviewMatch[1].replace(',', '');
                }
            }
            
            // Alternative method for review count
            if (!reviewCount) {
                const reviewElements = document.querySelectorAll('button[aria-label*="review"], button[aria-label*="ulasan"]');
                for (let element of reviewElements) {
                    const ariaLabel = element.getAttribute('aria-label');
                    const match = ariaLabel.match(/([0-9,]+)/);
                    if (match) {
                        reviewCount = match[1].replace(',', '');
                        break;
                    }
                }
            }
            
            return {
                rating: rating || 0,
                reviewCount: reviewCount || "0"
            };
        } catch (e) {
            console.error("Error finding detailed rating:", e);
            return { rating: 0, reviewCount: "0" };
        }
        """
        
        result = driver.execute_script(js_code)
        return result.get('rating', 0), result.get('reviewCount', "0")
        
    except Exception as e:
        print(f"Error extracting detailed rating: {e}")
        return 0, "0"

def get_latest_reviews(driver, limit=5):
    """Extract latest reviews with dates"""
    try:
        js_code = f"""
        try {{
            const reviews = [];
            const reviewElements = document.querySelectorAll('div[data-review-id]');
            
            for (let i = 0; i < Math.min({limit}, reviewElements.length); i++) {{
                const reviewEl = reviewElements[i];
                
                // Get review text
                const reviewTextEl = reviewEl.querySelector('span[data-expandable-section]');
                const reviewText = reviewTextEl ? reviewTextEl.textContent.trim() : '';
                
                // Get reviewer name
                const nameEl = reviewEl.querySelector('div[aria-label] button');
                const reviewerName = nameEl ? nameEl.textContent.trim() : '';
                
                // Get review date
                const dateEl = reviewEl.querySelector('span[aria-label*="ago"], span[aria-label*="yang lalu"]');
                const reviewDate = dateEl ? dateEl.textContent.trim() : '';
                
                // Get rating
                const ratingEl = reviewEl.querySelector('span[aria-label*="star"], span[aria-label*="bintang"]');
                const rating = ratingEl ? ratingEl.getAttribute('aria-label') : '';
                
                if (reviewText || reviewerName) {{
                    reviews.push({{
                        reviewer: reviewerName,
                        date: reviewDate,
                        rating: rating,
                        text: reviewText
                    }});
                }}
            }}
            
            return reviews;
        }} catch (e) {{
            console.error("Error finding reviews:", e);
            return [];
        }}
        """
        
        reviews = driver.execute_script(js_code)
        
        # Format reviews as string
        if reviews and len(reviews) > 0:
            formatted_reviews = []
            for review in reviews:
                review_str = f"[{review.get('date', 'No date')}] {review.get('reviewer', 'Anonymous')}"
                if review.get('rating'):
                    review_str += f" ({review['rating']})"
                if review.get('text'):
                    review_str += f": {review['text'][:200]}..."  # Limit text length
                formatted_reviews.append(review_str)
            return " | ".join(formatted_reviews)
        
        return "No reviews available"
        
    except Exception as e:
        print(f"Error extracting reviews: {e}")
        return "No reviews available"


def get_address_improved(driver):
    """Get address from Google Maps with improved extraction"""
    try:
        # Wait a bit for the page to load completely
        time.sleep(1)
        
        js_code = """
        try {
            const addressSelectors = [
                'button[data-item-id="address"]',
                'div[data-tooltip="Copy address"]',
                'button[aria-label*="Address:"]',
                'button[aria-label*="Alamat:"]'
            ];
            
            for (const selector of addressSelectors) {
                const elements = document.querySelectorAll(selector);
                for (let i = 0; i < elements.length; i++) {
                    const text = elements[i].textContent || elements[i].innerText;
                    if (text && text.trim().length > 5) {
                        if (elements[i].hasAttribute('aria-label')) {
                            const label = elements[i].getAttribute('aria-label');
                            const match = label.match(/(?:address|alamat):?\\s*(.*)/i);
                            if (match && match[1]) {
                                return match[1].trim();
                            }
                        }
                        return text.trim();
                    }
                }
            }
            
            const genericElements = document.querySelectorAll('span.fontBodyMedium, div.fontBodyMedium');
            for (let i = 0; i < genericElements.length; i++) {
                const text = genericElements[i].textContent || genericElements[i].innerText;
                if (text && text.trim().length > 10 &&
                    (text.includes(',') ||
                     text.includes('Jl.') ||
                     text.includes('Jalan'))) {
                    return text.trim();
                }
            }
            
            return "Address not available";
        } catch (e) {
            console.error("Error finding address:", e);
            return "Address not available";
        }
        """
        
        address = driver.execute_script(js_code)
        if address and address != "Address not available":
            return clean_address(address)
        
        # Fallback to selenium
        address_selectors = [
            "button[data-item-id='address']",
            "div[role='button'][aria-label*='Address']",
            "button[aria-label*='Address']",
            "div.rogA2c",
            "span.fontBodyMedium:contains('Jl.')",
            "span.fontBodyMedium:contains('Jalan')"
        ]
        
        for selector in address_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if text and len(text) > 10 and (
                        ',' in text or 'Jl.' in text or 'Jalan' in text
                    ):
                        return clean_address(text)
            except Exception:
                continue

    except Exception as e:
        print(f"Error extracting address: {e}")
    
    return "Address not available"

def close_place_details(driver):
    """Close any open place details panel"""
    try:
        # Try multiple methods to close place details
        close_methods = [
            lambda: driver.find_element(By.CSS_SELECTOR, 'button[aria-label="Close"]').click(),
            lambda: driver.find_element(By.CSS_SELECTOR, 'button[aria-label="Back"]').click(),
            lambda: driver.find_element(By.CSS_SELECTOR, '.VfPpkd-icon-LgbsSe[aria-label="Back"]').click(),
            lambda: driver.find_element(By.CSS_SELECTOR, '[data-value="Directions"] + button').click(),
        ]
        
        for method in close_methods:
            try:
                method()
                time.sleep(0.5)
                return True
            except:
                continue
                
        # If no close button found, try pressing Escape
        from selenium.webdriver.common.action_chains import ActionChains
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(0.5)
        
    except Exception as e:
        print(f"Error closing place details: {e}")
    
    return False

def save_current_progress(signal_number=None, frame=None):
    """Save current progress to files"""
    if len(global_data['names']) == 0:
        print("No data to save yet.")
        if signal_number:
            sys.exit(0)
        return
        
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    
    # Save CSV
    data = {
        'Name': global_data['names'],
        'Category': global_data['categories'],
        'Address': global_data['addresses'],
        'Province': global_data['provinces'],
        'Rating': global_data['ratings'],
        'Reviews': global_data['reviews_count'],
        'Link': global_data['links']
    }
    df = pd.DataFrame(data)
    
    os.makedirs('checkpoints', exist_ok=True)
    
    csv_filename = f"checkpoints/poi_results_{timestamp}.csv"
    df.to_csv(csv_filename, index=False, encoding='utf-8')
        
    # Save checkpoint
    checkpoint_data = {
        'search_query': global_data['search_query'],
        'processed_elements': list(global_data['processed_elements']),
        'seen_name_pairs': [list(pair) for pair in global_data['seen_name_pairs']],
        'coordinates_searched': list(global_data['coordinates_searched']),
        'target_area': global_data['target_area'],
        'last_coords': global_data['last_coords'],
        'poi_index': global_data['poi_index'],
        'timestamp': timestamp
    }
    
    checkpoint_filename = f"checkpoints/checkpoint_{timestamp}.json"
    with open(checkpoint_filename, 'w', encoding='utf-8') as f:
        json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
    
    print(f"Progress saved to checkpoints/ directory with timestamp {timestamp}")
    
    if signal_number:
        print("Script interrupted. Progress saved.")
        sys.exit(0)

def get_current_coordinates(driver):
    """Extract current coordinates from URL or map state"""
    try:
        current_url = driver.current_url
        coord_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', current_url)
        
        if coord_match:
            lat = float(coord_match.group(1))
            lng = float(coord_match.group(2))
            return (lat, lng)
        
        js_code = """
        try {
            if (window.APP_INITIALIZATION_STATE) {
                const data = JSON.parse(window.APP_INITIALIZATION_STATE[3][2]);
                if (data && data.length > 1 && data[1].length > 1) {
                    return [data[1][0][0][0], data[1][0][0][1]];
                }
            }
            return null;
        } catch(e) {
            console.error("Error getting coordinates:", e);
            return null;
        }
        """
        
        coords = driver.execute_script(js_code)
        if coords and len(coords) == 2:
            return (coords[0], coords[1])
            
    except Exception as e:
        print(f"Error getting current coordinates: {e}")
    
    # Default coordinates for Malang
    return (-7.9826, 112.6309)  # Central Malang coordinates

def poi_search(driver, poi_category, target_area):
    """Search for specific POI category in target area with improved error handling"""
    try:
        print(f"\n--- Searching for {poi_category} in {target_area} ---")
        
        search_query = f"{poi_category} in {target_area}"
        
        # Use the search function with retry
        if not perform_search_with_retry(driver, search_query):
            print(f"Failed to search for {poi_category} in {target_area}")
            return False
        
        coords = get_current_coordinates(driver)
        global_data['coordinates_searched'].add(coords)
        global_data['last_coords'] = coords
        global_data['search_query'] = search_query
        
        print(f"Successfully loaded search for {poi_category} in {target_area} at coordinates: {coords}")
        return True
    
    except Exception as e:
        print(f"Error in search for {poi_category} in {target_area}: {e}")
        return False

def process_search_results(driver, max_results_per_category, max_scroll_attempts, scroll_pause_time):
    """Process search results from current view with improved error handling"""
    scroll_attempts = 0
    keep_scrolling = True
    consecutive_no_new_results = 0
    max_consecutive_no_results = 3
    
    print("Starting to process search results...")
    initial_count = len(global_data['names'])
    current_count = initial_count
    
    while keep_scrolling and (current_count - initial_count) < max_results_per_category and scroll_attempts < max_scroll_attempts:
        try:
            # Wait for the feed to be present
            feed_container = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//div[@role="feed"]'))
            )
            
            # Find all place elements
            place_elements = driver.find_elements(By.CSS_SELECTOR, 'a.hfpxzc')
            
            if not place_elements:
                print("No place elements found, trying to scroll or refresh...")
                scroll_attempts += 1
                continue
            
            print(f"Found {len(place_elements)} place elements in the current view")
            
            results_before_batch = len(global_data['names'])
            new_results_added = 0
            
            # Process elements in smaller batches to avoid stale references
            batch_size = min(3, len(place_elements))
            
            for i in range(0, len(place_elements), batch_size):
                batch_elements = place_elements[i:i+batch_size]
                
                for element in batch_elements:
                    try:
                        # Get a unique identifier for this element
                        element_id = element.get_attribute('data-item-id') or element.get_attribute('href')
                        if not element_id:
                            continue
                            
                        if element_id in global_data['processed_elements']:
                            continue
                        
                        global_data['processed_elements'].add(element_id)
                        
                        # Get name from aria-label first
                        try:
                            name = element.get_attribute('aria-label')
                            if not name:
                                # Fallback to text content
                                try:
                                    name_element = element.find_element(By.CSS_SELECTOR, 'div.qBF1Pd, div.NrDZNb')
                                    name = name_element.text.strip()
                                except NoSuchElementException:
                                    continue
                        except:
                            continue
                        
                        if not name or len(name.strip()) < 3:
                            continue

                        # Click to show details with retry mechanism
                        click_success = False
                        click_attempts = 0
                        max_click_attempts = 3
                        
                        while not click_success and click_attempts < max_click_attempts:
                            try:
                                # Scroll element into view first
                                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                                time.sleep(0.5)
                                
                                # Try clicking the element
                                driver.execute_script("arguments[0].click();", element)
                                
                                # Wait for details to load
                                time.sleep(1.5)
                                
                                # Verify that we're on a place details page
                                current_url = driver.current_url
                                if '/place/' in current_url or '/maps/place/' in current_url:
                                    click_success = True
                                else:
                                    click_attempts += 1
                                    print(f"Click attempt {click_attempts} failed, retrying...")
                                    time.sleep(0.5)
                                    
                            except Exception as click_error:
                                click_attempts += 1
                                print(f"Click attempt {click_attempts} failed: {click_error}")
                                time.sleep(0.5)
                        
                        if not click_success:
                            print(f"Failed to click element after {max_click_attempts} attempts: {name}")
                            continue
                        
                        # Get current URL as link
                        link = driver.current_url
                        
                        # Get place category
                        category = get_category(driver)
                        
                        # Get rating and reviews
                        rating, reviews_count = get_rating_and_reviews(driver)
                        
                        # Get address with retry
                        address_attempts = 0
                        address = None
                        while address_attempts < 3 and not address:
                            address = get_address_improved(driver)
                            if address == "Address not available":
                                address_attempts += 1
                                time.sleep(0.5)
                            else:
                                break
                        
                        if not address:
                            address = "Address not available"
                        
                        # Extract province
                        province = extract_province_from_address(address)
                        
                        # Check for duplicates
                        name_address_pair = (name, address)
                        if name_address_pair in global_data['seen_name_pairs']:
                            print(f"Duplicate found, skipping: {name}")
                            # Close place details before continuing
                            close_place_details(driver)
                            continue
                        
                        global_data['seen_name_pairs'].add(name_address_pair)
                        
                        # Add data to global storage
                        global_data['names'].append(name)
                        global_data['categories'].append(category)
                        global_data['addresses'].append(address)
                        global_data['provinces'].append(province)
                        global_data['ratings'].append(rating)
                        global_data['reviews_count'].append(reviews_count)
                        global_data['links'].append(link)
                        
                        
                        new_results_added += 1
                        total_results = len(global_data['names'])
                        current_count = total_results
                        
                        category_results = current_count - initial_count
                        print(f"[{category_results}/{max_results_per_category}] {name} | {category} | Rating: {rating} ({reviews_count} reviews) | {address}")
                        
                        # Close place details to go back to search results
                        close_place_details(driver)
                        time.sleep(0.5)
                        
                        # Save progress every 10 items
                        if total_results % 10 == 0:
                            save_current_progress()
                        
                        # Check if we've reached the target for this category
                        if (current_count - initial_count) >= max_results_per_category:
                            print(f"Reached target of {max_results_per_category} results for current category!")
                            keep_scrolling = False
                            break
                        
                        # Add a small delay between processing elements
                        time.sleep(0.3)
                        
                    except StaleElementReferenceException:
                        print("Stale element reference, refreshing element list...")
                        break  # Break from current batch and get fresh elements
                    except Exception as e:
                        print(f"Error processing element: {e}")
                        # Try to close any open dialogs and continue
                        close_place_details(driver)
                        continue
                
                # If we've broken out of the batch loop due to stale reference, get fresh elements
                if scroll_attempts % 2 == 0:  # Refresh elements every 2 scroll attempts
                    print("Refreshing element list...")
                    time.sleep(1)
                    break
            
            # Check if we added new results in this iteration
            results_after_batch = len(global_data['names'])
            if results_after_batch == results_before_batch:
                consecutive_no_new_results += 1
                print(f"No new results found in this iteration ({consecutive_no_new_results}/{max_consecutive_no_results})")
            else:
                consecutive_no_new_results = 0
                print(f"Added {new_results_added} new results in this iteration")
            
            # Stop if we haven't found new results for several consecutive attempts
            if consecutive_no_new_results >= max_consecutive_no_results:
                print(f"No new results found for {max_consecutive_no_results} consecutive attempts. Stopping scroll.")
                keep_scrolling = False
                break
            
            # Scroll down to load more results
            if keep_scrolling and (current_count - initial_count) < max_results_per_category:
                try:
                    # Make sure we're back on the search results
                    search_results_present = driver.find_elements(By.XPATH, '//div[@role="feed"]')
                    
                    if not search_results_present:
                        print("Search results not visible, trying to navigate back...")
                        driver.execute_script("window.history.back();")
                        time.sleep(2)
                        
                        # Wait for search results to reappear
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, '//div[@role="feed"]'))
                        )
                        feed_container = driver.find_element(By.XPATH, '//div[@role="feed"]')
                    
                    # Perform scroll
                    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", feed_container)
                    scroll_attempts += 1
                    print(f"Scroll attempt {scroll_attempts}/{max_scroll_attempts}")
                    
                    # Wait for new content to load
                    time.sleep(scroll_pause_time)
                    
                    # Add random delay to avoid being detected as bot
                    time.sleep(random.uniform(0.5, 1.5))
                    
                except Exception as scroll_error:
                    print(f"Error during scrolling: {scroll_error}")
                    scroll_attempts += 1
                    time.sleep(2)
            
        except TimeoutException:
            print("Timed out waiting for results to load")
            scroll_attempts += 1
            time.sleep(2)
        except Exception as e:
            print(f"Error during processing: {e}")
            scroll_attempts += 1
            # Try to recover by resetting to search results
            try:
                driver.execute_script("window.history.back();")
                time.sleep(2)
            except:
                pass
    
    results_found = len(global_data['names']) - initial_count
    print(f"Finished processing search results. Found {results_found} results for current POI category.")
    return results_found

def scrape_pois_parallel(target_areas, max_results_per_category=20, 
                        max_workers=None, headless=True, **kwargs):
    """
    Parallel POI scraping for multiple areas
    target_areas: list of areas to scrape or single area string
    """
    
    # Ensure target_areas is a list
    if isinstance(target_areas, str):
        target_areas = [target_areas]
    
    # Calculate optimal number of workers
    if max_workers is None:
        cpu_count = psutil.cpu_count(logical=False)
        max_workers = min(cpu_count, 4)  # Reduce max from 8 to 4
    
    print(f"Starting parallel scraping with {max_workers} workers")
    print(f"Target areas: {target_areas}")
    
    # POI categories (same as your existing list)
    poi_categories = global_data['poi_categories']
    
    # Create work queue - combine areas and categories
    work_queue = []
    for area in target_areas:
        for category in poi_categories:
            work_queue.append((category, area, len(work_queue), max_results_per_category, {
                'headless': headless,
                'driver_path': kwargs.get('driver_path'),
                'proxy': kwargs.get('proxy')
            }))
    
    print(f"Total tasks to process: {len(work_queue)}")
    
    # Process in parallel
    all_results = []
    completed_tasks = 0
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_task = {
            executor.submit(worker_scrape_category, task): task 
            for task in work_queue
        }
        
        # Process completed tasks
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            category, area, task_id = task[0], task[1], task[2]
            
            try:
                results = future.result(timeout=300)  # 5 minute timeout per task
                all_results.extend(results)
                completed_tasks += 1
                
                print(f"✓ Completed {completed_tasks}/{len(work_queue)}: {category} in {area} ({len(results)} POIs)")
                
                # Save progress every 10 completed tasks
                if completed_tasks % 10 == 0:
                    save_parallel_progress(all_results, completed_tasks)
                    
            except Exception as e:
                print(f"✗ Failed task {task_id}: {category} in {area} - {e}")
    
    print(f"Parallel scraping completed. Total POIs collected: {len(all_results)}")
    return convert_results_to_dataframe(all_results)

def save_parallel_progress(results, completed_tasks):
    """Save progress during parallel execution"""
    if not results:
        return
    
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    df = convert_results_to_dataframe(results)
    
    os.makedirs('parallel_checkpoints', exist_ok=True)
    filename = f"parallel_checkpoints/progress_{completed_tasks}_tasks_{timestamp}.csv"
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"Progress saved: {len(results)} POIs to {filename}")

def convert_results_to_dataframe(results):
    """Convert parallel results to DataFrame"""
    if not results:
        return pd.DataFrame()
    
    df_data = {
        'Name': [r['name'] for r in results],
        'Category': [r['category'] for r in results],
        'Address': [r['address'] for r in results],
        'Province': [r['province'] for r in results],
        'Latitude': [r.get('latitude') for r in results],
        'Longitude': [r.get('longitude') for r in results],
        'ZIP_Code': [r.get('zip_code') for r in results],
        'City': [r.get('city') for r in results],
        'District': [r.get('district') for r in results],
        'Subdistrict': [r.get('subdistrict') for r in results],
        'Phone': [r.get('phone') for r in results],
        'Website': [r.get('website') for r in results],
        'Open_Hours': [r.get('open_hours') for r in results],
        'Rating': [r['rating'] for r in results],
        'Reviews': [r['reviews_count'] for r in results],
        'Stars': [r.get('stars') for r in results],
        'Num_Reviews': [r.get('num_reviews') for r in results],
        'Latest_Reviews': [r.get('latest_reviews') for r in results],
        'Link': [r['link'] for r in results]
    }
    
    return pd.DataFrame(df_data)

def scrape_national_pois(max_workers=8, max_results_per_category=50):
    """
    Scale scraping to national level
    """
    # Major Indonesian cities for POI scraping
    indonesian_cities = [
        "Jakarta", "Surabaya", "Bandung", "Bekasi", "Medan",
        "Tangerang", "Depok", "Semarang", "Palembang", "Makassar",
        "South Tangerang", "Bogor", "Batam", "Pekanbaru", "Bandar Lampung",
        "Malang", "Padang", "Denpasar", "Samarinda", "Tasikmalaya",
        "Pontianak", "Cimahi", "Balikpapan", "Jambi", "Sukabumi",
        "Yogyakarta", "Solo", "Manado", "Bengkulu", "Mataram"
    ]
    
    print(f"Starting national POI scraping for {len(indonesian_cities)} cities")
    print(f"Estimated total tasks: {len(indonesian_cities) * len(global_data['poi_categories'])}")
    
    # Process cities in batches to manage memory
    batch_size = 5
    all_national_results = []
    
    for i in range(0, len(indonesian_cities), batch_size):
        batch_cities = indonesian_cities[i:i+batch_size]
        print(f"\nProcessing cities batch {i//batch_size + 1}: {batch_cities}")
        
        batch_results = scrape_pois_parallel(
            target_areas=batch_cities,
            max_results_per_category=max_results_per_category,
            max_workers=max_workers,
            headless=True
        )
        
        all_national_results.append(batch_results)
        
        # Save batch results
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        batch_filename = f"results/national_batch_{i//batch_size + 1}_{timestamp}.csv"
        batch_results.to_csv(batch_filename, index=False, encoding='utf-8')
        print(f"Batch saved: {len(batch_results)} POIs to {batch_filename}")
        
        # Brief pause between batches
        time.sleep(30)
    
    # Combine all results
    if all_national_results:
        final_df = pd.concat(all_national_results, ignore_index=True)
        
        # Remove duplicates
        final_df = final_df.drop_duplicates(subset=['Name', 'Address'], keep='first')
        
        # Save final national dataset
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        final_filename = f"results/national_pois_complete_{timestamp}.csv"
        final_df.to_csv(final_filename, index=False, encoding='utf-8')
        
        print(f"\n🎉 National scraping completed!")
        print(f"Total unique POIs collected: {len(final_df)}")
        print(f"Final dataset saved to: {final_filename}")
        
        return final_df
    
    return pd.DataFrame()

def generate_poi_recommendations():
    """Generate additional POI categories based on collected data"""
    # This function would analyze collected POIs and suggest additional categories
    # For demonstration purposes, we'll return a static list of recommendations
    
    recommendations = [
        "Emerging Tech Hubs/Startups",
        "Wholesale Distribution Centers",
        "Wedding & Event Venues",
        "Car Dealerships & Automotive Centers",
        "Construction & Property Development Sites",
        "Remittance Service Centers",
        "Retirement Communities",
        "Craftsman Villages (Kampung Wisata/UKM)",
        "Agricultural Processing Centers",
        "Logistics Hubs",
        "Special Economic Zones"
    ]
    
    return recommendations

def analyze_collected_data(df):
    """Perform basic analysis on collected POI data"""
    if len(df) == 0:
        print("No data to analyze.")
        return
    
    print("\n=== POI Data Analysis for Banking Expansion ===")
    
    # Category distribution
    print("\nCategory Distribution:")
    category_counts = df['Category'].value_counts().head(15)
    for category, count in category_counts.items():
        print(f"- {category}: {count}")
    
    # Rating analysis
    print("\nRating Analysis:")
    avg_rating = df['Rating'].mean()
    print(f"- Average POI rating: {avg_rating:.2f}/5.0")
    
    high_rated = df[df['Rating'] >= 4.5]
    print(f"- Highly-rated POIs (4.5+): {len(high_rated)}")
    
    # Popular POIs (by review count)
    print("\nPopular POIs (by review count):")
    popular = df.sort_values('Reviews', ascending=False).head(5)
    for idx, row in popular.iterrows():
        print(f"- {row['Name']} ({row['Category']}): {row['Reviews']} reviews, Rating: {row['Rating']}")
    
    # Geographic distribution
    print("\nGeographic Distribution:")
    address_keywords = ['Klojen', 'Blimbing', 'Lowokwaru', 'Sukun', 'Kedungkandang']
    
    for keyword in address_keywords:
        district_count = df[df['Address'].str.contains(keyword, case=False)].shape[0]
        percentage = (district_count / len(df)) * 100
        print(f"- {keyword}: {district_count} POIs ({percentage:.1f}%)")
    
    # Banking expansion recommendations
    print("\nBanking Expansion Recommendations:")
    print("1. Consider areas with high concentrations of commercial POIs with high ratings")
    print("2. Target locations near educational institutions with large student populations")
    print("3. Identify areas with limited existing banking services but high POI density")
    print("4. Explore emerging residential areas with growing amenities")
    print("5. Evaluate transportation hubs as potential high-traffic branch locations")
    
    # Suggest additional POI categories to explore
    print("\nRecommended Additional POI Categories to Explore:")
    recommendations = generate_poi_recommendations()
    for i, rec in enumerate(recommendations[:5], 1):
        print(f"{i}. {rec}")
    
    print("\nFor a complete analysis, import this data into a GIS system to visualize spatial distribution.")

def export_to_gis_format(df, output_filename):
    """Export data to a format suitable for GIS mapping with real coordinates from Google Maps URLs"""
    import re
    
    print(f"\nExporting data to GIS-ready format: {output_filename}")
    
    # Create a copy to avoid modifying the original DataFrame
    gis_df = df.copy()
    
    def extract_coordinates(url):
        # Pattern to match latitude and longitude in Google Maps URLs
        lat_pattern = r'!3d(-?\d+\.\d+)'
        lng_pattern = r'!4d(-?\d+\.\d+)'
        
        # Extract latitude and longitude
        lat_match = re.search(lat_pattern, url)
        lng_match = re.search(lng_pattern, url)
        
        # Return coordinates if found, otherwise return None
        if lat_match and lng_match:
            return float(lat_match.group(1)), float(lng_match.group(1))
        else:
            return None, None
    
    # Apply the extraction function to get coordinates
    coordinates = gis_df['Link'].apply(extract_coordinates)
    
    # Split the results into separate latitude and longitude columns
    gis_df['latitude'], gis_df['longitude'] = zip(*coordinates)
    
    # Handle any potential missing coordinates with central Malang as fallback
    base_lat, base_lng = -7.9826, 112.6309  # Central Malang coordinates
    
    # Replace None values with base coordinates
    gis_df['latitude'] = gis_df['latitude'].fillna(base_lat)
    gis_df['longitude'] = gis_df['longitude'].fillna(base_lng)
    
    # Export to CSV
    gis_df.to_csv(output_filename, index=False)
    
    # Count how many real coordinates were extracted vs. fallbacks
    real_coords = (~gis_df['latitude'].isin([base_lat])).sum()
    print(f"Exported {len(gis_df)} POIs to {output_filename} with coordinates for GIS mapping")
    print(f"Successfully extracted {real_coords} real coordinates from URLs")
    
    if real_coords < len(gis_df):
        print(f"Used fallback coordinates for {len(gis_df) - real_coords} POIs without extractable coordinates")

def retry_failed_task(task, max_retries=2):
    """Retry a failed task with exponential backoff"""
    for attempt in range(max_retries):
        try:
            time.sleep(attempt * 2)  # Exponential backoff
            return worker_scrape_category(task)
        except Exception as e:
            print(f"Retry attempt {attempt + 1} failed for task {task[2]}: {e}")
            if attempt == max_retries - 1:
                return []
    return []

def worker_scrape_category(args):
    """Worker function for parallel processing of POI categories"""
    category, target_area, worker_id, max_results_per_category, config = args
    
    print(f"Worker {worker_id}: Starting {category} in {target_area}")
    
    # Initialize separate driver for this worker
    driver = None
    worker_results = []
    processed_in_worker = set()
    
    try:
        # Pass worker_id to initialize_driver
        driver = initialize_driver(
            headless=config.get('headless', True),
            driver_path=config.get('driver_path'),
            proxy=config.get('proxy'),
            worker_id=worker_id  # Add this line
        )
        
        # Rest of your existing code...
        search_query = f"{category} in {target_area}"
        
        if not perform_search_with_retry(driver, search_query):
            print(f"Worker {worker_id}: Failed to search for {category}")
            return worker_results
        
        results = process_category_results_simple(driver, category, max_results_per_category, worker_id, processed_in_worker)
        worker_results.extend(results)
        
        print(f"Worker {worker_id}: Completed {category} - found {len(results)} POIs")
        
    except Exception as e:
        print(f"Worker {worker_id}: Error processing {category}: {e}")
    finally:
        if driver:
            try:
                driver.quit()
                # Clean up temp directory
                temp_dir = os.path.join(os.getcwd(), f"temp_browser_data_worker_{worker_id}")
                if os.path.exists(temp_dir):
                    import shutil
                    try:
                        shutil.rmtree(temp_dir)
                    except:
                        pass
            except Exception as cleanup_error:
                print(f"Worker {worker_id}: Cleanup error: {cleanup_error}")
    
    return worker_results

def wait_for_new_content(driver, previous_count, timeout=10):
    """Wait for new content to load after scrolling"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        current_elements = driver.find_elements(By.CSS_SELECTOR, 'a.hfpxzc')
        if len(current_elements) > previous_count:
            return len(current_elements)
        time.sleep(0.5)
    return previous_count

def process_category_results_simple(driver, category, max_results, worker_id, processed_in_worker):
    """Simplified process search results for a specific category"""
    results = []
    scroll_attempts = 0
    max_scroll_attempts = 15  # Increase scroll attempts
    consecutive_no_new_results = 0
    max_consecutive_no_results = 3
    
    print(f"Worker {worker_id}: Starting to process {category} (target: {max_results} POIs)")
    
    while len(results) < max_results and scroll_attempts < max_scroll_attempts:
        try:
            # Get current place elements
            place_elements = driver.find_elements(By.CSS_SELECTOR, 'a.hfpxzc')
            current_element_count = len(place_elements)
            
            print(f"Worker {worker_id}: Found {current_element_count} place elements (scroll attempt {scroll_attempts})")

            if not place_elements:
                scroll_attempts += 1
                print(f"Worker {worker_id}: No elements found, continuing to scroll...")
                time.sleep(2)
                continue
            
            results_before = len(results)
            
            # Process ALL elements, not just first 10
            processed_this_iteration = 0
            for i, element in enumerate(place_elements):
                if len(results) >= max_results:
                    break
                    
                try:
                    # Get unique identifier for this element
                    element_id = element.get_attribute('data-item-id') or element.get_attribute('href')
                    if not element_id:
                        try:
                            element_id = element.get_attribute('href')
                        except:
                            continue
                            
                    if not element_id or element_id in processed_in_worker:
                        continue
                    
                    processed_in_worker.add(element_id)
                    
                    # Extract POI data with better error handling
                    poi_data = extract_poi_data(driver, element, worker_id)
                    if poi_data and poi_data['name']:
                        results.append(poi_data)
                        print(f"Worker {worker_id}: [{len(results)}/{max_results}] {poi_data['name']} | {poi_data.get('phone', 'No phone')} | {poi_data.get('stars', 0)} stars")
                        processed_this_iteration += 1
                        print(f"Worker {worker_id}: [{len(results)}/{max_results}] {poi_data['name']}")
                        
                        # Small delay between extractions
                        time.sleep(random.uniform(0.2, 0.5))
                
                except Exception as e:
                    print(f"Worker {worker_id}: Error processing element {i}: {e}")
                    continue

            print(f"Worker {worker_id}: Processed {processed_this_iteration} new elements this iteration")
            
            # Check if we found new results
            if len(results) == results_before:
                consecutive_no_new_results += 1
                print(f"Worker {worker_id}: No new results ({consecutive_no_new_results}/{max_consecutive_no_results})")
            else:
                consecutive_no_new_results = 0
            
            # Stop if no new results for several iterations
            if consecutive_no_new_results >= max_consecutive_no_results:
                print(f"Worker {worker_id}: Stopping - no new results for {max_consecutive_no_results} iterations")
                break
            
            # Enhanced scrolling logic
            if len(results) < max_results:
                try:
                    feed_container = driver.find_element(By.XPATH, '//div[@role="feed"]')
                    
                    # Get current scroll position
                    current_scroll = driver.execute_script("return arguments[0].scrollTop", feed_container)
                    max_scroll = driver.execute_script("return arguments[0].scrollHeight", feed_container)
                    
                    # Scroll down gradually
                    scroll_step = max_scroll // 4
                    new_scroll_position = min(current_scroll + scroll_step, max_scroll)
                    
                    driver.execute_script(f"arguments[0].scrollTop = {new_scroll_position}", feed_container)
                    
                    # Wait for content to load
                    time.sleep(2)
                    
                    # Check if we've reached the bottom
                    after_scroll = driver.execute_script("return arguments[0].scrollTop", feed_container)
                    if after_scroll == current_scroll:
                        print(f"Worker {worker_id}: Reached end of results")
                        break
                        
                    scroll_attempts += 1
                    
                    # Wait for new content to load
                    new_count = wait_for_new_content(driver, current_element_count)
                    if new_count == current_element_count:
                        print(f"Worker {worker_id}: No new content loaded after scroll")
                    
                except Exception as scroll_error:
                    print(f"Worker {worker_id}: Scroll error: {scroll_error}")
                    scroll_attempts += 1
                    if scroll_attempts >= max_scroll_attempts:
                        break
                        
        except Exception as e:
            print(f"Worker {worker_id}: Error in category processing: {e}")
            scroll_attempts += 1
            time.sleep(2)
    
    print(f"Worker {worker_id}: Completed {category} with {len(results)} POIs after {scroll_attempts} scroll attempts")
    return results

def process_category_results(driver, category, max_results, worker_id):
    """Process search results for a specific category"""
    results = []
    scroll_attempts = 0
    max_scroll_attempts = 8
    
    while len(results) < max_results and scroll_attempts < max_scroll_attempts:
        try:
            # Get current place elements
            place_elements = driver.find_elements(By.CSS_SELECTOR, 'a.hfpxzc')
            
            if not place_elements:
                scroll_attempts += 1
                continue
            
            # Process each element
            for element in place_elements[:min(10, len(place_elements))]:  # Process in batches
                if len(results) >= max_results:
                    break
                    
                try:
                    element_id = element.get_attribute('data-item-id') or element.get_attribute('href')
                    if not element_id:
                        continue
                    
                    # Thread-safe check for processed elements
                    with global_data['lock']:
                        if element_id in global_data['processed_elements']:
                            continue
                        global_data['processed_elements'].add(element_id)
                    
                    # Extract POI data
                    poi_data = extract_poi_data(driver, element, worker_id)
                    if poi_data:
                        # Check for duplicates
                        name_address_pair = (poi_data['name'], poi_data['address'])
                        with global_data['lock']:
                            if name_address_pair not in global_data['seen_name_pairs']:
                                global_data['seen_name_pairs'].add(name_address_pair)
                                results.append(poi_data)
                                print(f"Worker {worker_id}: [{len(results)}/{max_results}] {poi_data['name']}")
                
                except Exception as e:
                    print(f"Worker {worker_id}: Error processing element: {e}")
                    continue
            
            # Scroll for more results
            try:
                feed_container = driver.find_element(By.XPATH, '//div[@role="feed"]')
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", feed_container)
                time.sleep(1.5)
                scroll_attempts += 1
            except:
                break
                
        except Exception as e:
            print(f"Worker {worker_id}: Error in category processing: {e}")
            break
    
    return results

def extract_poi_data(driver, element, worker_id):
    """Extract POI data from a single element"""
    try:
        # Get name
        name = element.get_attribute('aria-label')
        if not name:
            try:
                name_element = element.find_element(By.CSS_SELECTOR, 'div.qBF1Pd, div.NrDZNb')
                name = name_element.text.strip()
            except:
                return None
        
        if not name or len(name.strip()) < 3:
            return None
        
        # Click to get details
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", element)
        time.sleep(2)
        
        # Extract all details
        link = driver.current_url
        category = get_category(driver)
        
        # Get coordinates from URL
        latitude, longitude = extract_coordinates_from_url(link)
        
        # Get basic rating info
        rating, reviews_count = get_rating_and_reviews(driver)
        
        # Get detailed rating info
        stars, num_reviews = get_detailed_rating_info(driver)
        
        # Get address and location details
        address = get_address_improved(driver)
        province = extract_province_from_address(address)
        zip_code, city, district, subdistrict = extract_location_details(address)
        
        # Get contact information
        phone = get_phone_number(driver)
        website = get_website(driver)
        open_hours = get_open_hours(driver)
        
        # Get reviews
        latest_reviews = get_latest_reviews(driver, 5)
        
        # Close details
        close_place_details(driver)
        time.sleep(0.3)
        
        return {
            'name': name,
            'category': category,
            'address': address,
            'province': province,
            'latitude': latitude,
            'longitude': longitude,
            'zip_code': zip_code,
            'city': city,
            'district': district,
            'subdistrict': subdistrict,
            'phone': phone,
            'website': website,
            'open_hours': open_hours,
            'rating': rating,
            'reviews_count': reviews_count,
            'stars': stars,
            'num_reviews': num_reviews,
            'latest_reviews': latest_reviews,
            'link': link
        }
        
    except Exception as e:
        print(f"Worker {worker_id}: Error extracting POI data: {e}")
        return None
    
def scrape_pois_parallel_threads(target_areas, max_results_per_category=20, 
                                max_workers=None, headless=True, **kwargs):
    """
    Alternative parallel POI scraping using threads (better for Windows)
    """
    
    # Ensure target_areas is a list
    if isinstance(target_areas, str):
        target_areas = [target_areas]
    
    # Calculate optimal number of workers (fewer for threads)
    if max_workers is None:
        max_workers = min(2, len(target_areas))  # Reduce from 4 to 2
    
    print(f"Starting thread-based parallel scraping with {max_workers} workers")
    print(f"Target areas: {target_areas}")
    
    # POI categories
    poi_categories = [
        # Automotive
        "Abarth dealer", "Acura dealer", "Alfa Romeo dealer", "Aston Martin dealer", 
        "Audi dealer", "Auto accessories wholesaler", "Auto air conditioning service",
        "Auto auction", "Auto body parts supplier", "Auto body shop", "Auto bodywork mechanic",
        "Auto broker", "Auto care products store", "Auto chemistry shop", "Auto dent removal service",
        "Auto electrical service", "Auto glass repair service", "Auto glass shop", 
        "Auto insurance agency", "Auto machine shop", "Auto market", "Auto painting",
        "Auto parts manufacturer", "Auto parts market", "Auto parts store", 
        "Auto radiator repair service", "Auto repair shop", "Auto restoration service",
        "Auto rickshaw stand", "Auto spring shop", "Auto sunroof shop", "Auto tag agency",
        "Auto tune up service", "Auto upholsterer", "Auto window tinting service",
        "Auto wrecker", "Automobile storage facility", "Bentley dealer", "BMW dealer",
        "BMW motorcycle dealer", "Brake shop", "Buick dealer", "Bugatti dealer",
        "Cadillac dealer", "Car accessories store", "Car alarm supplier", "Car battery store",
        "Car dealer", "Car detailing service", "Car factory", "Car finance and loan company",
        "Car inspection station", "Car leasing service", "Car manufacturer", "Car racing track",
        "Car rental agency", "Car repair and maintenance service", "Car security system installer",
        "Car sharing location", "Car stereo store", "Car wash", "Chauffeur service",
        "Chevrolet dealer", "Chrysler dealer", "Citroen dealer", "Dacia dealer",
        "Daihatsu dealer", "Dodge dealer", "DS Automobiles dealer", "Ducati dealer",
        "Ferrari dealer", "Fiat dealer", "Ford dealer", "Genesis dealer", "GMC dealer",
        "Harley-Davidson dealer", "Honda dealer", "Hyundai dealer", "Indian Motorcycle dealer",
        "Infiniti dealer", "Isuzu dealer", "Jaguar dealer", "Jeep dealer", "Karma dealer",
        "Kawasaki motorcycle dealer", "Kia dealer", "Lamborghini dealer", "Lancia dealer",
        "Land Rover dealer", "Lexus dealer", "Lincoln dealer", "Maserati dealer",
        "Maybach dealer", "Mazda dealer", "McLaren dealer", "Mercedes-Benz dealer",
        "MG dealer", "MINI dealer", "Mitsubishi dealer", "Motorcycle dealer",
        "Motorcycle driving school", "Motorcycle insurance agency", "Motorcycle parts store",
        "Motorcycle rental agency", "Motorcycle repair shop", "Motorcycle shop",
        "Motor scooter dealer", "Motor scooter repair shop", "Motor vehicle dealer",
        "Muffler shop", "Nissan dealer", "Oldsmobile dealer", "Opel dealer", "Peugeot dealer",
        "Pontiac dealer", "Porsche dealer", "Ram dealer", "Renault dealer", "Rolls-Royce dealer",
        "RV dealer", "RV detailing service", "RV inspection service", "RV repair shop",
        "RV storage facility", "RV supply store", "Saab dealer", "Saturn dealer",
        "Seat dealer", "Skoda dealer", "Smart Car dealer", "Smart dealer", "Subaru dealer",
        "Suzuki dealer", "Suzuki motorcycle dealer", "Tata Motors dealer", "Tesla showroom",
        "Tire manufacturer", "Tire repair shop", "Tire service", "Tire shop",
        "Toyota dealer", "Transmission shop", "Triumph motorcycle dealer", "Truck accessories store",
        "Truck dealer", "Truck parts supplier", "Truck rental agency", "Truck repair shop",
        "Truck stop", "Truck topper supplier", "Truck driving school", "Used auto parts store",
        "Used car dealer", "Used motorcycle dealer", "Used truck dealer", "Vehicle exporter",
        "Vehicle inspection service", "Vehicle shipping agent", "Vehicle wrapping service",
        "Volkswagen dealer", "Volvo dealer", "Yamaha motorcycle dealer",

        # Food and Beverage
        "Açaí shop", "Acaraje restaurant", "Afghan restaurant", "African restaurant",
        "Algerian Restaurant", "Alsace restaurant", "American restaurant", "Anago restaurant",
        "Andalusian restaurant", "Andhra restaurant", "Angler fish restaurant", "Anhui restaurant",
        "Arab restaurant", "Argentinian restaurant", "Armenian restaurant", "Asian fusion restaurant",
        "Asian restaurant", "Assamese restaurant", "Asturian restaurant", "Australian restaurant",
        "Austrian restaurant", "Awadhi restaurant", "Ayam penyet restaurant", "Bagel shop",
        "Bakery", "Bakso restaurant", "Balinese restaurant", "Bangladeshi restaurant",
        "Bar", "Bar & grill", "Bar PMU", "Bar restaurant furniture store", "Bar tabac",
        "Barbecue restaurant", "Batak restaurant", "Bavarian restaurant", "Beer distributor",
        "Beer garden", "Beer hall", "Beer store", "Belgian restaurant", "Bengali restaurant",
        "Berry restaurant", "Betawi restaurant", "Biryani restaurant", "Bistro",
        "Brazilian pastelaria", "Brazilian restaurant", "Breakfast restaurant", "Brewery",
        "Brewpub", "Brunch restaurant", "Bubble tea store", "Buffet restaurant",
        "Bulgarian restaurant", "Burrito restaurant", "Burmese restaurant", "Butcher shop",
        "Butcher shop deli", "Cafe", "Cafeteria", "Cajun restaurant", "Cake shop",
        "Californian restaurant", "Cambodian restaurant", "Canadian restaurant", "Candy store",
        "Cantabrian restaurant", "Cantonese restaurant", "Cape Verdean restaurant",
        "Caribbean restaurant", "Carvery", "Castilian restaurant", "Catalonian restaurant",
        "Catering food and drink supplier", "Caterer", "Cendol restaurant", "Central American restaurant",
        "Central European restaurant", "Central Javanese restaurant", "Champon noodle restaurant",
        "Chanko restaurant", "Charcuterie", "Cheesesteak restaurant", "Chicken restaurant",
        "Chicken shop", "Chicken wings restaurant", "Chilean restaurant", "Chinese bakery",
        "Chinese noodle restaurant", "Chinese restaurant", "Chinese supermarket", "Chinese takeaway",
        "Chinese tea house", "Chocolate cafe", "Chocolate factory", "Chocolate shop",
        "Chop bar", "Chophouse restaurant", "Churreria", "Cider bar", "Cider mill",
        "Cig kofte restaurant", "Cocktail bar", "Coffee roasters", "Coffee shop", "Coffee stand",
        "Coffee store", "Cold cut store", "Cold noodle restaurant", "Colombian restaurant",
        "Confectionery store", "Contemporary Louisiana restaurant", "Continental restaurant",
        "Cookie shop", "Costa Rican restaurant", "Couscous restaurant", "Crab house",
        "Creole restaurant", "Creperie", "Croatian restaurant", "Cuban restaurant",
        "Culinary school", "Cupcake shop", "Cured ham bar", "Cured ham store",
        "Cured ham warehouse", "Czech restaurant", "Dan Dan noodle restaurant", "Danish restaurant",
        "Deli", "Delivery Chinese restaurant", "Dessert restaurant", "Dessert shop",
        "Dhaba", "Dim sum restaurant", "Diner", "Dinner theater", "Doner kebab restaurant",
        "Donut shop", "Dried seafood store", "Dumpling restaurant", "Durum restaurant",
        "Dutch restaurant", "East African restaurant", "East Javanese restaurant",
        "Eastern European restaurant", "Eclectic restaurant", "Ecuadorian restaurant",
        "Egyptian restaurant", "English restaurant", "Eritrean restaurant", "Ethiopian restaurant",
        "European restaurant", "Falafel restaurant", "Family restaurant", "Fast food restaurant",
        "Fine dining restaurant", "Finnish restaurant", "Fish & chips restaurant",
        "Fish and chips takeaway", "Fish processing", "Fish restaurant", "Floridian restaurant",
        "Fondue restaurant", "Food bank", "Food broker", "Food court", "Food manufacturer",
        "Food processing company", "Food producer", "Franconian restaurant", "French restaurant",
        "French steakhouse restaurant", "Fresh food market", "Fried chicken takeaway",
        "Fruit and vegetable processing", "Fruit and vegetable store", "Fruit parlor",
        "Fugu restaurant", "Fujian restaurant", "Fusion restaurant", "Galician restaurant",
        "Gastropub", "Georgian restaurant", "German restaurant", "Gluten-free restaurant",
        "Goan restaurant", "Gourmet grocery store", "Greek restaurant", "Grocery delivery service",
        "Grocery store", "Gujarati restaurant", "Guizhou restaurant", "Gyro restaurant",
        "Haitian restaurant", "Hakka restaurant", "Halal restaurant", "Haleem restaurant",
        "Ham shop", "Hamburger restaurant", "Haute French restaurant", "Hawaiian restaurant",
        "Health food restaurant", "Hoagie restaurant", "Hong Kong style fast food restaurant",
        "Hookah bar", "Hot dog restaurant", "Hot dog stand", "Hot pot restaurant",
        "Hunan restaurant", "Hungarian restaurant", "Hyderabadi restaurant", "Ice cream shop",
        "Icelandic restaurant", "Ikan bakar restaurant", "Indian grocery store", "Indian Muslim restaurant",
        "Indian restaurant", "Indian sizzler restaurant", "Indian sweets shop", "Indian takeaway",
        "Indonesian restaurant", "Irish pub", "Irish restaurant", "Israeli restaurant",
        "Italian grocery store", "Italian restaurant", "Izakaya restaurant", "Jamaican restaurant",
        "Japanese cheap sweets shop", "Japanese confectionery shop", "Japanese curry restaurant",
        "Japanese delicatessen", "Japanese grocery store", "Japanese regional restaurant",
        "Japanese restaurant", "Japanese steakhouse", "Japanese sweets restaurant",
        "Japanized western restaurant", "Javanese restaurant", "Jewish restaurant", "Jiangsu restaurant",
        "Juice shop", "Kaiseki restaurant", "Kalle pache restaurant", "Karnataka restaurant",
        "Kashmiri restaurant", "Kebab shop", "Kerala restaurant", "Kofta restaurant",
        "Konkani restaurant", "Korean barbecue restaurant", "Korean beef restaurant",
        "Korean grocery store", "Korean restaurant", "Korean rib restaurant", "Koshari restaurant",
        "Kosher grocery store", "Kosher restaurant", "Kushiyaki restaurant", "Kyoto style Japanese restaurant",
        "Latin American restaurant", "Lebanese restaurant", "Lechon restaurant", "Ligurian restaurant",
        "Lithuanian restaurant", "Live music bar", "Live music venue", "Lombardian restaurant",
        "Lunch restaurant", "Macrobiotic restaurant", "Madrilian restaurant", "Malaysian restaurant",
        "Maltese restaurant", "Manado restaurant", "Mandarin restaurant", "Marathi restaurant",
        "Marche restaurant", "Meal delivery", "Takeout restaurant", "Meat packer",
        "Meat processor", "Meat products store", "Meat dish restaurant", "Mediterranean restaurant",
        "Mexican grocery store", "Mexican restaurant", "Mexican torta restaurant", "Meyhane",
        "Mid-Atlantic restaurant (US)", "Middle Eastern restaurant", "Miso cutlet restaurant",
        "Modern British restaurant", "Modern European restaurant", "Modern French restaurant",
        "Modern Indian restaurant", "Modern izakaya restaurant", "Momo restaurant",
        "Mongolian barbecue restaurant", "Monjayaki restaurant", "Moroccan restaurant",
        "Mughlai restaurant", "Murtabak restaurant", "Mutton barbecue restaurant", "Nasi goreng restaurant",
        "Nasi restaurant", "Nasi uduk restaurant", "Native American restaurant", "Navarraise restaurant",
        "Neapolitan restaurant", "Nepalese restaurant", "New American restaurant", "New England restaurant",
        "New Zealand restaurant", "Nicaraguan restaurant", "Noodle shop", "North African restaurant",
        "North Eastern Indian restaurant", "North Indian restaurant", "Northern Italian restaurant",
        "Norwegian restaurant", "Nuevo Latino restaurant", "Nyonya restaurant", "Oaxacan restaurant",
        "Obanzai restaurant", "Oden restaurant", "Odia restaurant", "Offal barbecue restaurant",
        "Okonomiyaki restaurant", "Organic food store", "Organic restaurant", "Oriental goods store",
        "Oyster bar restaurant", "Paan shop", "Pacific Northwest restaurant (Canada)",
        "Pacific Northwest restaurant (US)", "Pacific Rim restaurant", "Padang restaurant",
        "Paisa restaurant", "Pakistani restaurant", "Palatine restaurant", "Palestinian restaurant",
        "Pan-Asian restaurant", "Pan-Latin restaurant", "Pancake restaurant", "Panipuri shop",
        "Paraguayan restaurant", "Parsi restaurant", "Pasta shop", "Pastry shop", "Patisserie",
        "Pay by weight restaurant", "Pecel lele restaurant", "Pempek restaurant", "Pennsylvania Dutch restaurant",
        "Persian restaurant", "Peruvian restaurant", "Pho restaurant", "Piadina restaurant",
        "Pie shop", "Piedmontese restaurant", "Pilaf restaurant", "Pizza delivery", "Pizza restaurant",
        "Pizza takeaway", "Po' boys restaurant", "Poke bar", "Polish restaurant", "Polynesian restaurant",
        "Porridge restaurant", "Portuguese restaurant", "Pozole restaurant", "Pretzel store",
        "Provence restaurant", "Pub", "Pueblan restaurant", "Puerto Rican restaurant",
        "Punjabi restaurant", "Québécois restaurant", "Raclette restaurant", "Rajasthani restaurant",
        "Ramen restaurant", "Raw food restaurant", "Restaurant", "Brasserie", "Rice cake shop",
        "Rice cracker shop", "Rice restaurant", "Rice shop", "Romanian restaurant", "Roman restaurant",
        "Russian grocery store", "Russian restaurant", "Rustic furniture store", "Sake brewery",
        "Salad shop", "Salsa bar", "Salvadoran restaurant", "Sandwich shop", "Sardinian restaurant",
        "Satay restaurant", "Scandinavian restaurant", "Scottish restaurant", "Seafood donburi restaurant",
        "Seafood market", "Seafood restaurant", "Seblak restaurant", "Self service restaurant",
        "Serbian restaurant", "Seychelles restaurant", "Sfiha restaurant", "Sukiyaki and Shabu Shabu restaurant",
        "Shabu-shabu restaurant", "Shandong restaurant", "Shanghainese restaurant", "Shawarma restaurant",
        "Sicilian restaurant", "Singaporean restaurant", "Small plates restaurant", "Snack bar",
        "Soba noodle shop", "Soft drinks shop", "Suppon restaurant", "Soul food restaurant",
        "Soup kitchen", "Soup restaurant", "Soup shop", "South African restaurant",
        "South American restaurant", "South Asian restaurant", "Southeast Asian restaurant",
        "South Indian restaurant", "South Sulawesi restaurant", "Southern Italian restaurant",
        "Southern restaurant (US)", "Southwest France restaurant", "Southwestern restaurant (US)",
        "Spanish restaurant", "Steak house", "Steamboat restaurant", "Steamed bun shop",
        "Sukiyaki restaurant", "Sundae restaurant", "Sundanese restaurant", "Sushi restaurant",
        "Sushi takeaway", "Sweets and dessert buffet", "Swedish restaurant", "Swiss restaurant",
        "Syrian restaurant", "Syokudo and Teishoku restaurant", "Conveyor belt sushi restaurant",
        "Tabascan restaurant", "Tacaca restaurant", "Taco restaurant", "Taiwanese restaurant",
        "Takoyaki restaurant", "Tamale shop", "Tapas bar", "Tapas restaurant", "Tea house",
        "Tea store", "Tegal restaurant", "Temaki restaurant", "Tempura donburi restaurant",
        "Tempura restaurant", "Teppanyaki restaurant", "Tex-Mex restaurant", "Thai restaurant",
        "Threads and yarns wholesaler", "Tibetan restaurant", "Tiffin center", "Tiki bar",
        "Toast restaurant", "Tofu restaurant", "Tofu shop", "Tongue restaurant", "Tonkatsu restaurant",
        "Tortilla shop", "Traditional American restaurant", "Traditional restaurant", "Traditional teahouse",
        "Tunisian restaurant", "Turkish restaurant", "Turkmen restaurant", "Tuscan restaurant",
        "Udon noodle restaurant", "Ukrainian restaurant", "Unagi restaurant", "Uruguayan restaurant",
        "Uyghur cuisine restaurant", "Uzbeki restaurant", "Valencian restaurant", "Vegan restaurant",
        "Vegetable wholesale market", "Vegetarian cafe and deli", "Vegetarian restaurant",
        "Venezuelan restaurant", "Venetian restaurant", "Vietnamese restaurant", "Welsh restaurant",
        "West African restaurant", "Western restaurant", "Wine bar", "Wine cellar", "Wine club",
        "Wine storage facility", "Wine store", "Winery", "Wok restaurant", "Yakiniku restaurant",
        "Yakisoba Restaurant", "Yakitori restaurant", "Yemeni restaurant", "Yucatan restaurant",
        "Zhejiang restaurant",

        # Retail and Shopping
        "Adult entertainment store", "African goods store", "American grocery store", "Amish furniture store",
        "Ammunition supplier", "Amusement machine supplier", "Animal cafe", "Antique furniture store",
        "Antique store", "Appliance parts supplier", "Appliance store", "Aquarium shop",
        "Archery store", "Army & navy surplus shop", "Art supply store", "Asian grocery store",
        "Asian household goods store", "Audio visual equipment supplier", "Australian goods store",
        "Auto accessories wholesaler", "Baby clothing store", "Baby store", "Bag shop",
        "Bakery equipment", "Baking supply store", "Banner store", "Bar stool supplier",
        "Barber supply store", "Baseball goods store", "Basketball court contractor", "Bathroom supply store",
        "Batik clothing store", "Battery store", "Bead store", "Beauty product supplier",
        "Beauty supply store", "Bed shop", "Bedding store", "Bedroom furniture store",
        "Belt shop", "Bicycle store", "Billiards supply store", "Bird shop", "Blinds shop",
        "Board game club", "Boat accessories supplier", "Boat cover supplier", "Book store",
        "Boot store", "Bottle & can redemption center", "Boutique", "Bowling supply shop",
        "Brewing supply store", "Building materials store", "Cabinet store", "Cake decorating equipment shop",
        "Camera store", "Camping store", "Candle store", "Cane furniture store", "Cannabis store",
        "Canoe & kayak store", "Car accessories store", "Car battery store", "Car stereo store",
        "Carpet store", "Cash and carry wholesaler", "CD store", "Cell phone accessory store",
        "Cell phone store", "Ceramics wholesaler", "Cheese shop", "Children's book store",
        "Children's clothing store", "Children's furniture store", "Children's store", "Chinaware store",
        "Chocolate shop", "Christmas store", "Church supply store", "Cigar shop", "Cinema equipment supplier",
        "Clock and watch maker", "Clothes and fabric manufacturer", "Clothes market", "Clothing store",
        "Coin dealer", "Collectibles store", "Comic book store", "Computer accessories store",
        "Computer desk store", "Computer software store", "Computer store", "Confectionery store",
        "Consignment shop", "Convenience store", "Cookie shop", "Copying supply store",
        "Cosmetics store", "Costume jewelry shop", "Costume store", "Countertop store",
        "Craft store", "Cricket shop", "Currency exchange service", "Curtain store", "Custom t-shirt store",
        "Cutlery store", "Dairy store", "Dance store", "Dart supply store", "Deli",
        "Department store", "Dessert shop", "Diabetes equipment supplier", "Digital printer",
        "Discount store", "Discount supermarket", "Display home centre", "DJ supply store",
        "Do-it-yourself shop", "Doll store", "Dollar store", "Door shop", "Dress store",
        "Dried flower shop", "Drum store", "Dry fruit store", "Dry wall supply store",
        "DVD store", "Dye store", "Eastern European grocery store", "Educational supply store",
        "Electric bicycle store", "Electric motor store", "Electrical supply store", "Electronics store",
        "Envelope supplier", "Equipment supplier", "Eyebrow bar", "Fabric store", "Factory equipment supplier",
        "Farm equipment supplier", "Fashion accessories store", "Fastener supplier", "Feed manufacturer",
        "Animal feed store", "Felt boots store", "Fence supply store", "Figurine shop",
        "Filipino grocery store", "Fire alarm supplier", "Fireworks store", "Fish store",
        "Fishing store", "Fitness center", "Exercise equipment store", "Flag store",
        "Flamenco dance store", "Flower market", "Foam rubber supplier", "Fountain contractor",
        "Frozen food store", "Frozen yogurt shop", "Fruit and vegetable store", "Fuel supplier",
        "Fur coat shop", "Furnace store", "Furniture accessories", "Furniture store",
        "Futon store", "Game store", "Garden center", "Garden furniture shop", "Gas shop",
        "Gift basket store", "Gift shop", "Gift wrap store", "Glass & mirror shop",
        "Glassware store", "Golf shop", "Gourmet grocery store", "Greeting card shop",
        "Grill store", "Grocery store", "Guitar store", "Gun shop", "Haberdashery",
        "Hair extensions supplier", "Handbags shop", "Hardware store", "Hat shop",
        "Hawaiian goods store", "Health and beauty shop", "Health food store", "Hearing aid store",
        "Herb shop", "Herbal medicine store", "Hockey supply store", "Hobby store",
        "Home goods store", "Home improvement store", "Home theater store", "Hookah store",
        "Horse trailer dealer", "Hot tub store", "Hotel supply store", "Household chemicals supplier",
        "Hunting and fishing store", "Hunting store", "HVAC contractor", "Ice cream equipment supplier",
        "Indian grocery store", "Indian sweets shop", "Insulation materials store", "Interior plant service",
        "International school", "Internet shop", "Irish goods store", "Italian grocery store",
        "Janitorial equipment supplier", "Japanese grocery store", "Jewelry store", "Judaica store",
        "Junk store", "Kennel", "Kimono store", "Kitchen furniture store", "Kitchen supply store",
        "Kite shop", "Knife store", "Knit shop", "Korean grocery store", "Kosher grocery store",
        "Lamp shade supplier", "Landscaping supply store", "Law book store", "Lawn mower store",
        "Leather goods store", "Lighting store", "Linens store", "Lingerie store", "Linoleum store",
        "Liquor store", "Lock Store", "Luggage store", "Lumber store", "Magazine store",
        "Magic store", "Mailbox supplier", "Malaysian restaurant", "Map store", "Martial arts supply store",
        "Massage supply store", "Mattress store", "Meat products store", "Medical book store",
        "Medical supply store", "Men's clothing store", "Metaphysical supply store", "Mexican goods store",
        "Mexican grocery store", "Miniatures store", "Mobile home supply store", "Model train store",
        "Moving supply store", "Music box store", "Music store", "Musical instrument store",
        "Native american goods store", "Natural goods store", "Needlework shop", "Newsstand",
        "Notions store", "Novelty store", "Nut store", "Office supply store", "Oil store",
        "Optical wholesaler", "Oriental goods store", "Oriental rug store", "Outdoor clothing and equipment shop",
        "Outdoor furniture store", "Outdoor sports store", "Outerwear store", "Outlet mall",
        "Outlet store", "Oxygen equipment supplier", "Paintball store", "Paintings store",
        "Paper store", "Party store", "Passport photo processor", "Pen store", "Perfume store",
        "Pet store", "Pet supply store", "Phone repair service", "Photo shop", "Piano store",
        "Picture frame shop", "Plus size clothing store", "Popcorn store", "Poster store",
        "Pottery store", "Poultry store", "Printing equipment and supplies", "Religious book store",
        "Religious goods store", "Reptile store", "Retail store", "Rice shop", "Rock shop",
        "Rug store", "Rugby store", "Russian grocery store", "Saree Shop", "Seasonal goods store",
        "Second hand store", "Security system supplier", "Sewing machine store", "Sewing shop",
        "Sheet music store", "Shelving store", "Shoe store", "Shop supermarket furniture store",
        "Shopping mall", "Silk store", "Skateboard shop", "Ski shop", "Skin care products vending machine",
        "Smart shop", "Smoke shop", "Snowboard shop", "Soccer store", "Sofa store",
        "Soft drinks shop", "Sporting goods store", "Sports card store", "Sports memorabilia store",
        "Sports nutrition store", "Sportswear store", "Stationery store", "Stereo rental store",
        "Home audio store", "Sticker manufacturer", "Store", "Store equipment supplier",
        "Store fixture supplier", "Sunglasses store", "Supermarket", "Surplus store",
        "Swimwear store", "T-shirt store", "Table tennis supply store", "Tack shop",
        "Tatami store", "Tattoo supply store", "Tea store", "Telescope store", "Tennis store",
        "Textile mill", "Thread supplier", "3D printing service", "Thrift store", "Tile store",
        "Tire shop", "Toiletries store", "Tool store", "Toy store", "Traditional Kostume store",
        "Traditional market", "Trophy shop", "Tropical fish store", "Truck accessories store",
        "Typewriter supplier", "Underwear store", "Uniform store", "Unfinished furniture store",
        "Used appliance store", "Used bicycle shop", "Used book store", "Used CD store",
        "Used clothing store", "Used computer store", "Used furniture store", "Used game store",
        "Used musical instrument store", "Used office furniture store", "Used store fixture supplier",
        "Used tire shop", "Vacuum cleaner store", "Vaporizer store", "Variety store",
        "Vegetable wholesale market", "Video game store", "Video store", "Vintage clothing store",
        "Violin shop", "Vitamin & supplements store", "Wallpaper store", "Warehouse club",
        "Warehouse store", "Washer & dryer store", "Watch store", "Water ski shop",
        "Waterbed store", "Wedding souvenir shop", "Wedding store", "Western apparel store",
        "Wheel store", "Wheelchair store", "Wholesale bakery", "Wholesale drugstore",
        "Wholesale florist", "Wholesale food store", "Wholesale grocer", "Wholesale jeweler",
        "Wholesale market", "Wholesale plant nursery", "Wholesaler", "Wholesaler household appliances",
        "Wicker store", "Wig shop", "Wine store", "Women's clothing store", "Wood supplier",
        "Woodworking supply store", "Wool store", "Work clothes store", "Yarn store",
        "Youth clothing store",

        # Healthcare & Medical
        "Abortion clinic", "Acupuncture clinic", "Acupuncturist", "Addiction treatment center",
        "Adult day care center", "Allergist", "Alternative medicine practitioner", "Ambulance service",
        "Anesthesiologist", "Animal hospital", "Applied behavior analysis therapist", "Assisted living facility",
        "Birth center", "Birth certificate service", "Birth control center", "Blood bank",
        "Blood donation center", "Blood testing service", "Cancer treatment center", "Cardiologist",
        "Cardiovascular and thoracic surgeon", "Child health care centre", "Child psychiatrist",
        "Child psychologist", "Childbirth class", "Children Policlinic", "Children's hospital",
        "Chiropractor", "Community health centre", "Cosmetic dentist", "Cosmetic surgeon",
        "Craniosacral therapy", "Dental clinic", "Dental hygienist", "Dental implants periodontist",
        "Dental implants provider", "Dental laboratory", "Dental radiology", "Dental supply store",
        "Dentist", "Denture care center", "Dermatologist", "Diabetes center", "Diabetes equipment supplier",
        "Diabetologist", "Diagnostic center", "Dialysis center", "Dietitian", "Doctor",
        "Drug testing service", "Drug store", "Eating disorder treatment center", "Emergency care physician",
        "Emergency care service", "Emergency dental service", "Emergency room", "Emergency veterinarian service",
        "Endocrinologist", "Endodontist", "Endoscopist", "Eye care center", "Family practice physician",
        "Fertility clinic", "Fertility physician", "General hospital", "General practitioner",
        "Geriatrician", "Government hospital", "Gynecologist", "Hand surgeon", "Head and neck surgeon",
        "Health consultant", "Health counselor", "Health insurance agency", "Health resort",
        "Health spa", "Hearing aid repair service", "Heart hospital", "Hematologist",
        "Hepatologist", "HIV testing center", "Homeopath", "Homeopathic pharmacy", "Hospice",
        "Hospital", "Hospital equipment and supplies", "Hospital department", "Hyperbaric medicine physician",
        "Immunologist", "Infectious disease physician", "Intensivist", "Internal medicine ward",
        "Internist", "IV therapy service", "Laboratory", "Laboratory equipment supplier",
        "Lactation service", "Laser hair removal service", "Lasik surgeon", "Medical billing service",
        "Medical Center", "Medical certificate service", "Medical clinic", "Medical diagnostic imaging center",
        "Medical equipment manufacturer", "Medical equipment supplier", "Medical examiner",
        "Medical group", "Medical laboratory", "Medical lawyer", "Medical office", "Medical school",
        "Medical spa", "Medical technology manufacturer", "Medical transcription service",
        "Medicine exporter", "Mental health clinic", "Mental health service", "Men's health physician",
        "Midwife", "Military hospital", "MRI center", "Naturopathic practitioner", "Neonatal physician",
        "Nephrologist", "Neurologist", "Neurophysiologist", "Neuropsychologist", "Neurosurgeon",
        "Nurse practitioner", "Nursing agency", "Nursing home", "Nursing school", "Nutritionist",
        "Women's health clinic", "Occupational health service", "Occupational medical physician",
        "Occupational therapist", "Oncologist", "Ophthalmologist", "Ophthalmology clinic", "Optician",
        "Optometrist", "Oral and maxillofacial surgeon", "Oral surgeon", "Organ donation and tissue bank",
        "Organic drug store", "Oriental medicine clinic", "Oriental medicine store", "Orthodontist",
        "Orthopedic clinic", "Orthopedic shoe store", "Orthopedic supplies store", "Orthopedic surgeon",
        "Orthoptist", "Orthotics & prosthetics service", "Osteopath", "Otolaryngologist",
        "Otolaryngology clinic", "Pain control clinic", "Pain management physician", "Parapharmacy",
        "Pathologist", "Patients support association", "Pediatric cardiologist", "Pediatric clinic",
        "Pediatric dentist", "Pediatric dermatologist", "Pediatric endocrinologist", "Pediatric gastroenterologist",
        "Pediatric hematologist", "Pediatric nephrologist", "Pediatric neurologist", 
        "Pediatric oncologist", "Pediatric ophthalmologist", "Pediatric orthopedic surgeon",
        "Pediatric pulmonologist", "Pediatric rheumatologist", "Pediatric surgeon", 
        "Pediatric urologist", "Pediatrician", "Pedorthist", "Perinatal center", "Periodontist",
        "Permanent make-up clinic", "Pharmacy", "Physical examination center", "Physical fitness program",
        "Physical rehabilitation center", "Physician assistant", "Physician referral service",
        "Physical therapist", "Physical therapy clinic", "Physiotherapy equipment supplier",
        "Physiatrist", "Plastic surgeon", "Plastic surgery clinic", "Podiatrist", "Pregnancy care center",
        "Private hospital", "Proctologist", "Prosthodontist", "Psychiatric hospital", "Psychiatrist",
        "Psychic", "Psychoanalyst", "Psychologist", "Psychomotor therapist", "Psychoneurological specialized clinic",
        "Psychopedagogy clinic", "Psychosocial therapist", "Psychosomatic medical practitioner", 
        "Psychotherapist", "Public health department", "Public medical center", "Pulmonologist",
        "Radiologist", "Radiotherapist", "Reflexologist", "Rehabilitation center", "Reiki therapist",
        "Reproductive health clinic", "Speech pathologist", "Sperm bank", "Sports massage therapist",
        "Sports medicine clinic", "Sports medicine physician", "STD clinic", "STD testing service",
        "Surgeon", "Surgical center", "Surgical oncologist", "TB clinic", "Teeth whitening service",
        "Thai massage therapist", "Transplant surgeon", "Travel clinic", "University hospital",
        "Urgent care center", "Urologist", "Urology clinic", "Vascular surgeon", "Venereologist",
        "Veterinarian", "Veterinary pharmacy", "Walk-in clinic", "Weight loss service", "Wellness center",
        "X-ray lab",

        # Education & Training
        "Accounting school", "Acupuncture school", "Adult education school", "After school program",
        "Agricultural high school", "Aikido school", "Art school", "Apprenticeship center",
        "Architecture school", "Aromatherapy class", "Ballroom dance instructor", "Ballet school",
        "Barber school", "Bartending school", "Beauty school", "Berufsfachschule (vocational school with apprenticeship)",
        "Bilingual school", "Boarding school", "Boys' high school", "Business school", "Capoeira school",
        "Catholic school", "CBSE school", "Charter school", "Chess instructor", "Childbirth class",
        "Chinese language instructor", "Chinese language school", "Christian college", "Civil examinations academy",
        "Co-ed school", "Coaching center", "College", "College of agriculture", "Combined primary and secondary school",
        "Community college", "Community school", "Comprehensive secondary school", "Computer training school",
        "Conservatory of music", "Cooking class", "Cooking school", "Cosplay cafe", "Cost accounting service",
        "Cramming school", "Culinary school", "Dance school", "Distance learning center", "Drama school",
        "Drawing lessons", "Drivers license training school", "Driving school", "Drum school",
        "Education center", "Educational consultant", "Educational institution", "Educational testing service",
        "Elementary school", "Emergency training", "Emergency training school", "Engineering school",
        "English language camp", "English language school", "Evening school", "Faculty of chemistry",
        "Faculty of law", "Faculty of pharmacy", "Faculty of science", "Faculty of media and information science",
        "Farm school", "Fashion design school", "Fencing school", "Fire fighters academy", "Firearms academy",
        "Flamenco school", "Flight school", "Folk high school", "Foreign languages program school",
        "French language school", "General education school", "German language school", "Girls' high school",
        "Government college", "Government school", "Graduate school", "Grammar school", "Gymnasium school",
        "Hauptschule (lower-tier secondary school)", "High school", "Higher secondary school",
        "Hip hop dance class", "Hospitality and tourism school", "Hospitality high school",
        "Host club", "Hotel management school", "ICSE school", "Industrial technical engineers association",
        "Institute of technology", "International school", "Judo school", "Jujitsu school",
        "Junior college", "K-12 school", "Karate school", "Kickboxing school", "Kindergarten",
        "Kung fu school", "Language school", "Learning center", "Management school", "Mathematics school",
        "Middle school", "Military school", "Montessori preschool", "Montessori school", "Music college",
        "Music conservatory", "Music instructor", "Music school", "Nursery school", "Open university",
        "Painting lessons", "Parochial school", "Photography class", "Photography school", "Piano instructor",
        "Pilates studio", "Cue sports school", "Pottery classes", "Preschool", "Preparatory school",
        "Primary school", "Private college", "Private educational institution", "Private school",
        "Private university", "Public educational institution", "Real estate school", "Realschule (middle-tier secondary school)",
        "Religious school", "Sailing school", "Salsa classes", "Sambo school", "School",
        "School administration office", "School district office", "School for the deaf", "School house",
        "School for the visually impaired", "Secondary school", "Self defense school", "Seminary",
        "Senior high school", "Seventh-day Adventist church", "Sewing company", "Single sex secondary school",
        "Sixth form college", "Skating instructor", "Ski school", "Stitching class", "Specialized hospital",
        "Student career counseling office", "Student dormitory", "Student housing center", "Student union",
        "Students parents association", "Students support association", "Study at home school", "Studying center",
        "Summer camp organizer", "Surf school", "Swimming instructor", "Swimming school", "Taekwondo school",
        "Tai chi school", "Teachers college", "Technical school", "Technical university", "Telecommunications school",
        "Test preparation center", "Trade school", "Training centre", "Tutoring service", "University",
        "Academic department", "University library", "Vocational gymnasium school", "Vocational secondary school",
        "Vocational school", "Waldorf kindergarten", "Waldorf school", "Wing chun school", "Wood working class",
        "Wrestling school", "Yoga instructor", "Youth care service",

        # Goverment & Public Services
        "Agenzia Entrate", "Board of education", "Border crossing station", "Border guard",
        "Bureau of Indian Affairs", "Carabinieri police", "Central bank", "Chamber of agriculture",
        "Chamber of Commerce", "Chamber of handicrafts", "Citizen information bureau", "City clerk's office",
        "City courthouse", "City department of transportation", "City district office", "City employment department",
        "City government office", "City or town hall", "City tax office", "Civil defense",
        "Civil police", "Commissioner for Oaths", "Construction and maintenance office", "County government office",
        "Court executive officer", "Court reporter", "Customs broker", "Customs consultant",
        "Customs office", "Customs warehouse", "Department of housing", "Department of motor vehicles",
        "Department of Public Safety", "Department of Social Services", "Department of Transportation",
        "District attorney", "District government office", "District Justice", "District office",
        "Driver's license office", "Driving test center", "Environment office", "Environment renewable natural resources",
        "Federal Agency for Technical Relief", "Federal government office", "Federal police", "Fire station",
        "Government college", "Government economic program", "Government hospital", "Government office",
        "Government ration shop", "Government school", "Guardia Civil", "Guardia Di Finanza Police",
        "Highway patrol", "Immigration & naturalization service", "Immigration detention centre",
        "Institute of Geography and Statistics", "Intelligence agency", "Japanese prefecture government office",
        "Justice department", "Juvenile detention center", "Land planning authority", "Land reform institute",
        "Land surveying office", "Legal affairs bureau", "License bureau", "Local government office",
        "Main customs office", "Marine self defense force", "Marriage license bureau", "Military archive",
        "Military recruiting office", "Ministry of Education", "Municipal administration office",
        "Municipal Department of Tourism", "Municipal Guard", "National library", "National museum",
        "National park", "National reserve", "Patent office", "Pension office", "Police academy",
        "Police station", "Post office", "Probation office", "Public defender's office", "Public prosecutors office",
        "Public safety office", "Public utility company", "Public works department", "Regional airport",
        "Regional council", "Regional government office", "Registration office", "Registry office",
        "Sheriff's department", "Social security financial department", "Social security office",
        "State archive", "State employment department", "State government office", "State office of education",
        "State parliament", "State police", "Tax assessor", "Tax collector's office", "Tax consultant",
        "Tax department", "Tax office", "Tax preparation", "Tax preparation service", "Toll station",
        "United States Armed Forces Base", "Urban planning department", "Veterans affairs department",
        "Visa and passport office", "Visitor center", "Voter registration office", "Weigh station",

        # Entertainment & Recreation
        "Amateur theater", "Amusement center", "Amusement park", "Amusement park ride", "Amphitheater",
        "Animation studio", "Anime club", "Aquarium", "Arboretum", "Archery club", "Archery range",
        "Arena", "Art cafe", "Art center", "Art gallery", "Art museum", "Arts organization",
        "Athletic club", "Athletic field", "Athletic park", "Athletic track", "Auditorium",
        "Badminton club", "Badminton complex", "Badminton court", "Ballet theater", "Balloon artist",
        "Balloon ride tour agency", "Band", "Bar", "Baseball club", "Baseball field", "Basketball club",
        "Basketball court", "Batting cage center", "Beach club", "Beach entertainment shop",
        "Beach pavillion", "Beach volleyball club", "Beach volleyball court", "Bingo hall",
        "Blues club", "BMX club", "BMX park", "BMX track", "Board game club", "Boat club",
        "Bocce ball court", "Bowling alley", "Bowling club", "Boxing club", "Boxing gym",
        "Boxing ring", "Bridge club", "Bungee jumping center", "Cabaret club", "Call center",
        "Campground", "Camping cabin", "Camping farm", "Canoe and kayak club", "Canoeing area",
        "Carnival club", "Casino", "Casino hotel", "Chess and card club", "Chess club",
        "Children's amusement center", "Children hall", "Children's club", "Children's farm",
        "Children's museum", "Children's party buffet", "Children's theater", "Christmas market",
        "Circus", "City park", "Club", "Comedy club", "Community center", "Community garden",
        "Concert hall", "Conference center", "Conservative club", "Convention center", "Croquet club",
        "Cultural association", "Cultural center", "Cultural landmark", "Curling club", "Curling hall",
        "Cycling park", "Dance club", "Dance company", "Dance hall", "Dance pavillion",
        "Dance restaurant", "Dart bar", "Day spa", "Disc golf course", "Disco club",
        "Dive club", "Diving center", "Dog park", "Drive-in movie theater", "Escape room center",
        "Exhibition and trade centre", "Fairground", "Festival", "Festival hall", "Ferris wheel",
        "Fishing camp", "Fishing club", "Fishing pier", "Fishing pond", "Football club",
        "American football field", "Function room facility", "Gambling house", "Garden",
        "Gay bar", "Gay night club", "Gay sauna", "Girl bar", "Go club", "Go-kart track",
        "Golf club", "Golf course", "Golf driving range", "Greyhound stadium", "Gymnasium school",
        "Gymnastics center", "Gymnastics club", "Handball club", "Handball court", "Hang gliding center",
        "Haunted house", "Heritage building", "Heritage museum", "Heritage preservation", "Heritage railroad",
        "High ropes course", "Hiking area", "Hiking club", "Historical landmark", "Historical place museum",
        "Historical society", "History museum", "Hockey club", "Hockey field", "Hockey rink",
        "Horse boarding stable", "Horse riding field", "Horse riding school", "Horseback riding service",
        "Hot bedstone spa", "Hot tub repair service", "Hunting area", "Hunting club", "Hunting preserve",
        "Ice hockey club", "Ice skating club", "Ice skating rink", "IMAX theater", "Indoor cycling",
        "Indoor golf course", "Indoor playground", "Indoor snowcenter", "Indoor swimming pool",
        "Jazz club", "Kabaddi club", "Karaoke", "Karaoke bar", "Leagues club", "Leisure centre",
        "Lesbian bar", "Lido", "Little league club", "Little league field", "Live music bar",
        "Live music venue", "Lounge", "Mahjong house", "Marina", "Maritime museum", "Martial arts club",
        "Masonic center", "Movie rental store", "Movie studio", "Movie theater", "Museum",
        "Museum of space history", "Museum of zoology", "Music management and promotion", "Musical club",
        "Natural history museum", "Natural rock climbing area", "Nature preserve", "Night club",
        "Night market", "Nudist club", "Nudist park", "Observatory", "Observation deck",
        "Off-road race track", "Off roading area", "Off track betting shop", "Open air museum",
        "Opera company", "Opera house", "Orchestra", "Outdoor activity organiser", "Outdoor bath",
        "Outdoor equestrian facility", "Outdoor movie theater", "Outdoor swimming pool", "Pachinko parlor",
        "Paintball center", "Park", "Park & ride", "Performing arts group", "Performing arts theater",
        "Philharmonic hall", "Piano bar", "Pickleball court", "Picnic ground", "Pinball machine supplier",
        "Planetarium", "Playground", "Playgroup", "Polo club", "Pool billard club", "Pool hall",
        "Pony club", "Pony ride service", "Prawn fishing", "Public bath", "Public golf course",
        "Public library", "Public parking space", "Public sauna", "Public swimming pool", "Pub",
        "Puppet theater", "Queer bar", "Race car dealer", "Racecourse", "Racquetball club",
        "Raft trip outfitter", "Rafting", "Rail museum", "Ranch", "Recreation center",
        "Reenactment site", "Rock climbing", "Rock climbing gym", "Rock music club", "Rodeo",
        "Roller coaster", "Roller skating club", "Roller skating rink", "Rowing area", "Rowing club",
        "Rsl club", "Rugby", "Rugby club", "Rugby field", "Rugby league club", "Sailing club",
        "Sailing event area", "Samba school", "Sambodrome", "Sauna", "Sauna club", "Scale model club",
        "Model shop", "Science museum", "Scenic spot", "SCUBA instructor", "SCUBA tour agency",
        "Sculpture", "Sculpture museum", "Senior citizen center", "Shelter", "Shooting event area",
        "Shooting range", "Showroom", "Singles organization", "Skateboard park", "Skate sharpening service",
        "Skating instructor", "Skeet shooting range", "Ski club", "Ski resort", "Skittle club",
        "Skydiving center", "Snowmobile dealer", "Snowmobile rental service", "Social club", "Softball club",
        "Softball field", "Space of remembrance", "Spa", "Spa and health club", "Spa garden",
        "Sport tour agency", "Sports bar", "Sports club", "Sports complex", "Squash club",
        "Squash court", "Stadium", "Stage", "Summer toboggan run", "Surf lifesaving club",
        "Swimming basin", "Swimming competition", "Swimming facility", "Swimming lake", "Swimming pool",
        "Swim club", "Table tennis club", "Table tennis facility", "Taekwondo competition area",
        "Technology museum", "Television station", "Tennis club", "Tennis court", "Theater company",
        "Theater production", "Theme park", "Thermal baths", "Ticket office", "Traditional costume club",
        "Train depot", "Train repairing center", "Train yard", "Trial attorney", "Tribal headquarters",
        "Video arcade", "Video game rental kiosk", "Video game rental service", "Video game rental store",
        "Video karaoke", "Volleyball club", "Volleyball court", "War museum", "Water park",
        "Water polo pool", "Water skiing club", "Wax museum", "Weightlifting area", "Wildlife and safari park",
        "Wildlife park", "Wildlife refuge", "Yacht club", "Yakatabune", "Youth center", "Youth club",
        "Youth hostel", "Zoo",

        # Financial Services
        "Accountant", "Accounting firm", "Appraiser", "ATM", "Audit service", "Auditor",
        "Bank", "Bankruptcy attorney", "Bankruptcy service", "Business banking service", "Business broker",
        "Central bank", "Certified public accountant", "Chartered accountant", "Check cashing service",
        "Cooperative bank", "Credit counseling service", "Credit reporting agency", "Credit union",
        "Currency exchange service", "Estate appraiser", "Estate liquidator", "Federal credit union",
        "Finance broker", "Financial advisor", "Financial audit", "Financial consultant", "Financial institution",
        "Financial planner", "Home insurance agency", "Income protection insurance agency", "Insurance agency",
        "Insurance attorney", "Insurance broker", "Insurance company", "Investment bank", "Investment company",
        "Investment service", "Loan agency", "Loss adjuster", "Money changer", "Money order service",
        "Money transfer service", "Mortgage broker", "Mortgage lender", "Pawn shop", "Pegadaian",
        "Private equity firm", "Private sector bank", "Public sector bank", "Savings bank",
        "Securities company", "Superannuation consultant", "Tax preparation service", "Title company",
        "Trust bank", "Venture capital company",

        # Professional Services
        "Accounting software company", "Acoustical consultant", "Administrative attorney", "Adoption agency",
        "Advertising agency", "Commercial photographer", "Antenna service", "Aerial photographer",
        "Aerospace company", "Agricultural engineer", "Agricultural organization", "Air conditioning contractor",
        "Air conditioning repair service", "Air duct cleaning service", "Aircraft maintenance company",
        "Airbrushing service", "Airline", "Airline ticket agency", "Airport shuttle service",
        "Alternative fuel station", "Aluminum welder", "Architect", "Architects association",
        "Architectural and engineering model maker", "Architectural designer", "Architecture firm",
        "Aromatherapy service", "Art dealer", "Art restoration service", "Artist", "Asbestos testing service",
        "Asphalt contractor", "Attorney referral service", "Audio visual consultant", "Audiovisual equipment rental service",
        "Audio visual equipment repair service", "Audiologist", "Auditor", "Auto insurance agency",
        "Aviation consultant", "Aviation training institute", "Awning supplier", "Bail bonds service",
        "Balloon artist", "Banner store", "Barber shop", "Barrister", "Beautician", "Biochemical supplier",
        "Biotechnology company", "Bird control service", "Blacksmith", "Blast cleaning service",
        "Blueprint service", "Boat builders", "Boat cleaning service", "Boat detailing service",
        "Boat repair shop", "Boat storage facility", "Boat tour agency", "Boating instructor",
        "Body piercing shop", "Body shaping class", "Bonesetting house", "Book publisher", "Bookbinder",
        "Bookkeeping service", "BPO company", "BPO placement agency", "Branding agency", "Brewery",
        "Bricklayer", "Building consultant", "Building designer", "Building equipment hire service",
        "Building firm", "Building inspector", "Building materials market", "Building restoration service",
        "Building society", "Chartered surveyor", "Burglar alarm store", "Bus charter", "Bus company",
        "Bus ticket agency", "Bus tour agency", "Business administration service", "Business attorney",
        "Business development service", "Business management consultant", "Business networking company",
        "Business to business service", "Cabinet maker", "Cable company", "Call center", "Calligraphy lesson",
        "Camera repair shop", "Canoe & kayak rental service", "Canoe & kayak tour agency", "Car finance and loan company",
        "Car rental agency", "Car security system installer", "Carport and pergola builder", "Carriage ride service",
        "Casket service", "Catering equipment rental service", "Ceiling supplier", "Cement manufacturer",
        "Ceramic manufacturer", "Certification agency", "Chemical engineering service", "Chemical exporter",
        "Chemical manufacturer", "Chemical plant", "Chemistry lab", "Chimney services", "Chimney sweep",
        "Civil engineer", "Civil engineering company", "Civil law attorney", "Classified ads newspaper publisher",
        "Cleaners", "Cleaning products supplier", "Clock repair service", "Closed circuit television",
        "Coaching center", "Commercial agent", "Commercial printer", "Commercial real estate agency",
        "Commercial real estate inspector", "Computer consultant", "Computer networking service",
        "Computer repair service", "Computer security service", "Computer service", "Concrete contractor",
        "Concrete factory", "Conservatory construction contractor", "Conservatory supply & installation",
        "Construction company", "Construction equipment supplier", "Construction machine dealer",
        "Construction machine rental service", "Consultant", "Consumer advice center", "Container service",
        "Contractor", "Convention information bureau", "Conveyancer", "Copier repair service", "Copywriting service",
        "Corporate gift supplier", "Counselor", "Countertop contractor", "Courier service", "Court reporter",
        "Crane dealer", "Crane rental agency", "Crane service", "Credit counseling service", "Cremation service",
        "Crime victim service", "Criminal justice attorney", "Cruise agency", "Cruise line company",
        "Custom confiscated goods store", "Custom home builder", "Custom label printer", "Custom tailor",
        "Customs broker", "Customs consultant", "Cutlery store", "Data center", "Data entry service",
        "Data recovery service", "Database management company", "Dating service", "Day care center",
        "Debris removal service", "Debt collecting", "Debt collection agency", "Deck builder",
        "Delivery service", "Demolition contractor", "Design agency", "Design engineer", "Design institute",
        "Desktop publishing service", "Diaper service", "Diesel engine repair service", "Digital printing service",
        "Direct mail advertising", "Disability services and support organization", "Distribution service",
        "Diving contractor", "DJ service", "Dock builder", "Dogsled ride service", "Domestic abuse treatment center",
        "Door manufacturer", "Double glazing installer", "Doula", "Drafting equipment supplier", "Drafting service",
        "Drainage service", "Drawing lessons", "Dress and tuxedo rental service", "Dressmaker", "Drilling contractor",
        "Drone service", "Drug testing service", "Dry cleaner", "Dry wall contractor", "Dryer vent cleaning service",
        "Dumpster rental service", "Dynamometer supplier", "E commerce agency", "E-commerce service",
        "Ear piercing service", "Earth works company", "Ecologists association", "Economic consultant",
        "Economic development agency", "Educational consultant", "Educational testing service", "Elder law attorney",
        "Electric utility company", "Electric vehicle charging station contractor", "Electrical engineer",
        "Electrical installation service", "Electrical repair shop", "Electrician", "Electrolysis hair removal service",
        "Electronic engineering service", "Electronics engineer", "Electronics hire shop", "Electronics manufacturer",
        "Electronics repair shop", "Elevator manufacturer", "Elevator service", "Embossing service",
        "Embroidery service", "Emergency locksmith service", "Emergency training", "Employment agency",
        "Employment attorney", "Employment center", "Employment consultant", "Energy advisory service",
        "Energy equipment and solutions", "Engine rebuilding service", "Engineer", "Engineering consultant",
        "Engraver", "Entertainer", "Entertainment agency", "Environmental attorney", "Environmental consultant",
        "Environmental engineer", "Environmental health service", "Equipment exporter", "Equipment importer",
        "Equipment rental agency", "Escrow service", "Estate litigation attorney", "Estate planning attorney",
        "Event management company", "Event planner", "Event technology service", "Event ticket seller",
        "Excavating contractor", "Executive search firm", "Executive suite rental agency", "Executor",
        "Exhibition planner", "Exporter", "Fabrication engineer", "Factory equipment supplier",
        "Family counselor", "Family law attorney", "Family planning counselor", "Family service center",
        "Farm equipment repair service", "Farrier service", "Fashion designer", "Fax service",
        "Fence contractor", "Feng shui consultant", "Fiber optic products supplier", "Fiberglass repair service",
        "Film and photograph library", "Film production company", "Filtration plant", "Fingerprinting service",
        "Fire damage restoration service", "Fire protection consultant", "Fire protection equipment supplier",
        "Fire protection service", "Fireplace manufacturer", "Fishing charter", "Fitted furniture supplier",
        "Floor refinishing service", "Floor sanding and polishing service", "Flooring contractor", "Florist",
        "Flower delivery", "Flower designer", "Food and beverage consultant", "Food and beverage exporter",
        "Food broker", "Food machinery supplier", "Food manufacturing supply", "Food processing equipment",
        "Foreclosure service", "Foreign trade consultant", "Foreman builders association", "Forensic consultant",
        "Forestry service", "Fortune telling services", "Foster care service", "Fountain contractor",
        "Freight forwarding service", "Funeral celebrant service", "Funeral director", "Funeral home",
        "Fur manufacturer", "Fur service", "Furnace parts supplier", "Furnace repair service",
        "Furniture accessories supplier", "Furniture maker", "Furniture manufacturer", "Furniture rental service",
        "Furniture repair shop", "Garage builder", "Garage door supplier", "Garbage collection service",
        "Garbage dump service", "Garden building supplier", "Garden machinery supplier", "Gardener",
        "Garment exporter", "Gas company", "Gas engineer", "Gas installation service", "Gasfitter",
        "Gasket manufacturer", "Gazebo builder", "Gemologist", "Genealogist", "General contractor",
        "Geological research company", "Geological service", "Geotechnical engineer", "Gestalt therapist",
        "Glass blower", "Glass cutting service", "Glass engraving service", "Glass etching service",
        "Glass industry", "Glass manufacturer", "Glass merchant", "Glass repair service", "Glasses repair service",
        "Glassware manufacturer", "Glazier", "Goldfish store", "Goldsmith", "Golf course builder",
        "Golf instructor", "Graffiti removal service", "Graphic designer", "Greengrocer", "Gutter cleaning service",
        "Gutter service", "Hair extension technician", "Hair removal service", "Hair replacement service",
        "Hair salon", "Hair transplantation clinic", "Handyman/Handywoman/Handyperson", "Head start center",
        "Health consultant", "Health counselor", "Hearing aid repair service", "Heating contractor",
        "Heating equipment supplier", "Height works", "Helicopter charter", "Helicopter tour agency",
        "Heritage preservation", "Hiking guide", "Hindu priest", "Home automation company", "Home builder",
        "Home cinema installation", "Home help", "Home help service agency", "Home inspector",
        "Home insurance agency", "Home staging service", "Homekill service", "Homeless service",
        "Homeowners' association", "Horse breeder", "Horse rental service", "Horse trainer",
        "Horse transport supplier", "Horseshoe smith", "Houseboat rental service", "House cleaning service",
        "House clearance service", "House sitter", "House sitter agency", "Housing association",
        "Housing authority", "Housing cooperative", "Housing development", "Housing society", "Housing utility company",
        "Human resource consulting", "Hydraulic engineer", "Hydraulic equipment supplier", "Hydraulic repair service",
        "Hydroponics equipment supplier", "Hypnotherapy service", "Image consultant", "Immigration attorney",
        "Impermeabilization service", "Import export company", "Importer", "Industrial consultant",
        "Industrial design company", "Industrial engineer", "Information services", "Insolvency service",
        "Instrumentation engineer", "Insulation contractor", "Interior architect office", "Interior construction contractor",
        "Interior Decorator", "Interior designer", "Interior fitting contractor", "Internet cafe",
        "Internet marketing service", "Internet service provider", "Invitation printing service",
        "Iron steel contractor", "Iron ware dealer", "Iron works", "Irrigation equipment supplier",
        "Janitorial service", "Jeweler", "Jewelry appraiser", "Jewelry buyer", "Jewelry designer",
        "Jewelry engraver", "Jewelry equipment supplier", "Jewelry exporter", "Jewelry manufacturer",
        "Jewelry repair service", "Joiner", "Judicial auction", "Judicial scrivener", "Junk dealer",
        "Junk removal service", "Justice department", "Jute exporter", "Jute mill", "Key duplication service",
        "Kinesiologist", "Kinesiotherapist", "Knife manufacturer", "Knitwear manufacturer", "Laboratory equipment supplier",
        "Ladder supplier", "Laminating equipment supplier", "Lamination service", "Lamp repair service",
        "Land allotment", "Land surveyor", "Landscape architect", "Landscape designer", "Landscape lighting designer",
        "Landscaper", "Lapidary", "Laser cutting service", "Laser equipment supplier", "Laundromat",
        "Laundry", "Laundry service", "Law firm", "Lawn bowls club", "Lawn care service",
        "Lawn equipment rental service", "Lawn irrigation equipment supplier", "Lawn mower repair service",
        "Lawn sprinkler system contractor", "Lawyer", "Lawyers association", "Leasing service",
        "Leather cleaning service", "Leather goods manufacturer", "Leather goods supplier", "Leather repair service",
        "Legal services", "Life coach", "Life insurance agency", "Lighting consultant", "Lighting contractor",
        "Lighting manufacturer", "Limousine service", "Line marking service", "Liquidator", "Literacy program",
        "Live music venue", "Livery company", "Locksmith", "Loctician service", "Logging contractor",
        "Logistics service", "Longarm quilting service", "Luggage repair service", "Luggage storage facility",
        "Machine construction", "Machine knife supplier", "Machine maintenance service", "Machine repair service",
        "Machine shop", "Machine workshop", "Machinery parts manufacturer", "Machining manufacturer",
        "Magician", "Mailbox rental service", "Mailing machine supplier", "Mailing service", "Make-up artist",
        "Management school", "Manufacturer", "Mapping service", "Marine engineer", "Marine surveyor",
        "Market researcher", "Marketing agency", "Marketing consultant", "Marquee hire service",
        "Marriage celebrant", "Marriage or relationship counselor", "Masonry contractor", "Material handling equipment supplier",
        "Maternity store", "Measuring instruments supplier", "Mechanic", "Mechanical contractor",
        "Mechanical engineer", "Mechanical plant", "Media company", "Media consultant", "Media house",
        "Mediation service", "Meeting planning service", "Mehandi class", "Mehndi designer", "Memorial",
        "Memorial estate", "Memorial park", "Mercantile development", "Metal construction company",
        "Metal detecting equipment supplier",         "Metal fabricator", "Metal finisher", "Metal heat treating service", "Metal machinery supplier",
        "Metal polishing service", "Metal processing company", "Metal stamping service", "Metal working shop",
        "Metal workshop", "Metallurgy company", "Metalware dealer", "Metalware producer", "Microwave oven repair service",
        "Mill", "Millwork shop", "Mining consultant", "Mining engineer", "Mobile caterer", "Mobile disco service",
        "Mobile hairdresser", "Mobile home dealer", "Mobile home rental agency", "Mobile phone repair shop",
        "Mobility equipment supplier", "Model design company", "Model portfolio studio", "Modeling agency",
        "Modeling school", "Modular home builder", "Modular home dealer", "Mohel", "Mold maker",
        "Molding supplier", "Monogramming service", "Monument maker", "Moped dealer", "Mortgage broker",
        "Mortgage lender", "Mortuary", "Motor scooter repair shop", "Motoring club", "Mountain cable car",
        "Mountaineering class", "Movie rental kiosk", "Moving and storage service", "Moving company",
        "Multimedia and electronic book publisher", "Music producer", "Music publisher", "Musician",
        "Musician and composer", "Nanotechnology engineering service", "Natural stone exporter", "Natural stone supplier",
        "Natural stone wholesaler", "Needlework shop", "Neon sign shop", "News service", "Newspaper distribution service",
        "Newspaper publisher", "Non-governmental organization", "Non-profit organization", "Notary public",
        "Nuclear engineering service", "Nuclear power company", "Nuclear power plant", "Numerologist",
        "Convent", "Nursing agency", "Nursing association", "Nutritionist", "Occupational safety and health",
        "Office accessories wholesaler", "Office equipment rental service", "Office equipment repair service",
        "Office equipment supplier", "Office refurbishment service", "Office space rental agency", "Oil and gas exploration service",
        "Oil change service", "Oil & natural gas company", "Oil field equipment supplier", "Oil refinery",
        "Oilfield", "Olive oil bottling company", "Olive oil cooperative", "Olive oil manufacturer",
        "Optical instrument repair service", "Optical products manufacturer", "Orchid farm", "Orchid grower",
        "Organic farm", "Packaging company", "Packaging machinery", "Paint manufacturer", "Paint stripping service",
        "Painter", "Painting studio", "Paper bag supplier", "Paper distributor", "Paper exporter",
        "Paper mill", "Paper shredding machine supplier", "Paralegal services provider", "Parasailing ride operator",
        "Passport agent", "Patent attorney", "Paternity testing service", "Patio enclosure supplier",
        "Paving contractor", "Paving materials supplier", "Payroll service", "Personal chef service",
        "Personal concierge service", "Personal injury attorney", "Personal trainer", "Personal watercraft dealer",
        "Pest control service", "Pet adoption service", "Pet boarding service", "Pet cemetery",
        "Pet funeral service", "Pet groomer", "Pet moving service", "Pet sitter", "Pet trainer",
        "Petrochemical engineering service", "Petroleum products company", "Pharmaceutical company", "Pharmaceutical lab",
        "Photo agency", "Photo booth", "Photo lab", "Photo restoration service", "Photocopiers supplier",
        "Photographer", "Photography service", "Photography studio", "Piano maker", "Piano moving service",
        "Piano repair service", "Piano tuning service", "Pick your own farm produce", "Pile driving service",
        "Pilgrim hostel", "Pilgrimage place", "Piñatas supplier", "Pine furniture shop", "Pipe supplier",
        "Plast window store", "Plasterer", "Plastic bag supplier", "Plastic fabrication company",
        "Plastic injection molding service", "Plastic products supplier", "Plastic resin manufacturer",
        "Plastic products wholesaler", "Plating service", "Playground equipment supplier", "Plumber",
        "Plumbing supply store", "Plywood supplier", "Pneumatic tools supplier", "Podiatrist", "Polygraph service",
        "Polymer supplier", "Pond contractor", "Pond fish supplier", "Pond supply store", "Pool cleaning service",
        "Portable building manufacturer", "Portable toilet supplier", "Portrait studio", "POS terminal supplier",
        "Powder coating service", "Power station", "Power plant consultant", "Power plant equipment supplier",
        "Precision engineer", "Press advisory", "Pressure washing service", "Printer ink refill store",
        "Printer repair service", "Printing equipment supplier", "Private investigator", "Process server",
        "Professional and hobby associations", "Professional association", "Professional organizer", "Promotional products supplier",
        "Propane supplier", "Propeller shop", "Property administration service", "Property investment company",
        "Property maintenance", "Property management company", "Land registry office", "Protective clothing supplier",
        "Public relations firm", "Publisher", "Pump supplier", "PVC industry", "PVC windows supplier",
        "Pyrotechnician", "Quantity surveyor", "Quarry", "Radio broadcaster", "Railroad company",
        "Railroad contractor", "Railroad equipment supplier", "Railroad ties supplier", "Railway services",
        "Rainwater tank supplier", "Ready mix concrete supplier", "Real estate agency", "Real estate agent",
        "Real estate appraiser", "Real estate attorney", "Real estate auctioneer", "Real estate consultant",
        "Real estate developer", "Real estate rental agency", "Real estate surveyor", "Reclamation centre",
        "Record company", "Records storage facility", "Recording studio", "Recruiter", "Recycling center",
        "Recycling drop-off location", "Refrigerated transport service", "Refrigerator repair service", "Refugee camp",
        "Registered general nurse", "Rehearsal studio", "Religious destination", "Religious institution",
        "Religious lodging", "Religious organization", "Remodeler", "Rental car return location", "Renter's insurance agency",
        "Repair service", "Research and product development", "Research engineer", "Research foundation",
        "Research institute", "Residents association", "Resume service", "Retail space rental agency",
        "Retaining wall supplier", "Retirement community", "Retirement home", "Retreat center",
        "Rideshare pickup location", "River port", "Road construction company", "Road construction machine repair service",
        "Roads ports and canals engineers association", "Roofing contractor", "Roofing supply store",
        "Roommate referral service", "Rubber products supplier", "Rubber stamp store", "Safety equipment supplier",
        "Sailmaker", "Salvage dealer", "Salvage yard", "Sand & gravel supplier", "Sand plant",
        "Sandblasting service", "Sanitary inspection", "Sanitation service", "Satellite communication service",
        "Saw mill", "Saw sharpening service", "Scaffolding service", "Scaffolding rental service",
        "Scale repair service", "Scale supplier", "Scenography company", "School bus service",
        "Scientific equipment supplier", "Scooter rental service", "Scooter repair shop", "Scrap metal dealer",
        "Scrapbooking store", "Screen printer", "Screen printing shop", "Screen printing supply store",
        "Screen repair service", "Screen store", "Screw supplier", "Sculptor", "Seal shop",
        "Seaplane base", "Security guard service", "Security service", "Security system installation service",
        "Seed supplier", "Seitai", "Self service car wash", "Self service health station", "Self-storage facility",
        "Semi conductor supplier", "Septic system service", "Serviced accommodation", "Serviced apartment",
        "Sewage disposal service", "Sewage treatment plant", "Sewing machine repair service", "Sexologist",
        "Sharpening service", "Shed builder", "Sheep shearer", "Sheepskin and wool products supplier",
        "Sheet metal contractor", "Sheltered housing", "Shipbuilding and repair company", "Shipping and mailing service",
        "Shipping company", "Shipping equipment industry", "Shipping service", "Shipyard", "Shochu brewery",
        "Shoe factory", "Shoe repair shop", "Shoe shining service", "Shopfitter", "Short term apartment rental agency",
        "Shower door shop", "Shredding service", "Shrimp farm", "Siding contractor", "Sightseeing tour agency",
        "Sign shop", "Silk plant shop", "Silversmith", "Singing telegram service", "Skateboard park",
        "Ski rental service", "Ski repair service", "Skin care clinic", "Skylight contractor", "Slaughterhouse",
        "Sleep clinic", "Small appliance repair service", "Small claims assistance service", "Small engine repair service",
        "Smart locker manufacturer", "Smog inspection station", "Snow removal service", "Snowboard rental service",
        "Soapland", "Social services organization", "Social welfare center", "Social worker", "Societe de Flocage",
        "Sod supplier", "Software company", "Software training institute", "Soil testing service", "Solar energy company",
        "Solar energy system service", "Solar energy equipment supplier", "Solar hot water system supplier",
        "Solar panel maintenance service", "Solar photovoltaic power plant", "Solid fuel company", "Solid waste engineer",
        "Souvenir manufacturer", "Soy sauce maker", "Special educator", "Specialized clinic", "Spice exporter",
        "Spice wholesaler", "Spring supplier", "Stable", "Stage lighting equipment supplier", "Stained glass studio",
        "Stainless steel plant", "Stair contractor", "Stall installation service", "Stamp collectors club",
        "Stand bar", "State owned farm", "Statuary", "Steel construction company", "Steel distributor",
        "Steel erector", "Steel fabricator", "Steel framework contractor", "Steelwork design service",
        "Stereo repair service", "Stitching class", "Stock broker", "Stock exchange building",
        "Stone carving", "Stone cutter", "Stone supplier", "Storage facility", "Store equipment supplier",
        "Store fixture supplier", "Stringed instrument maker", "Structural engineer", "Stucco contractor",
        "Student career counseling office", "Student housing center", "Stylist", "Suburban train line",
        "Sugar factory", "Sugar shack", "Summer camp organizer", "Sunroom contractor", "Super public bath",
        "Superannuation consultant", "Superfund site", "Support group", "Surgeon", "Surgical center",
        "Surgical oncologist", "Surgical products wholesaler", "Surgical supply store", "Surinamese restaurant",
        "Surveyor", "Swimming pool contractor", "Swimming pool repair service", "Swimming pool supply store",
        "Tannery", "Tanning salon", "Tattoo and piercing shop", "Tattoo artist", "Tattoo removal service",
        "Tattoo shop", "Taxidermist", "Tea exporter", "Tea manufacturer", "Tea market place",
        "Tea wholesaler", "Telecommunications contractor", "Telecommunications engineer", "Telecommunications equipment supplier",
        "Telecommunications service provider", "Telemarketing service", "Telephone answering service", "Telephone company",
        "Telephone exchange", "Television repair service", "Tent rental service", "Tesla showroom",
        "Textile engineer", "Textile exporter", "Textile mill", "Thai massage therapist", "Theater supply store",
        "Theatrical costume supplier", "Thermal power plant", "Thread supplier", "Tile cleaning service",
        "Tile contractor", "Tile manufacturer", "Timeshare agency", "Tyre manufacturer", "Tire repair shop",
        "Tire service", "Title company", "Tobacco exporter", "Tobacco supplier", "Toner cartridge supplier",
        "Tool & die shop", "Tool grinding service", "Tool manufacturer", "Tool rental service",
        "Tool repair shop", "Tool wholesaler", "Toolroom", "Topography company", "Topsoil supplier",
        "Tour agency", "Tour operator", "Tourist information center", "Communications tower", "Towing equipment provider",
        "Towing service", "Toy and game manufacturer", "Toy manufacturer", "Trade fair construction company",
        "Trading card store", "Traditional market", "Trailer dealer", "Trailer manufacturer", "Trailer rental service",
        "Trailer repair shop", "Trailer supply store", "Train ticket agency", "Train ticket office",
        "Training centre", "Transcription service", "Transit depot", "Translation service", "Transmission shop",
        "Transportation escort service", "Transportation service", "Travel agency", "Travel lounge",
        "Tree farm", "Tree service", "Trophy shop", "Truck farmer", "Truck rental agency",
        "Truck repair shop", "Trucking company", "Truss manufacturer", "Tune up supplier", "Turf supplier",
        "Turnery", "Tutoring service", "Tuxedo shop", "Typewriter repair service", "Typing service",
        "Uniform store", "Hairdresser", "Upholstery cleaning service", "Upholstery shop", "Urgent care center",
        "Utility contractor", "Utility trailer dealer", "Holiday apartment", "Vacation home rental agency",
        "Vacuum cleaner repair shop", "Vacuum cleaning system supplier", "Valet parking service", "Van rental agency",
        "Vastu consultant", "VCR repair service", "Vehicle shipping agent", "Vehicle wrapping service",
        "Ventilating equipment manufacturer", "Veterans center", "Veterans organization", "Video camera repair service",
        "Video conferencing equipment supplier", "Video conferencing service", "Video duplication service",
        "Video editing service", "Video equipment repair service", "Video production service", "Villa",
        "Virtual office rental", "Visa consulting service", "Vocal instructor", "Volunteer organization",
        "Wallpaper installer", "Warehouse", "Washer & dryer repair service", "Waste management service",
        "Waste transfer station", "Watch manufacturer", "Watch repair service", "Water cooler supplier",
        "Water damage restoration service", "Water filter supplier", "Water jet cutting service",
        "Water mill", "Water pump supplier", "Water purification company", "Water skiing instructor",
        "Water skiing service", "Water softening equipment supplier", "Water sports equipment rental service",
        "Water tank cleaning service", "Water testing service", "Water treatment plant", "Water treatment supplier",
        "Water utility company", "Water works", "Water works equipment supplier", "Waterbed repair service",
        "Waterproofing service", "Wax supplier", "Waxing hair removal service", "Weather forecast service",
        "Weaving mill", "Web hosting company", "Website designer", "Wedding bakery", "Wedding buffet",
        "Wedding chapel", "Wedding dress rental service", "Wedding photographer", "Wedding planner",
        "Wedding service", "Wedding venue", "Welder", "Welding gas supplier", "Welding supply store",
        "Well drilling contractor", "Wellness hotel", "Wellness program", "Whale watching tour agency",
        "Wheel alignment service", "Wheelchair rental service", "Wheelchair repair service", "Wildlife rescue service",
        "Willow basket manufacturer", "Wind farm", "Wind turbine builder", "Window cleaning service",
        "Window installation service", "Window supplier", "Window tinting service", "Window treatment store",
        "Windsurfing store", "Wine wholesaler and importer", "Winemaking supply store", "Wire and cable supplier",
        "Wood and laminate flooring supplier", "Wood floor installation service", "Wood floor refinishing service",
        "Wood frame supplier", "Wood stove shop", "Woodworker", "Working women's hostel", "X-ray equipment supplier",
        "Yacht broker",

        # Transportation & Logistics
        "Aero dance class", "Aerobics instructor", "Aeroclub", "Aeromodel shop", "Aeronautical engineer",
        "Agistment service", "Air ambulance service", "Air compressor repair service", "Air compressor supplier",
        "Air force base", "Air taxi", "Aircraft dealer", "Aircraft manufacturer", "Aircraft rental service",
        "Aircraft supply store", "Airline", "Airline ticket agency", "Airplane", "Airport",
        "Airport shuttle service", "Airsoft supply store", "Airstrip", "ATV dealer", "ATV rental service",
        "ATV repair shop", "Bicycle club", "Bicycle rack", "Bicycle rental service", "Bicycle repair shop",
        "Bicycle wholesaler", "Bike wash", "Bus charter", "Bus company", "Bus depot", "Bus ticket agency",
        "Bus tour agency", "Bus station", "Car rental agency", "Chauffeur service", "Compressed natural gas station",
        "Courier service", "Cruise agency", "Cruise line company", "Cruise terminal", "Cycle rickshaw stand",
        "Distribution service", "Ferry service", "Freight forwarding service", "Gas station", "Helicopter charter",
        "Helicopter tour agency", "Heliport", "Limousine service", "Logistics service", "Marina",
        "Metropolitan train company", "Minibus taxi service", "Mobile money agent", "Mobile network operator",
        "Motor scooter dealer", "Motorcycle rental agency", "Motoring club", "Moving and storage service",
        "Moving company", "Parking garage", "Parking lot", "Parking lot for bicycles", "Parking lot for motorcycles",
        "Personal watercraft dealer", "Public parking space", "Recreational vehicle rental agency", "Rideshare pickup location",
        "River port", "RV park", "Scooter rental service", "Seaplane base", "Shipping and mailing service",
        "Shipping company", "Shipping service", "Subway station", "Taxi service", "Taxi stand",
        "Toll gate", "Toll road rest stop", "Toll station", "Train depot", "Train station",
        "Train ticket agency", "Train ticket office", "Train yard", "Transportation escort service",
        "Transportation service", "Travel agency", "Trucking company", "Tsukigime parking lot", "Van rental agency",
        "Vehicle shipping agent", "Weir",

        # Religious Facilities
        "Abbey", "Alliance church", "Anglican church", "Apostolic church", "Armenian church", "Ashram",
        "Assemblies of God church", "Bahá'í house of worship", "Baptist church", "Basilica", "Buddhist temple",
        "Calvary Chapel church", "Cathedral", "Catholic cathedral", "Catholic church", "Chapel",
        "Christian church", "Church", "Church council office", "Church of Christ", "Church of Jesus Christ of Latter-day Saints",
        "Church of the Nazarene", "Church supply store", "City pillar shrine", "Congregation", "Conservative synagogue",
        "Deaf church", "Disciples of Christ Church", "Eastern Orthodox Church", "Episcopal church", "Evangelical church",
        "Foursquare church", "Full Gospel church", "Gereja", "Gospel church", "Greek Orthodox church",
        "Gurudwara", "Hindu temple", "Hispanic church", "Jain temple", "Jehovah's Witness Kingdom Hall",
        "Korean church", "Lutheran church", "Masjid", "Mennonite church", "Messianic synagogue",
        "Methodist church", "Mission", "Monastery", "Moravian church", "Mosque", "Musalla",
        "New Age church", "Non-denominational church", "Orthodox church", "Orthodox synagogue", "Parish",
        "Parsi temple", "Pentecostal church", "Presbyterian church", "Priest", "Protestant church",
        "Pura", "Quaker church", "Rectory", "Reform synagogue", "Reformed church", "Religious destination",
        "Religious institution", "Religious lodging", "Religious organization", "Religious school", "Seminary",
        "Seventh-day Adventist church", "Shinto shrine", "Shrine", "Spiritist center", "Synagogue",
        "Taoist temple", "Temple", "Unitarian Universalist Church", "United Church of Canada", "United Church of Christ",
        "United Methodist church", "Unity church", "Vihara", "Vineyard church", "Wesleyan church",

        # Manufacturing & Industrial
        "Abrasives supplier", "Aerated drinks supplier", "Aggregate supplier", "Agricultural machinery manufacturer",
        "Agricultural product wholesaler", "Agricultural production", "Air compressor supplier", "Air filter supplier",
        "Aircraft manufacturer", "Alcohol manufacturer", "Alcoholic beverage wholesaler", "Alternator supplier",
        "Aluminum supplier", "Aluminum frames supplier", "Ammunition supplier", "Amusement machine supplier",
        "Amusement ride supplier", "Animal feed store", "Appliance parts supplier", "Architectural and engineering model maker",
        "Artificial plant supplier", "Asphalt mixing plant", "Audio visual equipment supplier", "Auto accessories wholesaler",
        "Auto body parts supplier", "Auto parts manufacturer", "Automotive storage facility", "Bakery equipment",
        "Baking supply store", "Bar stool supplier", "Barber supply store", "Basketball court contractor",
        "Battery manufacturer", "Battery wholesaler", "Bearing supplier", "Beauty products wholesaler",
        "Bed shop", "Beer distributor", "Beverage distributor", "Bicycle wholesaler", "Biochemical supplier",
        "Biotechnology company", "Boat accessories supplier", "Boat builders", "Boat cover supplier",
        "Boiler manufacturer", "Boiler supplier", "Books wholesaler", "Bottled water supplier", "Box lunch supplier",
        "Brick manufacturer", "Brewing supply store", "Building equipment hire service", "Building materials supplier",
        "Butane gas supplier", "Cable company", "Cake decorating equipment shop", "Camera store", "Camper shell supplier",
        "Cannery", "Car accessories store", "Car alarm supplier", "Car factory", "Car manufacturer",
        "Catering equipment rental service", "Catering food and drink supplier", "Cement manufacturer", "Cement supplier",
        "Ceramic manufacturer", "Ceramics wholesaler", "Chemical exporter", "Chemical manufacturer", "Chemical plant",
        "Chemical wholesaler", "Cheese manufacturer", "Chocolate factory", "Cider mill", "Cinema equipment supplier",
        "Clock and watch maker", "Clothes and fabric manufacturer", "Clothes and fabric wholesaler", "Clothing supplier",
        "Clothing wholesale market place", "Clothing wholesaler", "Coffee machine supplier", "Coffee roasters",
        "Coffee vending machine", "Coffee wholesaler", "Coffin supplier", "Commercial refrigerator supplier",
        "Computer accessories store", "Computer hardware manufacturer", "Computer software store", "Computer wholesaler",
        "Concrete factory", "Concrete metal framework supplier", "Concrete product supplier", "Condiments supplier",
        "Confectionery wholesaler", "Construction equipment supplier", "Construction machine dealer", "Construction material wholesaler",
        "Container supplier", "Containers supplier", "Copying supply store", "Corporate gift supplier",
        "Cosmetic products manufacturer", "Cosmetics industry", "Cosmetics wholesaler", "Cotton exporter",
        "Cotton mill", "Cotton supplier", "Crane dealer", "Crushed stone supplier", "Custom label printer",
        "Dairy", "Dairy farm", "Dairy farm equipment supplier", "Dairy supplier", "Data center",
        "Decal supplier", "Diesel engine dealer", "Diesel fuel supplier", "Digital printer", "Dirt supplier",
        "Disability equipment supplier", "Display stand manufacturer", "Disposable tableware supplier", "Door manufacturer",
        "Door supplier", "Door warehouse", "Drafting equipment supplier", "Dry ice supplier", "Dry wall supply store",
        "Dye store", "Dynamometer supplier", "Egg supplier", "EFTPOS equipment supplier", "Electric motor store",
        "Electric utility company", "Electric vehicle charging station", "Electrical appliance wholesaler",
        "Electrical equipment supplier", "Electrical products wholesaler", "Electrical substation", "Electronic parts supplier",
        "Electronics accessories wholesaler", "Electronics company", "Electronics manufacturer", "Electronics vending machine",
        "Electronics wholesaler", "Elevator manufacturer", "Energy equipment and solutions", "Energy supplier",
        "Engine rebuilding service", "Equipment exporter", "Equipment importer", "Equipment supplier",
        "Fabric product manufacturer", "Fabric wholesaler", "Factory equipment supplier", "Farm equipment supplier",
        "Fastener supplier", "Feed manufacturer", "Fertilizer supplier", "Fiber optic products supplier",
        "Fiberglass supplier", "Filter supplier", "Fire alarm supplier", "Fire protection equipment supplier",
        "Fire protection system supplier", "Fireplace manufacturer", "Firewood supplier", "Fireworks supplier",
        "Fish processing", "Fitted furniture supplier", "Flavours fragrances and aroma supplier", "Flour mill",
        "FMCG goods wholesaler", "FMCG manufacturer", "Foam rubber producer", "Foam rubber supplier",
        "Food machinery supplier", "Food manufacturer", "Food manufacturing supply", "Food processing company",
        "Food processing equipment", "Food products supplier", "Food seasoning manufacturer", "Forklift dealer",
        "Foundry", "Fresh food market", "Frozen dessert supplier", "Frozen food manufacturer", "Fruit and vegetable processing",
        "Fruit and vegetable wholesaler", "Fruit wholesaler", "Fruits wholesaler", "Fuel supplier",
        "Fur manufacturer", "Furnace parts supplier", "Furniture accessories", "Furniture accessories supplier",
        "Furniture manufacturer", "Furniture wholesaler", "Garage door supplier", "Garden building supplier",
        "Garden machinery supplier", "Garment exporter", "Gas cylinders supplier", "Gas logs supplier",
        "Gasket manufacturer", "Generator shop", "Glass block supplier", "Glass industry", "Glass manufacturer",
        "Glass merchant", "Glassware manufacturer", "Glassware wholesaler", "GPS supplier", "Granite supplier",
        "Gravel pit", "Gravel plant", "Green energy supplier", "Gypsum product supplier", "Hair extensions supplier",
        "Handicraft exporter", "Handicrafts wholesaler", "Hardware store", "Hay supplier", "Heating equipment supplier",
        "Heating oil supplier", "Helium gas supplier", "Hose supplier", "Hot water system supplier",
        "Hotel supply store", "Household chemicals supplier", "Household goods wholesaler", "Hub cap supplier",
        "HVAC contractor", "Hydraulic equipment supplier", "Hydroelectric power plant", "Hydroponics equipment supplier",
        "Hygiene articles wholesaler", "Ice cream equipment supplier", "Ice supplier", "Incense supplier",
        "Incineration plant", "Industrial chemicals wholesaler", "Industrial equipment supplier", "Industrial gas supplier",
        "Industrial supermarket", "Industrial technical engineers association", "Industrial vacuum equipment supplier",
        "Insulation materials store", "Insulator supplier", "Internet service provider", "Iron ware dealer",
        "Iron works", "Irrigation equipment supplier", "Janitorial equipment supplier", "Jewelry equipment supplier",
        "Jewelry exporter", "Jewelry manufacturer", "Jute exporter", "Jute mill", "Kerosene supplier",
        "Knife manufacturer", "Knitwear manufacturer", "Laboratory equipment supplier", "Ladder supplier",
        "Laminating equipment supplier", "Lamp shade supplier", "Landscaping supply store", "Laser equipment supplier",
        "Lawn equipment rental service", "Lawn irrigation equipment supplier", "Leather coats store", "Leather exporter",
        "Leather goods manufacturer", "Leather goods supplier", "Leather goods wholesaler", "Leather wholesaler",
        "License plate frames supplier", "Light bulb supplier", "Lighting manufacturer", "Lighting products wholesaler",
        "Linens store", "Lingerie manufacturer", "Lingerie wholesaler", "Livestock auction house", "Livestock breeder",
        "Livestock dealer", "Livestock producer", "Locks supplier", "Luggage wholesaler", "Machine construction",
        "Machine knife supplier", "Machine workshop", "Machinery parts manufacturer", "Machining manufacturer",
        "Mailing machine supplier", "Mailbox supplier", "Manufacturing plant", "Match box manufacturer",
        "Material handling equipment supplier", "Measuring instruments supplier", "Meat packer", "Meat processor",
        "Meat wholesaler", "Medical equipment manufacturer", "Medical equipment supplier", "Medical technology manufacturer",
        "Medicine exporter", "Metal construction company", "Metal detecting equipment supplier", "Metal heat treating service",
        "Metal industry suppliers", "Metal machinery supplier", "Metal supplier", "Metallurgy company",
        "Metalware dealer", "Metalware producer", "Mill", "Millwork shop", "Mine", "Mineral water company",
        "Mineral water wholesaler", "Mining company", "Mining equipment", "Mobile network operator", "Model train store",
        "Molding supplier", "Motor vehicle dealer", "Motorcycle dealer", "Motorcycle parts store", "Motorsports store",
        "Mulch supplier", "Multimedia and electronic book publisher", "Music publisher", "Musical instrument manufacturer",
        "Natural stone exporter", "Natural stone supplier", "Natural stone wholesaler", "Novelties wholesaler",
        "Nuclear power company", "Nuclear power plant", "Office accessories wholesaler", "Office equipment supplier",
        "Office supply wholesaler", "Oil and gas exploration service", "Oil & natural gas company", "Oil field equipment supplier",
        "Oil refinery", "Oil wholesaler", "Oilfield", "Olive oil bottling company", "Olive oil cooperative",
        "Olive oil manufacturer", "Optical products manufacturer", "Optical wholesaler", "Outboard motor store",
        "Oxygen equipment supplier", "Oyster supplier", "Packaging company", "Packaging machinery", "Paint manufacturer",
        "Pallet supplier", "Paper bag supplier", "Paper distributor", "Paper exporter", "Paper mill",
        "Paper shredding machine supplier", "Paving materials supplier", "Perfume store", "Personal watercraft dealer",
        "Petrochemical engineering service", "Petroleum products company", "Pharmaceutical company", "Pharmaceutical lab",
        "Pharmaceutical products wholesaler", "Photocopiers supplier", "Piano maker", "Piñatas supplier",
        "Pine furniture shop", "Pipe supplier", "Plastic bag supplier", "Plastic bags wholesaler",
        "Plastic fabrication company", "Plastic injection molding service", "Plastic products supplier",
        "Plastic resin manufacturer", "Plastic products wholesaler", "Plywood supplier", "Pneumatic tools supplier",
        "Pond fish supplier", "Pond supply store", "Portable building manufacturer", "Portable toilet supplier",
        "POS terminal supplier", "Powder coating service", "Power station", "Power plant equipment supplier",
        "Precision engineer", "Printed music publisher", "Printer ink refill store", "Printing equipment and supplies",
        "Printing equipment supplier", "Produce wholesaler", "Foie gras producer", "Promotional products supplier",
        "Propane supplier", "Propeller shop", "Protective clothing supplier", "Pump supplier", "PVC industry",
        "PVC windows supplier", "Quarry", "Radio broadcaster", "Railroad equipment supplier", "Railroad ties supplier",
        "Rainwater tank supplier", "Ready mix concrete supplier", "Record company", "Refrigerated transport service",
        "Retaining wall supplier", "Rice mill", "Rice wholesaler", "Rolled metal products supplier",
        "Rubber products supplier", "Safety equipment supplier", "Sake brewery", "Sand & gravel supplier",
        "Sand plant", "Scale supplier", "Scientific equipment supplier", "Screw supplier", "Seal shop",
        "Seat dealer", "Seating systems provider", "Seed supplier", "Semi conductor supplier", "Sheepskin and wool products supplier",
        "Sheet metal contractor", "Shelving store", "Shipbuilding and repair company", "Shipping equipment industry",
        "Shochu brewery", "Shoe factory", "Footwear wholesaler", "Silk store", "Silversmith",
        "Smart locker manufacturer", "Sod supplier", "Software company", "Solar energy equipment supplier",
        "Solar hot water system supplier", "Solid fuel company", "Souvenir manufacturer", "Soy sauce maker",
        "Spice exporter", "Spice wholesaler", "Sports accessories wholesaler", "Sportwear manufacturer",
        "Spring supplier", "Stage lighting equipment supplier", "Stainless steel plant", "Staple food package",
        "Stationery manufacturer", "Stationery wholesaler", "Steel construction company", "Steel distributor",
        "Steel erector", "Steel fabricator", "Steel framework contractor", "Sticker manufacturer",
        "Stone supplier", "Stringed instrument maker", "Sugar factory", "Swimming pool supply store",
        "Tea exporter", "Tea manufacturer", "Tea wholesaler", "Telecommunications equipment supplier",
        "Telephone company", "Textile engineer", "Textile exporter", "Textile mill", "Theater supply store",
        "Theatrical costume supplier", "Thermal power plant", "Thread supplier", "Threads and yarns wholesaler",
        "Tile manufacturer", "Tobacco exporter", "Tobacco supplier", "Toner cartridge supplier",
        "Tool manufacturer", "Tool wholesaler", "Toy and game manufacturer", "Toy manufacturer",
        "Tractor dealer", "Trailer dealer", "Trailer manufacturer", "Truck dealer", "Truss manufacturer",
        "Turf supplier", "Turnery", "Tyre manufacturer", "Vacuum cleaning system supplier", "Vegetable wholesaler",
        "Ventilating equipment manufacturer", "Video equipment repair service", "Water cooler supplier",
        "Water filter supplier", "Water pump supplier", "Water purification company", "Water treatment supplier",
        "Water works equipment supplier", "Wax supplier", "Weaving mill", "Welding gas supplier",
        "Wholesale bakery", "Wholesale drugstore", "Wholesale florist", "Wholesale food store", "Wholesale grocer",
        "Wholesale jeweler", "Wholesale market", "Wholesale plant nursery", "Wholesaler", "Wholesaler household appliances",
        "Willow basket manufacturer", "Wind farm", "Wind turbine builder", "Wine wholesaler and importer",
        "Winemaking supply store", "Wire and cable supplier", "Wood and laminate flooring supplier",
        "Wood frame supplier", "Wood supplier", "Woodworking supply store", "X-ray equipment supplier",

        # Agriculture & Farming
        "Agricultural association", "Agricultural cooperative", "Agricultural engineer", "Agricultural high school",
        "Agricultural machinery manufacturer", "Agricultural organization", "Agricultural product wholesaler",
        "Agricultural production", "Agricultural service", "Agrochemicals supplier", "Aquaculture farm",
        "Cattle farm", "Cattle market", "Chamber of agriculture", "College of agriculture", "Cotton exporter",
        "Cotton mill", "Cotton supplier", "Crop grower", "Dairy", "Dairy farm", "Dairy farm equipment supplier",
        "Dairy supplier", "Egg supplier", "Farm", "Farm bureau", "Farm equipment repair service",
        "Farm equipment supplier", "Farm household tour", "Farm school", "Farm shop", "Farmers' market",
        "Farmstay", "Feed manufacturer", "Animal feed store", "Fish farm", "Flour mill", "Fruit and vegetable processing",
        "Hay supplier", "Honey farm", "Livestock auction house", "Livestock breeder", "Livestock dealer",
        "Livestock producer", "Mulch supplier", "Orchard", "Orchid farm", "Orchid grower", "Organic farm",
        "Pick your own farm produce", "Pig farm", "Plant nursery", "Poultry farm", "Rice mill",
        "Seafood farm", "Seed supplier", "Shrimp farm", "State owned farm", "Tree farm",
        "Truck farmer", "Vineyard", "Wholesale plant nursery",

        # Utilities & Infrastructure
        "Alternative fuel station", "Asphalt mixing plant", "Cable company", "Cement manufacturer",
        "CNG fitment center", "Compressed natural gas station", "Concrete factory", "Container terminal",
        "Cruise terminal", "Desalination plant", "Electric utility company", "Electric vehicle charging station",
        "Electrical substation", "Energy supplier", "Filtration plant", "Gas company", "Gas station",
        "Green energy supplier", "Hydroelectric power plant", "Incineration plant", "Internet service provider",
        "Mobile network operator", "Nuclear power company", "Nuclear power plant", "Oil refinery",
        "Power station", "Public utility company", "Railroad company", "Satellite communication service",
        "Sewage disposal service", "Sewage treatment plant", "Solar energy company", "Solar photovoltaic power plant",
        "Solid fuel company", "Telecommunications service provider", "Telephone company", "Telephone exchange",
        "Thermal power plant", "Toll station", "Transit depot", "Water mill", "Water purification company",
        "Water treatment plant", "Water utility company", "Water works", "Waste management service",
        "Waste transfer station", "Wi-Fi spot", "Wind farm"
    ]  
    
    # Create work queue
    work_queue = []
    for area in target_areas:
        for category in poi_categories:
            work_queue.append((category, area, len(work_queue), max_results_per_category, {
                'headless': headless,
                'driver_path': kwargs.get('driver_path'),
                'proxy': kwargs.get('proxy')
            }))
    
    print(f"Total tasks to process: {len(work_queue)}")
    
    # Process with threads instead of processes
    all_results = []
    completed_tasks = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_task = {
            executor.submit(worker_scrape_category, task): task 
            for task in work_queue
        }
        
        # Process completed tasks
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            category, area, task_id = task[0], task[1], task[2]
            
            try:
                results = future.result(timeout=300)
                all_results.extend(results)
                completed_tasks += 1
                
                print(f"✓ Completed {completed_tasks}/{len(work_queue)}: {category} in {area} ({len(results)} POIs)")
                
                # Save progress every 10 completed tasks
                if completed_tasks % 10 == 0:
                    save_parallel_progress(all_results, completed_tasks)
                    
            except Exception as e:
                print(f"✗ Failed task {task_id}: {category} in {area} - {e}")
                # Retry the failed task
                print(f"🔄 Retrying task {task_id}...")
                retry_results = retry_failed_task(task)
                if retry_results:
                    all_results.extend(retry_results)
                    print(f"✓ Retry successful for {category} in {area}")
                else:
                    print(f"✗ Retry also failed for {category} in {area}")
    
    print(f"Thread-based parallel scraping completed. Total POIs collected: {len(all_results)}")
    return convert_results_to_dataframe(all_results)

def calculate_optimal_workers():
    """Calculate optimal number of workers based on system resources"""
    import psutil
    import os
    
    # Get system specs
    cpu_cores = psutil.cpu_count(logical=False)  # Physical cores
    logical_cores = psutil.cpu_count(logical=True)  # Logical cores
    total_memory_gb = psutil.virtual_memory().total / (1024**3)
    available_memory_gb = psutil.virtual_memory().available / (1024**3)
    
    print(f"System Resources:")
    print(f"- Physical CPU cores: {cpu_cores}")
    print(f"- Logical CPU cores: {logical_cores}")
    print(f"- Total RAM: {total_memory_gb:.1f} GB")
    print(f"- Available RAM: {available_memory_gb:.1f} GB")
    
    # Memory-based calculation (Chrome uses ~200-500MB per instance)
    memory_based_workers = max(1, int(available_memory_gb / 0.5))  # 500MB per worker
    
    # CPU-based calculation
    cpu_based_workers = max(1, logical_cores - 1)  # Leave 1 core for system
    
    # Network-based consideration (Google Maps rate limiting)
    network_based_workers = 8  # Conservative estimate for Google Maps
    
    # Conservative calculation
    optimal_workers = min(memory_based_workers, cpu_based_workers, network_based_workers)
    
    print(f"\nWorker Calculations:")
    print(f"- Memory-based max workers: {memory_based_workers}")
    print(f"- CPU-based max workers: {cpu_based_workers}")
    print(f"- Network-safe max workers: {network_based_workers}")
    print(f"- Recommended optimal workers: {optimal_workers}")
    
    return optimal_workers

def test_system_capacity(test_workers=None):
    """Test system capacity with different worker counts"""
    if test_workers is None:
        test_workers = calculate_optimal_workers()
    
    print(f"\n🧪 Testing system capacity with {test_workers} workers...")
    
    # Monitor system during test
    import threading
    import time
    
    def monitor_resources():
        """Monitor system resources during test"""
        start_time = time.time()
        peak_memory = 0
        peak_cpu = 0
        
        while time.time() - start_time < 30:  # Monitor for 30 seconds
            memory_percent = psutil.virtual_memory().percent
            cpu_percent = psutil.cpu_percent(interval=1)
            
            peak_memory = max(peak_memory, memory_percent)
            peak_cpu = max(peak_cpu, cpu_percent)
            
            print(f"📊 Memory: {memory_percent:.1f}% | CPU: {cpu_percent:.1f}%")
            
            if memory_percent > 90:
                print("⚠️  WARNING: Memory usage > 90%")
            if cpu_percent > 95:
                print("⚠️  WARNING: CPU usage > 95%")
                
        print(f"📈 Peak Memory: {peak_memory:.1f}% | Peak CPU: {peak_cpu:.1f}%")
        return peak_memory, peak_cpu
    
    # Start monitoring in background
    monitor_thread = threading.Thread(target=monitor_resources)
    monitor_thread.daemon = True
    monitor_thread.start()
    
    # Test with actual browser instances
    drivers = []
    try:
        print(f"Starting {test_workers} browser instances...")
        for i in range(test_workers):
            try:
                driver = initialize_driver(headless=True, worker_id=i)
                driver.get("https://maps.google.com")
                drivers.append(driver)
                print(f"✅ Worker {i+1}/{test_workers} started successfully")
                time.sleep(1)  # Stagger startup
            except Exception as e:
                print(f"❌ Worker {i+1} failed to start: {e}")
                break
        
        successful_workers = len(drivers)
        print(f"\n🎯 Successfully started {successful_workers}/{test_workers} workers")
        
        if successful_workers < test_workers:
            print(f"⚠️  Recommended max workers: {successful_workers}")
        
        # Let them run for a bit to test stability
        print("Testing stability for 10 seconds...")
        time.sleep(10)
        
        return successful_workers
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return 0
    finally:
        # Cleanup
        print("Cleaning up test browsers...")
        for i, driver in enumerate(drivers):
            try:
                driver.quit()
                print(f"✅ Closed worker {i+1}")
            except:
                pass

def main():
    import argparse

    args = type('Args', (), {
        'mode': 'threads',
        'areas': ['Jakarta'],
        'max_workers': 2,
        'max_per_category': 5,
        'headless': True
    })()
    
    parser = argparse.ArgumentParser(description='Parallel Google Maps POI Scraper')
    parser.add_argument('--mode', choices=['single', 'parallel', 'threads', 'national', 'test'], default='single',
                      help='Scraping mode')
    parser.add_argument('--areas', nargs='+', default=["Malang"],
                      help='Target areas for scraping')
    parser.add_argument('--max-workers', type=int, default=None,
                      help='Maximum number of parallel workers')
    parser.add_argument('--max-per-category', type=int, default=20,
                      help='Maximum POIs per category')
    parser.add_argument('--headless', action='store_true', default=True,
                      help='Run in headless mode')
    parser.add_argument('--auto-workers', action='store_true',
                      help='Automatically determine optimal workers')
    parser.add_argument('--test-workers', type=int, default=None,
                      help='Test system with specific number of workers')
    
    args = parser.parse_args()
    
    # Auto-determine workers if requested
    if args.auto_workers or args.max_workers is None:
        optimal_workers = calculate_optimal_workers()
        if args.max_workers is None:
            args.max_workers = optimal_workers
        print(f"🎯 Using {args.max_workers} workers")
    
    # Test mode
    if args.mode == 'test':
        if args.test_workers:
            successful_workers = test_system_capacity(args.test_workers)
        else:
            successful_workers = test_system_capacity()
        print(f"\n🎯 Your system can handle {successful_workers} workers safely")
        return
    
    if args.mode == 'threads':
        print(f"🚀 Starting Thread-based Parallel Scraping for: {args.areas}")
        df = scrape_pois_parallel_threads(
            target_areas=args.areas,
            max_results_per_category=args.max_per_category,
            max_workers=args.max_workers,
            headless=args.headless
        )
    elif args.mode == 'parallel':
        print(f"🚀 Starting Process-based Parallel Scraping for: {args.areas}")
        df = scrape_pois_parallel(
            target_areas=args.areas,
            max_results_per_category=args.max_per_category,
            max_workers=args.max_workers,
            headless=args.headless
        )
    elif args.mode == 'national':
        print("🚀 Starting National POI Scraping...")
        df = scrape_national_pois(
            max_workers=args.max_workers or 6,
            max_results_per_category=args.max_per_category
        )
    else:
        # Single area scraping
        print(f"🚀 Starting Single Area POI Scraping for: {args.areas[0]}")
        df = scrape_pois_in_malang(
            target_area=args.areas[0],
            max_results_per_category=args.max_per_category,
            headless=args.headless
        )
    
    # Save and analyze results
    if len(df) > 0:
        os.makedirs('results', exist_ok=True)
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        
        csv_filename = f"results/poi_results_{timestamp}.csv"
        df.to_csv(csv_filename, index=False, encoding='utf-8')
        print(f"Results saved to {csv_filename}")
        
        print(f"\n✅ Scraping completed successfully!")
        print(f"Total POIs collected: {len(df)}")
        analyze_collected_data(df)
    else:
        print("No results were collected.")

if __name__ == "__main__":
    # Essential for multiprocessing on Windows
    mp.freeze_support()
    
    try:
        print("=" * 60)
        print("GOOGLE MAPS POI SCRAPER FOR BANKING EXPANSION ANALYSIS")
        print("=" * 60)
        print("Version: 2.0 - Parallel Edition")
        print("Date: May 2025")
        print("=" * 60)
        main()
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Any progress should have been saved.")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        if 'global_data' in globals() and len(global_data.get('names', [])) > 0:
            print("Attempting to save collected data before exit...")
            save_current_progress()
        print("\nScript terminated with error.")

# Usage examples:
# Basic usage:
# python national_poi.py --mode threads --areas Palmerah Sudirman --max-workers 5 --max-per-category 1000