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

# Global data dictionary to track progress
def initialize_global_data():
    """Initialize global data structure with additional tracking for duplicates and new columns including kecamatan"""
    return {
        'names': [],
        'categories': [],
        'bank_names': [],
        'addresses': [],
        'cities': [],  # Base city names (e.g., "Palembang")
        'city_types': [],  # NEW: Full format (e.g., "Kota Palembang")
        'kecamatans': [],  # NEW: District names
        'zip_codes': [],
        'provinces': [],
        'links': [],
        'latitudes': [],
        'longitudes': [],
        'ratings': [],
        'reviews_counts': [],
        'phones': [],
        'websites': [],
        'opening_hours': [],
        'stars_1': [],
        'stars_2': [],
        'stars_3': [],
        'stars_4': [],
        'stars_5': [],
        'processed_elements': set(),
        'seen_name_pairs': set(),
        'seen_links': set(),
        'seen_coordinates': set(),
        'search_query': "",
        'coordinates_searched': set(),
        'target_area': "",
        'last_coords': None,
        'bank_index': 0,
        'city_index': 0,
        'keyword_index': 0,
        'search_phase': 'primary_area',
        'banks': [],
        'search_cities': [],
        'keywords': []
    }
    
def initialize_driver(headless=False, driver_path=None, proxy=None):
    """Initialize and configure the Chrome WebDriver"""
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-speech-api")
    chrome_options.add_argument("--disable-features=Translate,WebXR,WebSpeech,MediaRouter,OptimizationHints")
    chrome_options.add_argument("--window-size=1280,800")
    chrome_options.add_argument("--disable-application-cache")
    chrome_options.add_argument("--disk-cache-size=0")
    chrome_options.add_argument("--media-cache-size=0")
    chrome_options.add_argument("--lang=id")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Add options to prevent popups and notifications
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-infobars")
    
    if headless:
        chrome_options.add_argument("--headless")
        
    if proxy:
        chrome_options.add_argument(f'--proxy-server={proxy}')
    
    try:
        temp_dir = os.path.join(os.getcwd(), "temp_browser_data")
        os.makedirs(temp_dir, exist_ok=True)
        chrome_options.add_argument(f"--user-data-dir={temp_dir}")
    except Exception as e:
        print(f"Warning: Could not create custom cache directory: {e}")
    
    if driver_path:
        service = Service(driver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
    else:
        driver = webdriver.Chrome(options=chrome_options)
    
    driver.set_page_load_timeout(30)
    driver.delete_all_cookies()
    
    return driver

def extract_coordinates_from_link(link):
    """
    Mengekstrak latitude dan longitude dari link Google Maps
    Format: !8m2!3d[latitude]!4d[longitude]
    """
    try:
        # Pattern untuk mencari koordinat dalam format Google Maps
        lat_lng_pattern = r'!8m2!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)'
        match = re.search(lat_lng_pattern, link)
        
        if match and len(match.groups()) == 2:
            latitude = float(match.group(1))
            longitude = float(match.group(2))
            return latitude, longitude
        
        # Jika pattern pertama tidak cocok, coba pattern kedua (untuk URL baru)
        # Format: @[latitude],[longitude]
        alt_pattern = r'@(-?\d+\.\d+),(-?\d+\.\d+)'
        alt_match = re.search(alt_pattern, link)
        
        if alt_match and len(alt_match.groups()) == 2:
            latitude = float(alt_match.group(1))
            longitude = float(alt_match.group(2))
            return latitude, longitude
        
        return None, None
    except Exception as e:
        print(f"Error extracting coordinates: {e}")
        return None, None

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

def extract_zip_code(address):
    """Extract ZIP code from address"""
    if not address or address == "Address not available":
        return None
    
    # Pattern untuk kode pos Indonesia (5 digit)
    zip_pattern = r'\b\d{5}\b'
    match = re.search(zip_pattern, address)
    
    if match:
        return match.group(0)
    
    return None

def extract_kecamatan_from_address(address):
    """
    Extract kecamatan (district) information from address
    Returns the kecamatan name or None if not found
    """
    if not address or address == "Address not available":
        return None
    
    # Clean up address for better parsing
    address = address.replace("Kec.", "Kecamatan").replace("Kab.", "Kabupaten")
    
    print(f"DEBUG Kecamatan: Processing address: {address}")
    
    # Pattern untuk mencari kecamatan
    kecamatan_patterns = [
        r'(?:^|,\s*)Kecamatan\s+([A-Za-z\s]+?)(?:,|$)',  # "Kecamatan Bandung" di awal atau setelah koma
        r',\s*Kecamatan\s+([A-Za-z\s]+?)(?:,|$)',         # ", Kecamatan Bandung"
        r'(?:^|,\s*)([A-Za-z\s]+?)\s*Kecamatan(?:,|$)',   # "Bandung Kecamatan" atau "Bandung, Kecamatan"
        r',\s*([A-Za-z\s]+?)\s*Kecamatan(?:,|$)'          # ", Bandung Kecamatan"
    ]
    
    for pattern in kecamatan_patterns:
        kecamatan_matches = re.findall(pattern, address, re.IGNORECASE)
        for match in kecamatan_matches:
            kecamatan_name = match.strip()
            # Validasi nama kecamatan
            if (len(kecamatan_name) >= 3 and 
                not re.search(r'\d', kecamatan_name) and  # Tidak boleh ada angka
                "jalan" not in kecamatan_name.lower() and 
                "jl." not in kecamatan_name.lower() and
                "no." not in kecamatan_name.lower() and
                "gang" not in kecamatan_name.lower()):
                
                print(f"DEBUG Kecamatan: Found kecamatan: '{kecamatan_name}'")
                return kecamatan_name.title()
    
    # Jika tidak ditemukan dengan pattern eksplisit, cari kata yang mungkin kecamatan
    # Biasanya kecamatan berada setelah nama jalan tapi sebelum kota
    parts = address.split(',')
    clean_parts = [part.strip() for part in parts]
    
    # Cari bagian yang terlihat seperti kecamatan (biasanya di tengah alamat)
    for i, part in enumerate(clean_parts):
        part_lower = part.lower()
        # Skip bagian yang jelas bukan kecamatan
        if (len(part) >= 4 and 
            not re.search(r'\d{5}', part) and  # Bukan kode pos
            not re.search(r'^\d+', part) and   # Bukan nomor di awal
            "jalan" not in part_lower and 
            "jl." not in part_lower and
            "no." not in part_lower and
            "gang" not in part_lower and
            "gg." not in part_lower and
            "kota" not in part_lower and
            "kabupaten" not in part_lower and
            "provinsi" not in part_lower and
            len(part.split()) <= 3):  # Maksimal 3 kata
            
            # Jika ini di posisi tengah alamat, kemungkinan kecamatan
            if 1 <= i <= len(clean_parts) - 2:
                print(f"DEBUG Kecamatan: Potential kecamatan from position: '{part}'")
                return part.title()
    
    print("DEBUG Kecamatan: No kecamatan found")
    return None

def extract_city_from_address(address):
    """
    Extract city information from address with improved detection for Indonesian administrative divisions
    ALWAYS returns city name with "Kota" or "Kabupaten" prefix
    """
    # Get allowed_cities from global_data
    allowed_cities = global_data.get('search_cities', None)
    
    if not address or address == "Address not available":
        return "Unknown"
    
    # Clean up address for better parsing
    address = address.replace("Kec.", "Kecamatan").replace("Kab.", "Kabupaten")
    
    # Split address into parts
    parts = address.split(',')
    clean_parts = [part.strip() for part in parts]
    
    print(f"DEBUG City: Processing address: {address}")
    print(f"DEBUG City: Address parts: {clean_parts}")
    
    # STEP 1: PRIORITAS UTAMA - Deteksi "Kota X" dengan validasi ketat
    kota_patterns = [
        r'(?:^|,\s*)Kota\s+([A-Za-z\s]+?)(?:,|$)',  # "Kota Prabumulih" di awal atau setelah koma
        r',\s*Kota\s+([A-Za-z\s]+?)(?:,|$)'         # ", Kota Prabumulih"
    ]
    
    for pattern in kota_patterns:
        kota_matches = re.findall(pattern, address, re.IGNORECASE)
        for match in kota_matches:
            kota_name = match.strip()
            if len(kota_name) >= 3 and "camat" not in kota_name.lower():
                formatted_name = f"Kota {kota_name.title()}"
                print(f"DEBUG City: Found Kota pattern: '{formatted_name}'")
                
                if allowed_cities:
                    # Check against CSV cities
                    for city in allowed_cities:
                        city_str = str(city).strip()
                        if (kota_name.lower() == city_str.lower() or 
                            formatted_name.lower() == city_str.lower()):
                            print(f"DEBUG City: Matched with CSV city: {city_str}")
                            # Return with proper formatting
                            if city_str.lower().startswith('kota '):
                                return city_str.title()
                            else:
                                return f"Kota {city_str.title()}"
                
                # If no CSV validation or no match, return formatted name
                return formatted_name

    # STEP 2: Deteksi "Kabupaten X" dengan validasi ketat
    kabupaten_patterns = [
        r'(?:^|,\s*)Kabupaten\s+([A-Za-z\s]+?)(?:,|$)',  # "Kabupaten Bandung" di awal atau setelah koma  
        r',\s*Kabupaten\s+([A-Za-z\s]+?)(?:,|$)'         # ", Kabupaten Bandung"
    ]
    
    for pattern in kabupaten_patterns:
        kabupaten_matches = re.findall(pattern, address, re.IGNORECASE)
        for match in kabupaten_matches:
            kabupaten_name = match.strip()
            if len(kabupaten_name) >= 3:
                formatted_name = f"Kabupaten {kabupaten_name.title()}"
                print(f"DEBUG City: Found Kabupaten pattern: '{formatted_name}'")
                
                if allowed_cities:
                    # Check against CSV cities
                    for city in allowed_cities:
                        city_str = str(city).strip()
                        if (kabupaten_name.lower() == city_str.lower() or 
                            formatted_name.lower() == city_str.lower()):
                            print(f"DEBUG City: Matched with CSV city: {city_str}")
                            # Return with proper formatting
                            if city_str.lower().startswith('kabupaten '):
                                return city_str.title()
                            else:
                                return f"Kabupaten {city_str.title()}"
                
                # If no CSV validation or no match, return formatted name
                return formatted_name

    # STEP 3: Exact match dengan daftar CSV (untuk kasus tanpa prefix Kota/Kabupaten)
    if allowed_cities and isinstance(allowed_cities, list):
        print(f"DEBUG City: Checking exact matches with {len(allowed_cities)} CSV cities")
        
        # Sort cities by length (longest first) untuk menghindari partial matches
        sorted_cities = sorted(allowed_cities, key=lambda x: len(str(x)), reverse=True)
        
        for city in sorted_cities:
            city_str = str(city).strip()
            if not city_str or len(city_str) < 3:
                continue
                
            # Check for exact city name match with word boundaries dalam setiap part
            for part in clean_parts:
                # Exact match dengan word boundary
                if re.search(r'\b' + re.escape(city_str) + r'\b', part, re.IGNORECASE):
                    # Pastikan ini bukan bagian dari nama jalan atau tempat lain
                    skip_keywords = ['jalan', 'jl.', 'street', 'st.', 'gang', 'gg.', 'no.', 'nomor']
                    part_lower = part.lower()
                    
                    is_street_name = any(keyword in part_lower for keyword in skip_keywords)
                    
                    if not is_street_name:
                        print(f"DEBUG City: Exact match found: '{city_str}' in part '{part}'")
                        # ALWAYS add prefix if not already present
                        if not city_str.lower().startswith(('kota ', 'kabupaten ')):
                            # Default to "Kota" if we can't determine
                            return f"Kota {city_str.title()}"
                        else:
                            return city_str.title()

    # STEP 4: Fallback - cari kata yang terlihat seperti nama kota dan tambahkan prefix
    print("DEBUG City: Using fallback method")
    
    for part in clean_parts:
        if (len(part) >= 3 and 
            "camat" not in part.lower() and 
            not re.search(r'^\d', part) and
            not re.search(r'\d{5}$', part) and
            not any(keyword in part.lower() for keyword in ['jalan', 'jl.', 'gang', 'gg.', 'no.'])):
            
            # Check if it already has prefix
            if part.lower().startswith(('kota ', 'kabupaten ')):
                return part.title()
            else:
                # Add Kota prefix as default
                clean_part = part.strip().title()
                print(f"DEBUG City: Fallback match with Kota prefix: 'Kota {clean_part}'")
                return f"Kota {clean_part}"
    
    print("DEBUG City: No suitable city found, returning Unknown")
    return "Unknown"

def test_city_extraction():
    """
    Test function untuk memverifikasi ekstraksi kota
    """
    # Setup global_data untuk testing
    global global_data
    global_data = {
        'search_cities': [
            'Prabumulih', 'Kota Prabumulih', 'Palembang', 'Kota Palembang',
            'Jakarta', 'Kota Jakarta', 'Bandung', 'Kota Bandung',
            'Kabupaten Bandung', 'Surabaya', 'Kota Surabaya'
        ]
    }
    
    test_addresses = [
        "JL. Jenderal Sudirman. 236 B, Karang Raja, Prabu, Mulih Timur, Prabumulih, Palembang, Kota Prabumulih, Sumatera Selatan 31113",
        "Jl. Asia Afrika No. 8, Braga, Sumur Bandung, Kota Bandung, Jawa Barat 40111",
        "Jl. Malioboro No. 123, Sosromenduran, Gedong Tengen, Kota Yogyakarta, DIY 55271",
        "Jl. Thamrin No. 1, Menteng, Jakarta Pusat, DKI Jakarta 10310",
        "Jl. Raya Bandung, Cikampek, Kabupaten Karawang, Jawa Barat 41373"
    ]
    
    print("=== TESTING CITY EXTRACTION FUNCTION ===")
    for i, address in enumerate(test_addresses, 1):
        print(f"\nTest {i}:")
        print(f"Address: {address}")
        result = extract_city_from_address(address)
        print(f"Extracted City: {result}")
        print("-" * 80)

def sanitize_city_name(city_name):
    """Clean up city names to prevent malformed searches"""
    if not city_name or not isinstance(city_name, str) or len(city_name) < 2:
        return "Unknown"
    
    # Trim excess whitespace
    city_name = city_name.strip()
    
    # Remove known problematic fragments
    problematic_fragments = ["ecamatan", "ecamat", "amatan", "Kec "]
    for fragment in problematic_fragments:
        if fragment.lower() in city_name.lower():
            city_name = re.sub(r'(?i)' + re.escape(fragment), ' ', city_name)
    
    # Clean up whitespace after replacements
    city_name = re.sub(r'\s+', ' ', city_name).strip()
    
    # Fix common malformed prefixes
    if city_name.startswith("Kota ") and len(city_name) < 10:
        remaining = city_name[5:].strip()
        if len(remaining) <= 2:  # If only 1-2 chars remain after "Kota ", it's probably malformed
            return "Unknown"
    
    # Handle "Kota Kabupaten" error (should never happen together)
    if "Kota Kabupaten" in city_name:
        city_name = city_name.replace("Kota Kabupaten", "Kabupaten")
    
    # If we end up with something too short or containing digits, return Unknown
    if len(city_name) < 3 or re.search(r'\d', city_name):
        return "Unknown"
        
    return city_name

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
    
    # If nothing else works, return Unknown
    return "Unknown"

def load_cities_from_csv(csv_filename):
    """Load cities list from CSV file"""
    try:
        df = pd.read_csv(csv_filename)
        
        city_column = None
        possible_columns = ['city', 'name', 'district', 'kota', 'kabupaten', 'daerah']
        
        for col in possible_columns:
            if col in df.columns:
                city_column = col
                break
        
        if not city_column and len(df.columns) > 0:
            city_column = df.columns[0]
            
        if city_column:
            cities = df[city_column].tolist()
            cities = [str(city) for city in cities if city and not pd.isna(city)]
            print(f"Loaded {len(cities)} cities from column '{city_column}' in {csv_filename}")
            return cities
        else:
            raise ValueError("No suitable column found in CSV file")
            
    except Exception as e:
        print(f"Error loading cities from CSV: {e}")
        print("Using default list of Indonesian cities instead")
        return [
            "Jakarta", "Surabaya", "Bandung", "Medan", "Semarang",
            "Makassar", "Palembang", "Tangerang", "Depok", "Bekasi",
            "Bogor", "Malang", "Yogyakarta", "Solo", "Denpasar",
            "Balikpapan", "Samarinda", "Banjarmasin", "Manado", "Padang",
            "Pekanbaru", "Batam", "Pontianak", "Bandar Lampung", "Ambon",
            "Jayapura", "Mataram", "Kupang", "Jambi", "Bengkulu"
        ]

def extract_bank_name(name):
    """Extract bank name from the place name"""
    if not name:
        return None
    
    # List of known Indonesian banks
    banks = [


        "BCA", "BNI", "BRI", "Mandiri", "CIMB Niaga", "CIMB", "Danamon", "BTN", "Shinhan", "IBK", "Sahabat Sampoerna",
        "Permata", "OCBC NISP", "OCBC", "BPD", "Bukopin", "Mega", "Panin", "J Trust", "Neo Commerce",
        "HSBC", "Maybank", "Citibank", "BSI", "BTPN", "UOB", "DKI", "BJB", "Bank Papua", "BPR",
        "Bank Jatim", "Bank Jateng", "Sinarmas", "Standard Chartered","DBS", "Woori", "China Construction",
        "Commonwealth", "Bank Bali", "Muamalat", "Bank Syariah Indonesia", "Mayapada", "CNB", "Nobu", "KEB Hana"
    ]
    
    # Check for bank names in the place name
    for bank in banks:
        # Different matching patterns for bank names
        patterns = [
            r'\b' + re.escape(bank) + r'\b',  # Exact match
            r'\bBank\s+' + re.escape(bank) + r'\b',  # With "Bank" prefix
            r'\b' + re.escape(bank) + r'\s+Bank\b'   # With "Bank" suffix
        ]
        
        for pattern in patterns:
            if re.search(pattern, name, re.IGNORECASE):
                return bank
    
    # Try to match common bank name patterns
    bank_match = re.search(r'\bBank\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)', name)
    if bank_match:
        return bank_match.group(0).strip()
    
    # Check for KCP/KC (Kantor Cabang/Kantor Cabang Pembantu) patterns
    kcp_match = re.search(r'(KCP|KC)\s+([A-Za-z]+)', name)
    if kcp_match:
        return kcp_match.group(2).strip()
    
    return None

def extract_review_count_from_text(text):
    """
    Extract review count from text that may contain both rating and review count
    
    Examples:
    - "4,7\n(2.001)" -> should extract 2001
    - "4,1\n551 ulasan" -> should extract 551
    - "4,6\n479 ulasan" -> should extract 479
    - "2.001 ulasan" -> should extract 2001
    """
    if not text:
        return None
    
    print(f"Extracting review count from: '{repr(text)}'")
    
    # First, look for explicit review patterns with "ulasan" or "review"
    # This is the most reliable method
    review_patterns = [
        r'(\d{1,3}[.,]\d{3}(?:[.,]\d{3})*)\s*(?:ulasan|review)',  # "2.001 ulasan"
        r'(\d+)\s*(?:ulasan|review)',  # "479 ulasan"
        r'\((\d{1,3}[.,]\d{3}(?:[.,]\d{3})*)\)',  # "(2.001)"
        r'\((\d+)\)'  # "(479)"
    ]
    
    for pattern in review_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            try:
                print(f"Found review pattern match: '{match}'")
                clean_num = parse_review_number(match)
                if clean_num is not None:
                    return clean_num
            except ValueError:
                continue
    
    # If no explicit review patterns found, try to extract from multiline text
    # Split by lines and look for numbers that are likely review counts
    lines = text.split('\n')
    print(f"Text split into lines: {lines}")
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Skip lines that look like ratings (e.g., "4,7", "4.5")
        if re.match(r'^\d+[.,]\d+$', line):
            print(f"Skipping rating line: '{line}'")
            continue
        
        # Look for lines with "ulasan" or "review"
        if re.search(r'(?:ulasan|review)', line, re.IGNORECASE):
            # Extract number from this line
            number_match = re.search(r'(\d{1,3}[.,]\d{3}(?:[.,]\d{3})*|\d+)', line)
            if number_match:
                try:
                    clean_num = parse_review_number(number_match.group(1))
                    if clean_num is not None:
                        print(f"Found review count from line '{line}': {clean_num}")
                        return clean_num
                except ValueError:
                    continue
        
        # Look for lines that are just numbers in parentheses (review count indicators)
        paren_match = re.match(r'^\((\d{1,3}[.,]\d{3}(?:[.,]\d{3})*|\d+)\)$', line.strip())
        if paren_match:
            try:
                clean_num = parse_review_number(paren_match.group(1))
                if clean_num is not None:
                    print(f"Found review count from parentheses '{line}': {clean_num}")
                    return clean_num
            except ValueError:
                continue
        
        # Look for standalone numbers that could be review counts
        # But be very careful not to pick up ratings or other numbers
        standalone_match = re.match(r'^(\d{3,})$', line.strip())  # At least 3 digits
        if standalone_match:
            try:
                clean_num = parse_review_number(standalone_match.group(1))
                if clean_num is not None and clean_num >= 10:  # Reasonable minimum for review count
                    print(f"Found potential review count from standalone number '{line}': {clean_num}")
                    return clean_num
            except ValueError:
                continue
    
    print("No review count found in text")
    return None

def get_category(driver):
    """Extract place category from Google Maps"""
    try:
        # Extract category using JavaScript
        js_code = """
        try {
            const categorySelectors = [
                'button[data-item-id="category"], button[jsaction*="category"], button[aria-label*="Category"]',
                'span.DkEaL, .fontTitleSmall',
                'div[jsaction*="category"]'
            ];
            
            for (const selector of categorySelectors) {
                const elements = document.querySelectorAll(selector);
                for (let i = 0; i < elements.length; i++) {
                    const text = elements[i].textContent || elements[i].innerText;
                    if (text && text.trim().length > 1 && 
                        !text.includes('http') && !text.includes('www.') && 
                        !text.includes('@') && !text.includes('Rp') &&
                        !text.includes('Address') && !text.includes('Alamat') &&
                        text.trim().length < 50) {
                        
                        // For elements with aria-label attribute containing "Category"
                        if (elements[i].hasAttribute('aria-label')) {
                            const label = elements[i].getAttribute('aria-label');
                            if (label.toLowerCase().includes('category') || label.toLowerCase().includes('kategori')) {
                                const match = label.match(/(?:category|kategori):?\\s*(.*)/i);
                                if (match && match[1]) {
                                    return match[1].trim();
                                }
                            }
                        }
                        
                        return text.trim();
                    }
                }
            }
            
            // Try to find category in a different way for some place layouts
            const categoryArea = document.querySelector('.m6QErb.tLjsW.eKbjU');
            if (categoryArea) {
                const categoryText = categoryArea.querySelector('button');
                if (categoryText) {
                    return categoryText.textContent || categoryText.innerText;
                }
            }
            
            return "Not specified";
        } catch (e) {
            console.error("Error finding category:", e);
            return "Not specified";
        }
        """
        
        category = driver.execute_script(js_code)
        
        if category and category != "Not specified":
            # Clean up category text
            category = category.replace('"', '').strip()
            # Remove any trailing separators or markers
            category = re.sub(r'[·•⋅,.:;-]\s*$', '', category)
            
            if len(category) < 2 or len(category) > 50:
                return "Not specified"
                
            return category
        
        # Fallback to Selenium selectors if JS fails
        category_selectors = [
            "button[data-item-id='category']",
            "span.DkEaL",
            ".fontTitleSmall",
            "div[jsaction*='category']"
        ]
        
        for selector in category_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if text and len(text) > 1 and len(text) < 50:
                        # Clean the category text
                        text = text.replace('"', '').strip()
                        return text
            except:
                continue
                
    except Exception as e:
        print(f"Error extracting category: {e}")
        
    return "Not specified"

def get_rating_info(driver):
    """Extract rating and review count with improved handling of multiline text"""
    try:
        # Wait a moment for everything to load
        time.sleep(1.5)
        
        rating = None
        reviews_count = None
        
        # -------- EXTRACT RATING --------
        rating_selectors = [
            'div.F7nice span[aria-hidden="true"]',
            'span.ceNzKf',
            'span.fontDisplayLarge',
            'div.jANrlb div.fontDisplayLarge',
            '[aria-label*="stars"]',
            '[aria-label*="bintang"]'
        ]
        
        for selector in rating_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip() or element.get_attribute('aria-label') or ""
                    # Look for rating pattern (e.g., "4.5")
                    rating_match = re.search(r'(\d+[.,]\d+)', text)
                    if rating_match:
                        rating = float(rating_match.group(1).replace(',', '.'))
                        print(f"Found rating: {rating} from element '{text}'")
                        break
                if rating:
                    break
            except Exception as e:
                print(f"Error with rating selector {selector}: {e}")
                continue
        
        # -------- EXTRACT REVIEW COUNT - IMPROVED WITH MULTILINE HANDLING --------
        review_specific_selectors = [
            'button[jsaction*="pane.rating.moreReviews"]',
            'a[href*="reviews"][jsaction*="reviews"]',
            'div.F7nice span.fontBodyMedium[jsaction*="reviews"]',
            'div.jANrlb div.fontBodyMedium span'
        ]
        
        for selector in review_specific_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip() or element.get_attribute('aria-label') or ""
                    
                    print(f"Review text candidate: '{text}'")
                    
                    # Check for strings that contain both a number and the word "review" or "ulasan"
                    if re.search(r'\d+.*(?:review|ulasan)', text, re.IGNORECASE):
                        # CRITICAL FIX: Handle multiline text properly
                        clean_num = extract_review_count_from_text(text)
                        
                        if clean_num is not None and clean_num > 0 and clean_num < 1000000:
                            reviews_count = clean_num
                            print(f"Successfully extracted reviews: {reviews_count} from '{text}'")
                            break
                        else:
                            print(f"Rejected implausible review count: {clean_num}")
                
                if reviews_count is not None:
                    break
            except Exception as e:
                print(f"Error with review selector {selector}: {e}")
                continue
        
        # If we still don't have a review count, try more aggressive pattern matching
        if reviews_count is None:
            try:
                review_areas = driver.find_elements(By.CSS_SELECTOR, 
                    'div.jANrlb, div.F7nice, div[jsaction*="pane.rating"]')
                
                for area in review_areas:
                    area_text = area.text
                    print(f"Checking area text: '{area_text}'")
                    
                    # CRITICAL FIX: Extract review count from multiline text
                    clean_num = extract_review_count_from_text(area_text)
                    
                    if clean_num is not None and clean_num > 0 and clean_num < 1000000:
                        reviews_count = clean_num
                        print(f"Found reviews from area text: {reviews_count}")
                        break
            except Exception as e:
                print(f"Error with pattern matching: {e}")
        
        # Final validation
        if rating == 0 or rating == '0':
            rating = None
        if reviews_count == 0 or reviews_count == '0':
            reviews_count = None
            
        # If there are no reviews, there should be no rating
        if reviews_count is None:
            rating = None
            
        print(f"Final result - Rating: {rating}, Reviews: {reviews_count}")
        return rating, reviews_count
        
    except Exception as e:
        print(f"Error getting rating information: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def parse_review_number(number_text):
    """
    Parse review number with intelligent thousand separator detection
    Fixed version that handles edge cases better
    """
    if not number_text:
        return None
    
    # Remove any whitespace
    number_text = number_text.strip()
    
    print(f"Parsing number: '{number_text}'")
    
    # Case 1: Simple number without separators (e.g., "479")
    if number_text.isdigit():
        result = int(number_text)
        print(f"Simple number: {result}")
        return result
    
    # Case 2: Contains separators - need to determine if it's thousand separator or decimal
    if '.' in number_text:
        parts = number_text.split('.')
        
        # If there are more than 2 parts, it's probably not a valid number
        if len(parts) > 2:
            print(f"Too many dots, treating as invalid: {number_text}")
            return None
        
        if len(parts) == 2:
            left_part = parts[0]
            right_part = parts[1]
            
            # CRITICAL LOGIC: Determine if dot is thousand separator or decimal
            if (len(right_part) == 3 and 
                right_part.isdigit() and 
                left_part.isdigit() and 
                1 <= len(left_part) <= 3):
                
                # This looks like thousand separator (e.g., "2.001")
                combined = left_part + right_part
                result = int(combined)
                print(f"Thousand separator detected: {left_part}.{right_part} -> {result}")
                
                # Additional validation: result should be reasonable for review count
                if result >= 1000:  # Should be at least 1000 if using thousand separator
                    return result
                else:
                    print(f"Result {result} too small for thousand separator, treating as error")
                    return None
            
            else:
                # This might be a decimal or malformed number
                print(f"Not a valid thousand separator pattern: {number_text}")
                return None
    
    # Case 3: Comma as thousand separator (e.g., "2,001")
    if ',' in number_text:
        parts = number_text.split(',')
        
        if len(parts) == 2:
            left_part = parts[0]
            right_part = parts[1]
            
            if (len(right_part) == 3 and 
                right_part.isdigit() and 
                left_part.isdigit() and 
                1 <= len(left_part) <= 3):
                
                combined = left_part + right_part
                result = int(combined)
                print(f"Comma thousand separator: {left_part},{right_part} -> {result}")
                return result
    
    # Case 4: Space as thousand separator (e.g., "2 001")
    if ' ' in number_text:
        parts = number_text.split(' ')
        parts = [p for p in parts if p]  # Remove empty parts
        
        if len(parts) == 2:
            left_part = parts[0]
            right_part = parts[1]
            
            if (len(right_part) == 3 and 
                right_part.isdigit() and 
                left_part.isdigit() and 
                1 <= len(left_part) <= 3):
                
                combined = left_part + right_part
                result = int(combined)
                print(f"Space thousand separator: {left_part} {right_part} -> {result}")
                return result
    
    print(f"Could not parse number: '{number_text}'")
    return None

def get_stars_distribution(driver):
    """Extract distribution of star ratings (1-5)"""
    try:
        # JavaScript to extract star distribution
        js_code = """
        try {
            // First try to open the reviews if they're not visible
            const reviewsButton = document.querySelector('button[aria-label*="review"], a[href*="reviews"]');
            if (reviewsButton) {
                try {
                    reviewsButton.click();
                } catch (e) {
                    console.warn("Could not click reviews button:", e);
                }
            }
            
            // Wait a moment for the reviews to load
            setTimeout(() => {}, 1000);
            
            // Try to find the stars distribution
            const starsData = {
                star1: 0,
                star2: 0,
                star3: 0,
                star4: 0,
                star5: 0
            };
            
            // Look for star distribution elements
            const starRows = document.querySelectorAll('.hqtEJd');
            for (let i = 0; i < starRows.length; i++) {
                const row = starRows[i];
                const starCount = 5 - i; // Rows are usually 5 to 1 stars
                
                // Find the width of the filled portion of the bar
                const bar = row.querySelector('.T5jBvc'); // This is the outer container
                const filledBar = row.querySelector('.Oks9x'); // This is the filled portion
                
                if (bar && filledBar && bar.offsetWidth > 0) {
                    const percentage = filledBar.offsetWidth / bar.offsetWidth;
                    
                    // Try to find the total review count
                    const totalReviewElement = document.querySelector('[aria-label*="review"]');
                    let totalReviews = 0;
                    
                    if (totalReviewElement) {
                        const totalText = totalReviewElement.getAttribute('aria-label') || totalReviewElement.textContent;
                        if (totalText) {
                            const match = totalText.match(/(\\d+[\\s.,]?\\d*[\\s.,]?\\d*)/);
                            if (match) {
                                totalReviews = parseInt(match[0].replace(/[\\s.,]/g, ''));
                            }
                        }
                    }
                    
                    // Calculate approximate count based on percentage
                    if (totalReviews > 0) {
                        switch(starCount) {
                            case 1: starsData.star1 = Math.round(totalReviews * percentage); break;
                            case 2: starsData.star2 = Math.round(totalReviews * percentage); break;
                            case 3: starsData.star3 = Math.round(totalReviews * percentage); break;
                            case 4: starsData.star4 = Math.round(totalReviews * percentage); break;
                            case 5: starsData.star5 = Math.round(totalReviews * percentage); break;
                        }
                    }
                }
            }
            
            return starsData;
        } catch (e) {
            console.error("Error extracting stars distribution:", e);
            return {
                star1: 0,
                star2: 0,
                star3: 0,
                star4: 0,
                star5: 0
            };
        }
        """
        
        stars_data = driver.execute_script(js_code)
        return (
            stars_data.get('star1', 0),
            stars_data.get('star2', 0),
            stars_data.get('star3', 0),
            stars_data.get('star4', 0),
            stars_data.get('star5', 0)
        )
        
    except Exception as e:
        print(f"Error getting stars distribution: {e}")
        return 0, 0, 0, 0, 0

def get_contact_info(driver):
    """Extract phone and website with strict validation to avoid incorrect data"""
    try:
        # Wait for page to load fully
        time.sleep(1.5)
        
        phone = None
        website = None
        
        # -------- PHONE NUMBER EXTRACTION --------
        # Only look for dedicated phone buttons with a very specific approach
        try:
            # First try to find buttons that are explicitly labeled as phone buttons
            strict_phone_selectors = [
                'button[data-item-id="phone"]', 
                'button[aria-label^="Phone:"]',
                'button[aria-label^="Telepon:"]'
            ]
            
            # Check each selector
            for selector in strict_phone_selectors:
                phone_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                
                if phone_elements and len(phone_elements) > 0:
                    # For buttons with aria-label="Phone: 123-456-7890"
                    aria_label = phone_elements[0].get_attribute('aria-label')
                    if aria_label and ("Phone:" in aria_label or "Telepon:" in aria_label):
                        label_parts = aria_label.split(':', 1)
                        if len(label_parts) > 1:
                            phone_candidate = label_parts[1].strip()
                            
                            # Validate it contains digits and looks like a phone number
                            # (Should be at least 5 digits, no more than 15 digits)
                            digits_only = re.sub(r'\D', '', phone_candidate)
                            if 5 <= len(digits_only) <= 15:
                                phone = phone_candidate
                                break
                    
                    # If aria-label didn't work, check direct button text
                    if not phone:
                        button_text = phone_elements[0].text.strip()
                        if button_text:
                            # Validate button text has digits and reasonable length
                            digits_only = re.sub(r'\D', '', button_text)
                            if 5 <= len(digits_only) <= 15:
                                phone = button_text
                                break
            
            # If strict selectors failed, try a different approach
            if not phone:
                # Try to specifically click the phone button and get text from a popup
                broader_selectors = [
                    'button[jsaction*="phone"]', 
                    'div[role="button"][data-item-id="phone"]',
                    'button[jsaction*=".phone"]'
                ]
                
                for selector in broader_selectors:
                    phone_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if phone_elements and len(phone_elements) > 0:
                        # Try to click the button to reveal the phone number
                        driver.execute_script("arguments[0].click();", phone_elements[0])
                        time.sleep(0.5)
                        
                        # Look for a popup with the phone number
                        popups = driver.find_elements(By.CSS_SELECTOR, '.g88MCb, [role="dialog"] .fontBodyMedium')
                        for popup in popups:
                            popup_text = popup.text
                            if popup_text:
                                # Look for text that matches common Indonesian phone formats
                                phone_match = re.search(r'(?:\+62|0)[0-9\s\-]{7,15}', popup_text)
                                if phone_match:
                                    phone = phone_match.group(0)
                                    break
                                
                                # Fallback to any sequence of digits of appropriate length
                                digit_match = re.search(r'[0-9\s\-\(\)\+]{7,20}', popup_text)
                                if digit_match:
                                    candidate = digit_match.group(0)
                                    if len(re.sub(r'\D', '', candidate)) >= 5:
                                        phone = candidate
                                        break
                        
                        # Close popup by pressing Escape
                        try:
                            from selenium.webdriver.common.keys import Keys
                            from selenium.webdriver.common.action_chains import ActionChains
                            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                            time.sleep(0.3)
                        except:
                            pass
                        
                        if phone:
                            break
        except Exception as e:
            print(f"Error extracting phone: {e}")
        
        # -------- WEBSITE EXTRACTION --------
        try:
            website_selectors = [
                'button[data-item-id="authority"]',
                'a[data-item-id="authority"]',
                'button[aria-label^="Website:"]',
                'button[jsaction*="authority"]'
            ]
            
            for selector in website_selectors:
                website_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                
                if website_elements and len(website_elements) > 0:
                    # Try aria-label first
                    aria_label = website_elements[0].get_attribute('aria-label')
                    if aria_label and "Website:" in aria_label:
                        label_parts = aria_label.split(':', 1)
                        if len(label_parts) > 1:
                            website = label_parts[1].strip()
                    
                    # Try href attribute
                    if not website:
                        href = website_elements[0].get_attribute('href')
                        if href and ('http' in href or 'www.' in href):
                            website = href
                    
                    # Try text content
                    if not website:
                        text = website_elements[0].text
                        if text and ('http' in text or 'www.' in text):
                            website = text
                    
                    if website:
                        break
        except Exception as e:
            print(f"Error extracting website: {e}")
        
        # Final validation to prevent false positives
        if phone:
            # Ensure it's not a rating pattern like "1(40)"
            if re.match(r'\d+\(\d+\)', phone):  # Like "1(40)"
                print(f"Rejecting phone that looks like a rating: {phone}")
                phone = None
                
            # Ensure it has enough digits to be a real phone number
            digits_only = re.sub(r'\D', '', phone) if phone else ""
            if len(digits_only) < 5:
                print(f"Rejecting phone with too few digits: {phone}")
                phone = None
                
            # Ensure it doesn't end with typical rating patterns
            if phone and re.search(r'/5$|stars$|rating$', phone, re.IGNORECASE):
                print(f"Rejecting phone with rating indicators: {phone}")
                phone = None
        
        return phone, website
        
    except Exception as e:
        print(f"Error in contact info extraction: {e}")
        return None, None

def get_opening_hours(driver):
    """Extract opening hours information"""
    try:
        # JavaScript to extract opening hours
        js_code = """
        try {
            // Try different methods to get opening hours
            
            // Method 1: Look for hours button
            const hoursButton = document.querySelector('button[data-item-id="oh"], button[aria-label*="hours"], button[aria-label*="jam"]');
            if (hoursButton) {
                const hoursText = hoursButton.getAttribute('aria-label') || hoursButton.textContent;
                if (hoursText) {
                    const hoursMatch = hoursText.match(/(?:hours|jam):\\s*([^\\n]+)/i);
                    if (hoursMatch && hoursMatch[1]) {
                        return hoursMatch[1].trim();
                    }
                    
                    // If we didn't find the pattern, but the text mentions hours/operational
                    if (hoursText.toLowerCase().includes('open') || 
                        hoursText.toLowerCase().includes('buka') ||
                        hoursText.toLowerCase().includes('tutup') ||
                        hoursText.toLowerCase().includes('closed')) {
                        return hoursText.trim();
                    }
                }
            }
            
            // Method 2: Look for specific sections or elements
            const hoursSections = document.querySelectorAll('.OqCZI, .fontBodyMedium');
            for (let i = 0; i < hoursSections.length; i++) {
                const text = hoursSections[i].textContent;
                if (text && (
                    text.toLowerCase().includes('open') || 
                    text.toLowerCase().includes('buka') ||
                    text.toLowerCase().includes('24 jam') ||
                    text.toLowerCase().includes('tutup') ||
                    text.toLowerCase().includes('closed') ||
                    text.toLowerCase().includes('senin') ||
                    text.toLowerCase().includes('monday')
                )) {
                    return text.trim();
                }
            }
            
            return null;
        } catch (e) {
            console.error("Error extracting opening hours:", e);
            return null;
        }
        """
        
        hours = driver.execute_script(js_code)
        if hours:
            # Clean up the hours text
            hours = hours.replace('"', '').strip()
            hours = re.sub(r'\\n', ' ', hours)
            hours = re.sub(r'\s+', ' ', hours)
            return hours
            
        return None
        
    except Exception as e:
        print(f"Error getting opening hours: {e}")
        return None

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
    """Close any open place details panel with improved error handling"""
    try:
        # First check if driver session is still active
        try:
            # A lightweight command to check if session is active
            current_url = driver.current_url
        except:
            print("Driver session no longer active, cannot close place details")
            return False
            
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
        try:
            from selenium.webdriver.common.action_chains import ActionChains
            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(0.5)
            return True
        except:
            pass
            
    except Exception as e:
        print(f"Error closing place details: {e}")
    
    return False



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
    
    # Default coordinates for various Indonesian cities
    default_coords = {
        "yogyakarta": (-7.7956, 110.3695),
        "jakarta": (-6.2088, 106.8456),
        "bandung": (-6.9175, 107.6191),
        "surabaya": (-7.2575, 112.7521),
        "medan": (3.5952, 98.6722),
        "makassar": (-5.1477, 119.4327),
        "bali": (-8.4095, 115.1889),
        "denpasar": (-8.6705, 115.2126),
    }
    
    target_area = global_data.get('target_area', '').lower()
    
    for area, coords in default_coords.items():
        if area in target_area or target_area in area:
            print(f"Using default coordinates for {area}")
            return coords
    
    print(f"No default coordinates for {target_area}, using Yogyakarta as fallback")
    return (-7.7956, 110.3695)

def is_in_target_area(coordinates, target_area):
    """Check if coordinates are within target area"""
    if not target_area:
        return True
    # Simplified area check - implement your own logic
    return True

def primary_area_search(driver, target_area, keyword):
    """Search for branches in primary target area with improved error handling"""
    try:
        print(f"\n--- Phase 1: Searching for {keyword} Bank in {target_area} ---")
        
        search_query = f"{keyword} Bank in {target_area}"
        
        # Use the new search function with retry
        if not perform_search_with_retry(driver, search_query):
            print(f"Failed to search for {keyword} in {target_area}")
            return False
        
        coords = get_current_coordinates(driver)
        global_data['coordinates_searched'].add(coords)
        global_data['last_coords'] = coords
        global_data['search_query'] = search_query
        
        print(f"Successfully loaded {target_area} search at coordinates: {coords}")
        return True
    
    except Exception as e:
        print(f"Error in {target_area} search: {e}")
        return False

def city_search(driver, city, keyword):
    """Search for branches in specific city with improved error handling"""
    try:
        print(f"\n--- Phase 2: Searching for {keyword} in {city} ---")
        
        search_query = f"{keyword} Bank in {city}"
        
        # Use the new search function with retry
        if not perform_search_with_retry(driver, search_query):
            print(f"Failed to search for {keyword} Bank in {city}")
            return False
        
        coords = get_current_coordinates(driver)
        global_data['coordinates_searched'].add(coords)
        global_data['last_coords'] = coords
        global_data['search_query'] = search_query
        
        print(f"Successfully loaded city search for {city} at coordinates: {coords}")
        return True
    
    except Exception as e:
        print(f"Error in city search for {city}: {e}")
        return False

def bank_city_search(driver, bank, city, keyword):
    """Search for specific bank branches in city with improved error handling"""
    try:
        # Sanitize city name to prevent malformed searches
        sanitized_city = sanitize_city_name(city)
        if sanitized_city == "Unknown":
            print(f"WARNING: City name '{city}' couldn't be sanitized properly, using generic term")
            sanitized_city = "Indonesia"  # Fallback to a generic location
        
        print(f"\n--- Phase 3: Searching for {keyword} Bank {bank} in {sanitized_city} ---")
        
        search_query = f"{keyword} Bank {bank} in {sanitized_city}"
        
        # Use the search function with retry
        if not perform_search_with_retry(driver, search_query):
            print(f"Failed to search for {keyword} Bank {bank} in {sanitized_city}")
            return False
        
        coords = get_current_coordinates(driver)
        global_data['coordinates_searched'].add(coords)
        global_data['last_coords'] = coords
        global_data['search_query'] = search_query
        
        print(f"Successfully loaded bank+city search for Bank {bank} in {sanitized_city} at coordinates: {coords}")
        return True
    
    except Exception as e:
        print(f"Error in bank+city search for {bank} in {city}: {e}")
        return False

def process_search_results(driver, max_results, max_scroll_attempts, scroll_pause_time):
    """Process search results from current view with improved city and kecamatan extraction"""
    scroll_attempts = 0
    keep_scrolling = True
    consecutive_no_new_results = 0
    max_consecutive_no_results = 3
    
    print("Starting to process search results...")
    
    while keep_scrolling and len(global_data['names']) < max_results and scroll_attempts < max_scroll_attempts:
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
                        
                        # After clicking on the element and confirming we're on a details page
                        try:
                            # Wait explicitly for the place details container to be fully loaded
                            details_loaded = WebDriverWait(driver, 5).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.m6QErb.DxyBCb.kA9KIf.dS8AEf, div.m6QErb.tLjsW.eKbjU'))
                            )
                            
                            # Wait a bit longer to ensure all content is loaded
                            time.sleep(2)
                            
                            # Ensure all dynamic elements are loaded by scrolling slightly
                            driver.execute_script("window.scrollBy(0, 100);")
                            time.sleep(0.5)
                        except Exception as wait_error:
                            print(f"Error waiting for place details to load: {wait_error}")
                        
                        # Get current URL as link
                        link = driver.current_url
                        
                        # Extract latitude and longitude from link
                        latitude, longitude = extract_coordinates_from_link(link)
                        
                        # Get category
                        category = get_category(driver)
                        
                        # Extract bank name from place name
                        bank_name = extract_bank_name(name)
                        
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
                        
                        # Extract city from address with improved function (ALWAYS with Kota/Kabupaten prefix)
                        city = extract_city_from_address(address)
                        
                        # Extract kecamatan from address (NEW)
                        kecamatan = extract_kecamatan_from_address(address)
                        
                        # Extract ZIP code from address
                        zip_code = extract_zip_code(address)
                        
                        # Extract province from address
                        province = extract_province_from_address(address)
                        
                        # Wait specifically for rating information to load
                        try:
                            WebDriverWait(driver, 3).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'span.fontDisplayLarge, div.F7nice, [aria-label*="stars"]'))
                            )
                        except:
                            print(f"Rating information may not be available for {name}")
                        
                        # Get rating and review count with retry
                        rating, reviews_count = get_rating_info(driver)
                        if rating is None and reviews_count is None:
                            print(f"Failed to extract rating for {name}, retrying...")
                            time.sleep(1)  # Wait a bit longer
                            rating, reviews_count = get_rating_info(driver)  # Try again
                        
                        # Ensure empty values stay None instead of being converted to 0 or other values
                        if rating == 0 or rating == '0' or rating == '':
                            rating = None
                        if reviews_count == 0 or reviews_count == '0' or reviews_count == '':
                            reviews_count = None
                        
                        # NEW FIX: If there are no reviews, there should be no rating
                        if reviews_count is None:
                            rating = None
                        
                        # Get stars distribution (1-5)
                        stars_1, stars_2, stars_3, stars_4, stars_5 = get_stars_distribution(driver)
                        
                        # Get contact information (phone and website)
                        phone, website = get_contact_info(driver)
                        
                        # Get opening hours
                        opening_hours = get_opening_hours(driver)
                        
                        # ENHANCED DUPLICATE DETECTION
                        duplicate_found = False
                        duplicate_reason = None
                        
                        # 1. Check for duplicate based on name and address pair
                        name_address_pair = (name, address)
                        if name_address_pair in global_data['seen_name_pairs']:
                            duplicate_found = True
                            duplicate_reason = "name+address"
                        
                        # 2. Check for duplicate based on URL
                        if link in global_data['seen_links']:
                            duplicate_found = True
                            duplicate_reason = "URL"
                        
                        # 3. Check for duplicate based on coordinates
                        if latitude is not None and longitude is not None:
                            rounded_coords = (round(latitude, 5), round(longitude, 5))
                            if rounded_coords in global_data['seen_coordinates']:
                                duplicate_found = True
                                duplicate_reason = "coordinates"
                        
                        if duplicate_found:
                            print(f"Duplicate found ({duplicate_reason}), skipping: {name}")
                            close_place_details(driver)
                            continue
                        
                        # If we reach here, the place is unique - add to seen collections
                        global_data['seen_name_pairs'].add(name_address_pair)
                        global_data['seen_links'].add(link)
                        if latitude is not None and longitude is not None:
                            global_data['seen_coordinates'].add((round(latitude, 5), round(longitude, 5)))
                        
                        # Ensure all lists have the same length before appending
                        current_length = len(global_data['names'])
                        
                        # Add data to global storage - using try/except to catch any inconsistencies
                        try:
                            global_data['names'].append(name)
                            global_data['categories'].append(category)
                            global_data['bank_names'].append(bank_name)
                            global_data['addresses'].append(address)
                            global_data['cities'].append(city)  # Now always contains Kota/Kabupaten prefix
                            global_data['kecamatans'].append(kecamatan)  # NEW: Add kecamatan data
                            global_data['zip_codes'].append(zip_code)
                            global_data['provinces'].append(province)
                            global_data['links'].append(link)
                            global_data['latitudes'].append(latitude)
                            global_data['longitudes'].append(longitude)
                            global_data['ratings'].append(rating)
                            global_data['reviews_counts'].append(reviews_count)
                            global_data['phones'].append(phone)
                            global_data['websites'].append(website)
                            global_data['opening_hours'].append(opening_hours)
                            global_data['stars_1'].append(stars_1)
                            global_data['stars_2'].append(stars_2)
                            global_data['stars_3'].append(stars_3)
                            global_data['stars_4'].append(stars_4)
                            global_data['stars_5'].append(stars_5)
                        except Exception as array_error:
                            print(f"Error adding data to arrays: {array_error}")
                            # Rollback - remove the last item from any arrays that were already appended to
                            for key in global_data:
                                if isinstance(global_data[key], list) and len(global_data[key]) > current_length:
                                    global_data[key].pop()
                            continue
                        
                        new_results_added += 1
                        total_results = len(global_data['names'])
                        print(f"[{total_results}/{max_results}] {name} | {category} | {address} | City: {city} | Kecamatan: {kecamatan} | Rating: {rating} | Reviews: {reviews_count}")
                        
                        # Close place details to go back to search results
                        close_place_details(driver)
                        time.sleep(0.5)
                        
                        # Save progress every 25 items
                        if total_results % 25 == 0:
                            save_current_progress()
                        
                        # Check if we've reached the target
                        if total_results >= max_results:
                            print(f"Reached target of {max_results} results!")
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
                        try:
                            close_place_details(driver)
                        except:
                            pass
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
            if keep_scrolling and len(global_data['names']) < max_results:
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
    
    print(f"Finished processing search results. Found {len(global_data['names'])} total results.")

def scrape_google_maps(target_area="Yogyakarta", max_results=5000,
                     scroll_pause_time=1.5, max_scroll_attempts=20,
                     resume_from=None, headless=False, driver_path=None,
                     proxy=None, timeout=30, cities_csv=None,
                     selected_phases=None, max_cities=100):
    """
    Enhanced Google Maps scraper with three-phase search strategy and improved city extraction
    """
    # Set default value for selected_phases if not provided
    if selected_phases is None:
        selected_phases = ['primary_area', 'cities', 'bank_city']
        
    print(f"Running scraper with selected phases: {', '.join(selected_phases)}")
    
    # Define banks for targeted searches
    banks = [
        "bca", "bni", "bri", "mandiri", "cimb", "danamon", "btn", "Rakyat Indonesia",
        "permata", "ocbc", "bpd", "bukopin", "mega", "panin",
        "hsbc", "maybank", "citibank", "bsi", "syariah indonesia", "btpn", "uob"

    ]
    
    # Define keywords for branch offices (instead of ATM)
    keywords = ["KC", "KCP", "ATM"]  # KC = Kantor Cabang, KCP = Kantor Cabang Pembantu
    
    # Load cities from CSV - PENTING! Simpan dalam variable terpisah untuk menghindari konflik
    search_cities = []
    if cities_csv and os.path.exists(cities_csv):
        search_cities = load_cities_from_csv(cities_csv)
        print(f"Loaded {len(search_cities)} cities from {cities_csv}")
    else:
        print("No cities CSV provided or file not found. Using default Indonesian cities.")
        search_cities = [
            "Jakarta", "Surabaya", "Bandung", "Medan", "Semarang",
            "Makassar", "Palembang", "Tangerang", "Depok", "Bekasi",
            "Bogor", "Malang", "Solo", "Denpasar", "Balikpapan",
            "Samarinda", "Banjarmasin", "Manado", "Padang", "Pekanbaru",
            "Batam", "Pontianak", "Bandar Lampung", "Ambon", "Jayapura"
        ]
    
    # MODIFIED: Limit the cities list to the maximum number specified
    if max_cities and len(search_cities) > max_cities:
        print(f"Limiting cities to {max_cities} from {len(search_cities)} total")
        search_cities = search_cities[:max_cities]
    
    # Set up global data
    global global_data
    global_data = initialize_global_data()
    global_data['banks'] = banks
    global_data['search_cities'] = search_cities.copy()  # Store search cities separately
    global_data['cities'] = search_cities.copy()  # Keep cities for backward compatibility
    global_data['keywords'] = keywords
    global_data['target_area'] = target_area
    global_data['search_query'] = f"{keywords[0]} in {target_area}"
    
    # Initialize result_cities array to store extracted cities from addresses
    global_data['result_cities'] = []
    
    # Inisialisasi variabel pelacakan pencarian berulang
    global_data['last_progress_check'] = 0  # Jumlah hasil terakhir untuk mengukur kemajuan
    global_data['no_progress_iterations'] = 0  # Berapa kali tidak ada kemajuan
    global_data['total_search_cities'] = len(search_cities)  # Jumlah total kota yang akan dicari
    global_data['processed_cities_count'] = 0  # Track how many cities we've processed
    global_data['cities_per_bank'] = {}  # Track cities processed for each bank
    
    # Register signal handlers
    signal.signal(signal.SIGINT, save_current_progress)
    signal.signal(signal.SIGTERM, save_current_progress)
    
    # Load checkpoint if resuming
    if resume_from:
        try:
            with open(resume_from, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
            
            global_data['processed_elements'] = set(checkpoint['processed_elements'])
            global_data['seen_name_pairs'] = set(tuple(item) for item in checkpoint['seen_name_pairs'])
            global_data['search_query'] = checkpoint['search_query']
            
            # Restore seen_links and seen_coordinates
            if 'seen_links' in checkpoint:
                global_data['seen_links'] = set(checkpoint['seen_links'])
            if 'seen_coordinates' in checkpoint:
                global_data['seen_coordinates'] = set(tuple(item) for item in checkpoint['seen_coordinates'])
            
            if 'coordinates_searched' in checkpoint:
                global_data['coordinates_searched'] = set(tuple(item) for item in checkpoint['coordinates_searched'])
            if 'target_area' in checkpoint:
                global_data['target_area'] = checkpoint['target_area']
            if 'last_coords' in checkpoint and checkpoint['last_coords']:
                global_data['last_coords'] = tuple(checkpoint['last_coords'])
            
            if 'search_phase' in checkpoint:
                global_data['search_phase'] = checkpoint['search_phase']
                if global_data['search_phase'] == 'yogyakarta':
                    global_data['search_phase'] = 'primary_area'
            if 'bank_index' in checkpoint:
                global_data['bank_index'] = checkpoint['bank_index']
            if 'city_index' in checkpoint:
                global_data['city_index'] = checkpoint['city_index']
            if 'keyword_index' in checkpoint:
                global_data['keyword_index'] = checkpoint['keyword_index']
            
            # Restore processed cities count if available
            if 'processed_cities_count' in checkpoint:
                global_data['processed_cities_count'] = checkpoint['processed_cities_count']
            if 'cities_per_bank' in checkpoint:
                global_data['cities_per_bank'] = checkpoint['cities_per_bank']
            
            print(f"Resuming from checkpoint with {len(global_data['processed_elements'])} processed elements")
            print(f"Search phase: {global_data['search_phase']}")
            print(f"Cities processed so far: {global_data['processed_cities_count']}/{len(global_data['search_cities'])}")
            
            # Check if the current phase from checkpoint is in selected phases
            if global_data['search_phase'] not in selected_phases:
                print(f"Warning: Checkpoint phase '{global_data['search_phase']}' is not in selected phases.")
                
                # Find the next appropriate phase
                phase_order = ['primary_area', 'cities', 'bank_city']
                current_index = phase_order.index(global_data['search_phase']) if global_data['search_phase'] in phase_order else -1
                
                # Find phases that come after the current one
                next_phases = [p for p in selected_phases if p in phase_order and phase_order.index(p) > current_index]
                
                if next_phases:
                    # Move to the next selected phase
                    next_phase = next_phases[0]
                    print(f"Moving to next selected phase: {next_phase}")
                    global_data['search_phase'] = next_phase
                    
                    # Reset indices based on the new phase
                    if next_phase == 'cities':
                        global_data['city_index'] = 0
                        global_data['keyword_index'] = 0
                    elif next_phase == 'bank_city':
                        global_data['bank_index'] = 0
                        global_data['city_index'] = 0
                        global_data['keyword_index'] = 0
                else:
                    # If no next phase available, start with the first selected phase
                    if selected_phases:
                        first_phase = selected_phases[0]
                        print(f"No next phase available. Starting from first selected phase: {first_phase}")
                        global_data['search_phase'] = first_phase
                        
                        # Reset all indices
                        global_data['city_index'] = 0
                        global_data['bank_index'] = 0
                        global_data['keyword_index'] = 0
                    else:
                        print("Error: No phases selected to run!")
                        return pd.DataFrame()  # Return empty DataFrame
            
            # Load previous results
            checkpoint_base = resume_from.replace('.json', '')
            csv_file = f"{checkpoint_base.replace('checkpoint_', 'google_maps_results_')}.csv"
            if os.path.exists(csv_file):
                previous_df = pd.read_csv(csv_file, encoding='utf-8')
                
                # Load all available columns from the previous results
                if 'Name' in previous_df.columns:
                    global_data['names'] = previous_df['Name'].tolist()
                if 'Category' in previous_df.columns:
                    global_data['categories'] = previous_df['Category'].tolist()
                else:
                    global_data['categories'] = ["Not specified"] * len(global_data['names'])
                if 'Bank_Name' in previous_df.columns:
                    global_data['bank_names'] = previous_df['Bank_Name'].tolist()
                else:
                    global_data['bank_names'] = [None] * len(global_data['names'])
                if 'Address' in previous_df.columns:
                    global_data['addresses'] = previous_df['Address'].tolist()
                
                # IMPROVED: Load or recreate result_cities
                if 'result_cities' in previous_df.columns:
                    global_data['result_cities'] = previous_df['result_cities'].tolist()
                elif 'City' in previous_df.columns:
                    # Initially load City column as result_cities
                    global_data['result_cities'] = previous_df['City'].tolist()
                    
                    # Optionally re-extract with improved function
                    if 'Address' in previous_df.columns:
                        print("Re-extracting cities from addresses with improved function...")
                        global_data['result_cities'] = [
                            extract_city_from_address(addr) for addr in previous_df['Address']
                        ]
                else:
                    # Create result_cities from addresses
                    global_data['result_cities'] = [
                        extract_city_from_address(addr) for addr in global_data['addresses']
                    ]
                
                # Load legacy cities from City column for backward compatibility
                if 'City' in previous_df.columns:
                    global_data['cities'] = previous_df['City'].tolist()
                else:
                    # If no City column, use result_cities as cities
                    global_data['cities'] = global_data['result_cities'].copy()
                
                if 'ZIP_Code' in previous_df.columns:
                    global_data['zip_codes'] = previous_df['ZIP_Code'].tolist()
                else:
                    global_data['zip_codes'] = [extract_zip_code(addr) for addr in global_data['addresses']]
                if 'Province' in previous_df.columns:
                    global_data['provinces'] = previous_df['Province'].tolist()
                else:
                    global_data['provinces'] = [extract_province_from_address(addr) for addr in global_data['addresses']]
                if 'Link' in previous_df.columns:
                    global_data['links'] = previous_df['Link'].tolist()
                    # Add links to seen_links set for duplicate prevention
                    global_data['seen_links'].update(set(previous_df['Link'].tolist()))
                
                # Load latitude and longitude
                if 'Latitude' in previous_df.columns:
                    global_data['latitudes'] = previous_df['Latitude'].tolist()
                else:
                    global_data['latitudes'] = []
                    for link in global_data['links']:
                        lat, _ = extract_coordinates_from_link(link)
                        global_data['latitudes'].append(lat)
                
                if 'Longitude' in previous_df.columns:
                    global_data['longitudes'] = previous_df['Longitude'].tolist()
                else:
                    global_data['longitudes'] = []
                    for link in global_data['links']:
                        _, lng = extract_coordinates_from_link(link)
                        global_data['longitudes'].append(lng)
                
                # Add coordinates to seen_coordinates set for duplicate prevention
                for lat, lng in zip(global_data['latitudes'], global_data['longitudes']):
                    if lat is not None and lng is not None:
                        global_data['seen_coordinates'].add((round(lat, 5), round(lng, 5)))
                
                # Load additional fields
                if 'Rating' in previous_df.columns:
                    global_data['ratings'] = previous_df['Rating'].tolist()
                else:
                    global_data['ratings'] = [None] * len(global_data['names'])
                if 'Reviews_Count' in previous_df.columns:
                    global_data['reviews_counts'] = previous_df['Reviews_Count'].tolist()
                else:
                    global_data['reviews_counts'] = [None] * len(global_data['names'])
                if 'Phone' in previous_df.columns:
                    global_data['phones'] = previous_df['Phone'].tolist()
                else:
                    global_data['phones'] = [None] * len(global_data['names'])
                if 'Website' in previous_df.columns:
                    global_data['websites'] = previous_df['Website'].tolist()
                else:
                    global_data['websites'] = [None] * len(global_data['names'])
                if 'Opening_Hours' in previous_df.columns:
                    global_data['opening_hours'] = previous_df['Opening_Hours'].tolist()
                else:
                    global_data['opening_hours'] = [None] * len(global_data['names'])
                
                # Load star ratings
                if 'Star_1' in previous_df.columns:
                    global_data['stars_1'] = previous_df['Star_1'].tolist()
                else:
                    global_data['stars_1'] = [0] * len(global_data['names'])
                if 'Star_2' in previous_df.columns:
                    global_data['stars_2'] = previous_df['Star_2'].tolist()
                else:
                    global_data['stars_2'] = [0] * len(global_data['names'])
                if 'Star_3' in previous_df.columns:
                    global_data['stars_3'] = previous_df['Star_3'].tolist()
                else:
                    global_data['stars_3'] = [0] * len(global_data['names'])
                if 'Star_4' in previous_df.columns:
                    global_data['stars_4'] = previous_df['Star_4'].tolist()
                else:
                    global_data['stars_4'] = [0] * len(global_data['names'])
                if 'Star_5' in previous_df.columns:
                    global_data['stars_5'] = previous_df['Star_5'].tolist()
                else:
                    global_data['stars_5'] = [0] * len(global_data['names'])
                
                print(f"Loaded {len(global_data['names'])} previous results")
                global_data['last_progress_check'] = len(global_data['names'])  # Init progress check
            
            # PERBAIKAN: Setelah memuat checkpoint, verifikasi city_index
            if global_data['city_index'] >= len(global_data['search_cities']):
                print(f"WARNING: City index ({global_data['city_index']}) exceeds available cities ({len(global_data['search_cities'])})")
                if 'bank_city' in selected_phases:
                    print(f"Resetting city_index to 0 and moving to bank_city phase")
                    global_data['city_index'] = 0
                    global_data['search_phase'] = 'bank_city'
                    global_data['bank_index'] = 0
                else:
                    print(f"Resetting city_index to 0 and staying in current phase")
                    global_data['city_index'] = 0
                
            if checkpoint.get('target_area', '') != target_area:
                print(f"WARNING: Checkpoint has different target area:")
                print(f"- Target area: '{checkpoint.get('target_area', '')}' vs current '{target_area}'")
                    
                response = input("Continue anyway? (y/n): ")
                if response.lower() != 'y':
                    return pd.DataFrame()
                
        except Exception as e:
            print(f"Error loading checkpoint: {e}")
            print("Starting fresh scrape")
            
            # If starting fresh, initialize with the first selected phase
            if selected_phases:
                global_data['search_phase'] = selected_phases[0]
                print(f"Starting fresh with first selected phase: {global_data['search_phase']}")

    # If not resuming from checkpoint, set the initial phase to the first selected phase
    elif selected_phases:
        global_data['search_phase'] = selected_phases[0]
        print(f"Starting with the first selected phase: {global_data['search_phase']}")

    # Set up WebDriver
    driver = None
    try:
        driver = initialize_driver(headless=headless, driver_path=driver_path, proxy=proxy)
        if timeout:
            driver.set_page_load_timeout(timeout)
        
        def recover_browser(headless=False, driver_path=None, proxy=None, timeout=30):
            """Recreate browser when session becomes invalid"""
            global driver
            
            print("Attempting browser recovery...")
            
            # First, try to safely close existing browser
            try:
                if driver is not None:
                    driver.quit()
            except Exception as e:
                print(f"Error closing existing browser: {e}")
            
            # Create a fresh browser instance
            try:
                print("Creating new browser instance...")
                driver = initialize_driver(headless=headless, driver_path=driver_path, proxy=proxy)
                if timeout:
                    driver.set_page_load_timeout(timeout)
                
                # Verify the browser works by navigating to Google Maps
                driver.get('https://maps.google.com')
                time.sleep(3)
                
                # Check if page loaded successfully
                if 'maps.google.com' in driver.current_url:
                    print("Browser recovery successful")
                    return True
                else:
                    print("Browser recovery failed: Could not navigate to maps.google.com")
                    return False
                    
            except Exception as e:
                print(f"Browser recovery failed: {e}")
                return False
        
        # Main search loop with enhanced error handling
        consecutive_failures = 0
        max_consecutive_failures = 3
        session_recreate_count = 0
        max_session_recreations = 3
        
        while len(global_data['names']) < max_results:
            # Get the current keyword based on keyword_index
            keyword_index = global_data['keyword_index']
            current_keyword = global_data['keywords'][keyword_index]
            
            # Phase 1: Primary area search
            if global_data['search_phase'] == 'primary_area' and 'primary_area' in selected_phases:
                print(f"\n--- PHASE 1: Searching for {current_keyword} in {target_area} ---")
                print(f"Current progress: {len(global_data['names'])}/{max_results} places found")
                
                # Check global progress
                current_results_count = len(global_data['names'])
                if current_results_count <= global_data['last_progress_check'] + 2:
                    global_data['no_progress_iterations'] += 1
                    print(f"Low progress detected: {global_data['no_progress_iterations']} iterations with minimal results")
                    
                    # If 3 consecutive iterations with minimal progress
                    if global_data['no_progress_iterations'] >= 3:
                        print(f"No significant progress for {global_data['no_progress_iterations']} iterations.")
                        
                        # Move to the next selected phase
                        next_phase = None
                        if 'cities' in selected_phases:
                            next_phase = 'cities'
                        elif 'bank_city' in selected_phases:
                            next_phase = 'bank_city'
                            
                        if next_phase:
                            print(f"MOVING to {next_phase} phase to find more results...")
                            global_data['search_phase'] = next_phase
                            global_data['city_index'] = 0
                            global_data['keyword_index'] = 0
                            global_data['no_progress_iterations'] = 0
                            save_current_progress()
                            continue
                        else:
                            print("No more selected phases to run after primary_area.")
                            break
                else:
                    # Reset counter if significant progress
                    global_data['no_progress_iterations'] = 0
                
                # Update last progress check value
                global_data['last_progress_check'] = current_results_count
                
                search_success = primary_area_search(driver, target_area, current_keyword)
                
                if not search_success:
                    consecutive_failures += 1
                    print(f"Search failed (attempt {consecutive_failures}/{max_consecutive_failures})")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        print(f"Max consecutive failures reached. Trying next keyword or moving to next phase...")
                        keyword_index += 1
                        if keyword_index >= len(global_data['keywords']):
                            # If we've tried all keywords, find the next selected phase
                            next_phase = None
                            if 'cities' in selected_phases:
                                next_phase = 'cities'
                            elif 'bank_city' in selected_phases:
                                next_phase = 'bank_city'
                                
                            if next_phase:
                                print(f"Moving to {next_phase} phase...")
                                global_data['search_phase'] = next_phase
                                global_data['city_index'] = 0
                                global_data['keyword_index'] = 0
                            else:
                                print("No more selected phases to run.")
                                break
                        else:
                            # Try next keyword
                            global_data['keyword_index'] = keyword_index
                        consecutive_failures = 0
                        session_recreate_count = 0  # Reset session recreation counter
                        continue
                    
                    # Attempt browser recovery with proper parameters
                    if recover_browser(headless=headless, driver_path=driver_path, proxy=proxy, timeout=timeout):
                        session_recreate_count += 1
                        if session_recreate_count >= max_session_recreations:
                            print(f"Browser had to be recreated {session_recreate_count} times. Moving to next phase.")
                            
                            # Find the next selected phase
                            next_phase = None
                            if 'cities' in selected_phases:
                                next_phase = 'cities'
                            elif 'bank_city' in selected_phases:
                                next_phase = 'bank_city'
                                
                            if next_phase:
                                global_data['search_phase'] = next_phase
                                global_data['city_index'] = 0
                                global_data['keyword_index'] = 0
                            else:
                                print("No more selected phases to run.")
                                break
                                
                            consecutive_failures = 0
                            session_recreate_count = 0
                        continue
                    else:
                        print("Browser recovery failed, moving to next phase")
                        
                        # Find the next selected phase
                        next_phase = None
                        if 'cities' in selected_phases:
                            next_phase = 'cities'
                        elif 'bank_city' in selected_phases:
                            next_phase = 'bank_city'
                            
                        if next_phase:
                            global_data['search_phase'] = next_phase
                            global_data['city_index'] = 0
                            global_data['keyword_index'] = 0
                        else:
                            print("No more selected phases to run.")
                            break
                        continue
                
                # Reset counters on successful search
                consecutive_failures = 0
                session_recreate_count = 0
                
                results_before = len(global_data['names'])
                process_search_results(driver, max_results, max_scroll_attempts, scroll_pause_time)
                results_after = len(global_data['names'])
                results_found = results_after - results_before
                
                print(f"\nFound {results_found} places with keyword {current_keyword} in {target_area}. Total: {results_after}/{max_results}")
                save_current_progress()
                
                # IMPROVED: Check if we found any new results - if not, move faster
                if results_found == 0:
                    print(f"No new results found for {target_area} with keyword {current_keyword}")
                    keyword_index += 1
                    if keyword_index >= len(global_data['keywords']):
                        print(f"All keywords tried for {target_area} with no results. Moving to next phase.")
                        
                        # Find the next selected phase
                        next_phase = None
                        if 'cities' in selected_phases:
                            next_phase = 'cities'
                        elif 'bank_city' in selected_phases:
                            next_phase = 'bank_city'
                            
                        if next_phase:
                            global_data['search_phase'] = next_phase
                            global_data['city_index'] = 0
                            global_data['keyword_index'] = 0
                        else:
                            print("No more selected phases to run.")
                            break
                    else:
                        global_data['keyword_index'] = keyword_index
                    save_current_progress()
                    continue
                
                # Try next keyword
                keyword_index += 1
                if keyword_index >= len(global_data['keywords']):
                    # If we've tried all keywords, move to the next selected phase
                    print("\n--- All keywords tried. Moving to next phase ---")
                    
                    next_phase = None
                    if 'cities' in selected_phases:
                        next_phase = 'cities'
                    elif 'bank_city' in selected_phases:
                        next_phase = 'bank_city'
                        
                    if next_phase:
                        global_data['search_phase'] = next_phase
                        global_data['city_index'] = 0
                        global_data['keyword_index'] = 0
                    else:
                        print("No more selected phases to run.")
                        break
                else:
                    # Try next keyword
                    global_data['keyword_index'] = keyword_index
            
            # Phase 2: City search - FIXED VERSION
            elif global_data['search_phase'] == 'cities' and 'cities' in selected_phases:
                city_index = global_data['city_index']
                
                # PERBAIKAN: Log progres pencarian kota secara jelas
                print(f"\nCITY PROGRESS: Currently at city index {city_index}/{len(global_data['search_cities'])-1} ({(city_index/len(global_data['search_cities'])*100):.1f}% complete)")
                
                # VALIDATION: Check if city_index is valid
                if city_index >= len(global_data['search_cities']):
                    print(f"City index ({city_index}) exceeds available cities ({len(global_data['search_cities'])}). Moving to next phase.")
                    
                    # Find the next selected phase
                    next_phase = None
                    if 'bank_city' in selected_phases:
                        next_phase = 'bank_city'
                        
                    if next_phase:
                        global_data['search_phase'] = next_phase
                        global_data['bank_index'] = 0
                        global_data['city_index'] = 0
                        global_data['keyword_index'] = 0
                    else:
                        print("No more selected phases to run.")
                        break
                    
                    save_current_progress()  # Save progress before moving to next phase
                    continue
                
                city = global_data['search_cities'][city_index]
                keyword_index = global_data['keyword_index']
                current_keyword = global_data['keywords'][keyword_index]
                
                # PERBAIKAN: Simplified stuck detection - hanya track overall progress
                current_results_count = len(global_data['names'])
                if 'last_progress_check' not in global_data:
                    global_data['last_progress_check'] = current_results_count
                    global_data['no_progress_iterations'] = 0
                
                # Check overall progress (less aggressive than before)
                if current_results_count <= global_data['last_progress_check'] + 1:  # Very minimal progress
                    global_data['no_progress_iterations'] += 1
                else:
                    global_data['no_progress_iterations'] = 0
                    global_data['last_progress_check'] = current_results_count
                
                # Only skip to next phase if we've tried many cities with no progress
                if global_data['no_progress_iterations'] >= 20:  # Much more patient
                    tried_cities = global_data.get('processed_cities_count', 0)
                    if tried_cities >= 15:  # Only after trying many cities
                        print(f"No significant progress for {global_data['no_progress_iterations']} iterations after trying {tried_cities} cities.")
                        
                        # Find the next selected phase
                        next_phase = None
                        if 'bank_city' in selected_phases:
                            next_phase = 'bank_city'
                            
                        if next_phase:
                            print(f"SWITCHING to {next_phase} phase to find more results...")
                            global_data['search_phase'] = next_phase
                            global_data['bank_index'] = 0
                            global_data['city_index'] = 0
                            global_data['keyword_index'] = 0
                            global_data['no_progress_iterations'] = 0
                            save_current_progress()
                            continue
                        else:
                            print("No more selected phases to run after trying multiple cities with minimal results.")
                            break
                
                print(f"\n--- PHASE 2: Searching for {current_keyword} in {city} ({city_index+1}/{len(global_data['search_cities'])}) ---")
                print(f"Keyword {keyword_index+1}/{len(global_data['keywords'])}: {current_keyword}")
                print(f"Current progress: {len(global_data['names'])}/{max_results} places found")
                
                search_success = city_search(driver, city, current_keyword)
                
                if not search_success:
                    consecutive_failures += 1
                    print(f"Search failed for {current_keyword} in {city} (attempt {consecutive_failures}/{max_consecutive_failures})")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        print(f"Max consecutive failures reached. Moving to next keyword/city...")
                        consecutive_failures = 0
                        session_recreate_count = 0
                        
                        # PERBAIKAN: Simple progression logic
                        keyword_index += 1
                        if keyword_index >= len(global_data['keywords']):
                            # Finished all keywords for this city, move to next city
                            print(f"Completed all keywords for {city}. Moving to next city.")
                            city_index += 1
                            keyword_index = 0
                            global_data['processed_cities_count'] = global_data.get('processed_cities_count', 0) + 1
                            
                            # Check if we've finished all cities
                            if city_index >= len(global_data['search_cities']):
                                print(f"Completed all cities. Moving to next phase...")
                                next_phase = None
                                if 'bank_city' in selected_phases:
                                    next_phase = 'bank_city'
                                    
                                if next_phase:
                                    global_data['search_phase'] = next_phase
                                    global_data['bank_index'] = 0
                                    global_data['city_index'] = 0
                                    global_data['keyword_index'] = 0
                                else:
                                    print("No more selected phases to run.")
                                    break
                            else:
                                # Update indices for next city
                                global_data['city_index'] = city_index
                                global_data['keyword_index'] = keyword_index
                        else:
                            # Try next keyword with same city
                            global_data['keyword_index'] = keyword_index
                            
                        save_current_progress()
                        continue
                    
                    # Attempt browser recovery with proper parameters
                    if recover_browser(headless=headless, driver_path=driver_path, proxy=proxy, timeout=timeout):
                        session_recreate_count += 1
                        if session_recreate_count >= max_session_recreations:
                            print(f"Browser had to be recreated {session_recreate_count} times. Moving to next keyword/city.")
                            consecutive_failures = 0
                            session_recreate_count = 0
                            
                            # PERBAIKAN: Simple progression logic
                            keyword_index += 1
                            if keyword_index >= len(global_data['keywords']):
                                # Finished all keywords for this city, move to next city
                                print(f"Completed all keywords for {city}. Moving to next city.")
                                city_index += 1
                                keyword_index = 0
                                global_data['processed_cities_count'] = global_data.get('processed_cities_count', 0) + 1
                                
                                # Check if we've finished all cities
                                if city_index >= len(global_data['search_cities']):
                                    print(f"Completed all cities. Moving to next phase...")
                                    next_phase = None
                                    if 'bank_city' in selected_phases:
                                        next_phase = 'bank_city'
                                        
                                    if next_phase:
                                        global_data['search_phase'] = next_phase
                                        global_data['bank_index'] = 0
                                        global_data['city_index'] = 0
                                        global_data['keyword_index'] = 0
                                    else:
                                        print("No more selected phases to run.")
                                        break
                                else:
                                    # Update indices for next city
                                    global_data['city_index'] = city_index
                                    global_data['keyword_index'] = keyword_index
                            else:
                                # Try next keyword with same city
                                global_data['keyword_index'] = keyword_index
                                
                            save_current_progress()
                        continue
                    else:
                        print("Browser recovery failed, moving to next keyword/city")
                        
                        # PERBAIKAN: Simple progression logic
                        keyword_index += 1
                        if keyword_index >= len(global_data['keywords']):
                            # Finished all keywords for this city, move to next city
                            print(f"Completed all keywords for {city}. Moving to next city.")
                            city_index += 1
                            keyword_index = 0
                            global_data['processed_cities_count'] = global_data.get('processed_cities_count', 0) + 1
                            
                            # Check if we've finished all cities
                            if city_index >= len(global_data['search_cities']):
                                print(f"Completed all cities. Moving to next phase...")
                                next_phase = None
                                if 'bank_city' in selected_phases:
                                    next_phase = 'bank_city'
                                    
                                if next_phase:
                                    global_data['search_phase'] = next_phase
                                    global_data['bank_index'] = 0
                                    global_data['city_index'] = 0
                                    global_data['keyword_index'] = 0
                                else:
                                    print("No more selected phases to run.")
                                    break
                            else:
                                # Update indices for next city
                                global_data['city_index'] = city_index
                                global_data['keyword_index'] = keyword_index
                        else:
                            # Try next keyword with same city
                            global_data['keyword_index'] = keyword_index
                            
                        save_current_progress()
                        continue
                
                # Reset counters on successful search
                consecutive_failures = 0
                session_recreate_count = 0
                
                results_before = len(global_data['names'])
                process_search_results(driver, max_results, max_scroll_attempts, scroll_pause_time)
                results_after = len(global_data['names'])
                results_for_city = results_after - results_before
                
                print(f"\nFound {results_for_city} places with keyword {current_keyword} in {city}. Total: {results_after}/{max_results}")
                
                # PERBAIKAN: ALWAYS follow the normal progression - keyword by keyword, then city by city
                keyword_index += 1
                if keyword_index >= len(global_data['keywords']):
                    # Finished all keywords for this city, move to next city
                    print(f"Completed all {len(global_data['keywords'])} keywords for {city}. Moving to next city.")
                    city_index += 1
                    keyword_index = 0
                    global_data['processed_cities_count'] = global_data.get('processed_cities_count', 0) + 1
                    
                    # Check if we've finished all cities
                    if city_index >= len(global_data['search_cities']):
                        print(f"Completed all cities in Phase 2. Moving to next phase...")
                        next_phase = None
                        if 'bank_city' in selected_phases:
                            next_phase = 'bank_city'
                            
                        if next_phase:
                            global_data['search_phase'] = next_phase
                            global_data['bank_index'] = 0
                            global_data['city_index'] = 0
                            global_data['keyword_index'] = 0
                        else:
                            print("No more selected phases to run.")
                            break
                    else:
                        # Update indices for next city
                        global_data['city_index'] = city_index
                        global_data['keyword_index'] = keyword_index
                else:
                    # Continue with next keyword for same city
                    global_data['keyword_index'] = keyword_index
                    print(f"Moving to next keyword ({keyword_index+1}/{len(global_data['keywords'])}) for {city}")
                
                # Save progress after each successful search
                save_current_progress()
                
                if results_after >= max_results:
                    print(f"Reached target of {max_results} results!")
                    break
            
            # Phase 3: Bank+City search - SIGNIFICANTLY IMPROVED
            elif global_data['search_phase'] == 'bank_city' and 'bank_city' in selected_phases:
                bank_index = global_data['bank_index']
                city_index = global_data['city_index']
                
                # Validasi bank index
                if bank_index >= len(global_data['banks']):
                    print("Completed all bank+city combinations!")
                    break
                
                # Get current bank name
                bank = global_data['banks'][bank_index]
                
                # Show progress of processing ALL cities for current bank
                total_cities = len(global_data['search_cities'])
                if bank not in global_data['cities_per_bank']:
                    global_data['cities_per_bank'][bank] = 0
                
                cities_processed_for_bank = global_data['cities_per_bank'].get(bank, 0)
                print(f"\nBANK+CITY PROGRESS: Bank {bank_index+1}/{len(global_data['banks'])} ({bank.upper()})")
                print(f"Cities processed for {bank.upper()}: {cities_processed_for_bank}/{total_cities} ({cities_processed_for_bank/total_cities*100:.1f}%)")
                
                # Validasi city index - CRITICAL FIX: Only move to next bank after ALL cities are processed
                if city_index >= len(global_data['search_cities']):
                    print(f"\nCompleted all cities for bank {bank.upper()}! Moving to next bank.")
                    bank_index += 1
                    city_index = 0
                    global_data['city_index'] = city_index
                    global_data['bank_index'] = bank_index
                    
                    # Reset cities processed counter for the new bank
                    if bank_index < len(global_data['banks']):
                        new_bank = global_data['banks'][bank_index]
                        if new_bank not in global_data['cities_per_bank']:
                            global_data['cities_per_bank'][new_bank] = 0
                    
                    save_current_progress()
                    
                    if bank_index >= len(global_data['banks']):
                        print("Completed all bank+city combinations!")
                        break
                    continue
                
                # Get current city name
                city = global_data['search_cities'][city_index]
                keyword_index = global_data['keyword_index']
                current_keyword = global_data['keywords'][keyword_index]
                
                print(f"\n--- PHASE 3: Searching for {current_keyword} {bank.upper()} in {city} ---")
                print(f"Bank: {bank_index+1}/{len(global_data['banks'])}, City: {city_index+1}/{len(global_data['search_cities'])}")
                print(f"Current progress: {len(global_data['names'])}/{max_results} places found")
                
                search_success = bank_city_search(driver, bank, city, current_keyword)
                
                if not search_success:
                    consecutive_failures += 1
                    print(f"Search failed for {current_keyword} {bank} in {city} (attempt {consecutive_failures}/{max_consecutive_failures})")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        print(f"Max consecutive failures reached. Trying next keyword or combination...")
                        keyword_index += 1
                        if keyword_index >= len(global_data['keywords']):
                            # If we've tried all keywords, move to next city
                            keyword_index = 0
                            city_index += 1
                            
                            # Track city processed for this bank
                            global_data['cities_per_bank'][bank] = global_data['cities_per_bank'].get(bank, 0) + 1
                            
                            # Only if we've finished all cities, move to next bank
                            if city_index >= len(global_data['search_cities']):
                                print(f"Completed all cities for bank {bank.upper()}! Moving to next bank.")
                                city_index = 0
                                bank_index += 1
                                
                                # Reset cities processed counter for the new bank
                                if bank_index < len(global_data['banks']):
                                    new_bank = global_data['banks'][bank_index]
                                    if new_bank not in global_data['cities_per_bank']:
                                        global_data['cities_per_bank'][new_bank] = 0
                        
                        global_data['keyword_index'] = keyword_index
                        global_data['bank_index'] = bank_index
                        global_data['city_index'] = city_index
                        consecutive_failures = 0
                        session_recreate_count = 0  # Reset session recreation counter
                        continue
                    
                    # Attempt browser recovery with proper parameters
                    if recover_browser(headless=headless, driver_path=driver_path, proxy=proxy, timeout=timeout):
                        session_recreate_count += 1
                        if session_recreate_count >= max_session_recreations:
                            print(f"Browser had to be recreated {session_recreate_count} times. Moving to next combination.")
                            keyword_index += 1
                            if keyword_index >= len(global_data['keywords']):
                                # If we've tried all keywords, move to next city
                                keyword_index = 0
                                city_index += 1
                                
                                # Track city processed for this bank
                                global_data['cities_per_bank'][bank] = global_data['cities_per_bank'].get(bank, 0) + 1
                                
                                # Only if we've finished all cities, move to next bank
                                if city_index >= len(global_data['search_cities']):
                                    print(f"Completed all cities for bank {bank.upper()}! Moving to next bank.")
                                    city_index = 0
                                    bank_index += 1
                                    
                                    # Reset cities processed counter for the new bank
                                    if bank_index < len(global_data['banks']):
                                        new_bank = global_data['banks'][bank_index]
                                        if new_bank not in global_data['cities_per_bank']:
                                            global_data['cities_per_bank'][new_bank] = 0
                            
                            global_data['keyword_index'] = keyword_index
                            global_data['bank_index'] = bank_index
                            global_data['city_index'] = city_index
                            consecutive_failures = 0
                            session_recreate_count = 0
                        continue
                    else:
                        print("Browser recovery failed, moving to next combination")
                        keyword_index += 1
                        if keyword_index >= len(global_data['keywords']):
                            # If we've tried all keywords, move to next city
                            keyword_index = 0
                            city_index += 1
                            
                            # Track city processed for this bank
                            global_data['cities_per_bank'][bank] = global_data['cities_per_bank'].get(bank, 0) + 1
                            
                            # Only if we've finished all cities, move to next bank
                            if city_index >= len(global_data['search_cities']):
                                print(f"Completed all cities for bank {bank.upper()}! Moving to next bank.")
                                city_index = 0
                                bank_index += 1
                                
                                # Reset cities processed counter for the new bank
                                if bank_index < len(global_data['banks']):
                                    new_bank = global_data['banks'][bank_index]
                                    if new_bank not in global_data['cities_per_bank']:
                                        global_data['cities_per_bank'][new_bank] = 0
                        
                        global_data['keyword_index'] = keyword_index
                        global_data['bank_index'] = bank_index
                        global_data['city_index'] = city_index
                        continue
                
                # Reset counters on successful search
                consecutive_failures = 0
                session_recreate_count = 0
                
                results_before = len(global_data['names'])
                process_search_results(driver, max_results, max_scroll_attempts, scroll_pause_time)
                results_after = len(global_data['names'])
                results_for_combo = results_after - results_before
                
                print(f"\nFound {results_for_combo} places with keyword {current_keyword} for {bank.upper()} in {city}. Total: {results_after}/{max_results}")
                save_current_progress()
                
                # Try next keyword or move to next combination (normal flow)
                keyword_index += 1
                if keyword_index >= len(global_data['keywords']):
                    # If we've tried all keywords, move to next city
                    keyword_index = 0
                    city_index += 1
                    
                    # Track city processed for this bank
                    global_data['cities_per_bank'][bank] = global_data['cities_per_bank'].get(bank, 0) + 1
                    
                    # Only if we've finished all cities, move to next bank
                    if city_index >= len(global_data['search_cities']):
                        print(f"Completed all cities for bank {bank.upper()}! Moving to next bank.")
                        city_index = 0
                        bank_index += 1
                        
                        # Reset cities processed counter for the new bank
                        if bank_index < len(global_data['banks']):
                            new_bank = global_data['banks'][bank_index]
                            if new_bank not in global_data['cities_per_bank']:
                                global_data['cities_per_bank'][new_bank] = 0
                
                global_data['keyword_index'] = keyword_index
                global_data['bank_index'] = bank_index
                global_data['city_index'] = city_index
                
                if results_after >= max_results:
                    print(f"Reached target of {max_results} results!")
                    break
            
            # Current phase is not in selected_phases, move to the next selected phase
            elif global_data['search_phase'] not in selected_phases:
                current_phase = global_data['search_phase']
                print(f"Current phase '{current_phase}' is not in selected phases.")
                
                # Find the next appropriate phase
                phases_order = ['primary_area', 'cities', 'bank_city']
                current_index = phases_order.index(current_phase) if current_phase in phases_order else -1
                
                next_phases = [p for p in selected_phases if p in phases_order and phases_order.index(p) > current_index]
                
                if next_phases:
                    next_phase = next_phases[0]
                    print(f"Moving to next selected phase: {next_phase}")
                    global_data['search_phase'] = next_phase
                    
                    # Reset indices for the new phase
                    if next_phase == 'cities':
                        global_data['city_index'] = 0
                        global_data['keyword_index'] = 0
                    elif next_phase == 'bank_city':
                        global_data['bank_index'] = 0
                        global_data['city_index'] = 0
                        global_data['keyword_index'] = 0
                else:
                    print("No more selected phases to run!")
                    break
            
            # Unknown phase
            else:
                print("Unknown search phase - terminating.")
                break
        
        # Final save
        save_current_progress()
        
        # Create DataFrame
        data = {
            'Name': global_data['names'],
            'Category': global_data['categories'],
            'Bank_Name': global_data['bank_names'],
            'Address': global_data['addresses'],
            'City': global_data['result_cities'] if len(global_data['result_cities']) == len(global_data['names']) else global_data['cities'],
            'ZIP_Code': global_data['zip_codes'],
            'Province': global_data['provinces'],
            'Link': global_data['links'],
            'Latitude': global_data['latitudes'],
            'Longitude': global_data['longitudes'],
            'Rating': global_data['ratings'],
            'Reviews_Count': global_data['reviews_counts'],
            'Phone': global_data['phones'],
            'Website': global_data['websites'],
            'Opening_Hours': global_data['opening_hours'],
            'Star_1': global_data['stars_1'],
            'Star_2': global_data['stars_2'],
            'Star_3': global_data['stars_3'],
            'Star_4': global_data['stars_4'],
            'Star_5': global_data['stars_5']
        }
        df = pd.DataFrame(data)
        
        # Apply city validation to fix any remaining issues
        df = validate_and_fix_cities(df)
        
        # Print summary
        print(f"\nScraping completed. Found {len(df)} locations.")
        
        if global_data['search_phase'] == 'primary_area':
            print(f"Search completed in {target_area} phase only.")
        elif global_data['search_phase'] == 'cities':
            city_index = min(global_data['city_index'], len(global_data['search_cities'])-1)
            progress = (city_index / len(global_data['search_cities'])) * 100
            print(f"Search completed in cities phase. Searched {city_index+1}/{len(global_data['search_cities'])} cities ({progress:.1f}%).")
        else:
            # More detailed bank+city progress summary
            total_cities = len(global_data['search_cities'])
            bank_progress = {}
            
            for bank, cities_done in global_data['cities_per_bank'].items():
                bank_progress[bank] = (cities_done / total_cities) * 100
                
            print(f"Search completed in bank+city phase.")
            print(f"Bank progress summary:")
            for bank, progress in bank_progress.items():
                print(f"- {bank.upper()}: {global_data['cities_per_bank'].get(bank, 0)}/{total_cities} cities ({progress:.1f}%)")
            
            print(f"Total cities processed across all banks: {global_data['processed_cities_count']}")
        
        return df
        
    except Exception as e:
        print(f"Error in main scraper function: {e}")
        import traceback
        traceback.print_exc()
        
        if len(global_data['names']) > 0:
            save_current_progress()
        
        # Create DataFrame even on error, with available data
        data = {
            'Name': global_data['names'],
            'Category': global_data['categories'],
            'Bank_Name': global_data['bank_names'],
            'Address': global_data['addresses'],
            'City': global_data['result_cities'] if len(global_data['result_cities']) == len(global_data['names']) else global_data['cities'],
            'ZIP_Code': global_data['zip_codes'],
            'Province': global_data['provinces'],
            'Link': global_data['links'],
            'Latitude': global_data['latitudes'],
            'Longitude': global_data['longitudes'],
            'Rating': global_data['ratings'],
            'Reviews_Count': global_data['reviews_counts'],
            'Phone': global_data['phones'],
            'Website': global_data['websites'],
            'Opening_Hours': global_data['opening_hours'],
            'Star_1': global_data['stars_1'],
            'Star_2': global_data['stars_2'],
            'Star_3': global_data['stars_3'],
            'Star_4': global_data['stars_4'],
            'Star_5': global_data['stars_5']
        }
        df = pd.DataFrame(data)
        
        # Still apply validation even on error exit
        df = validate_and_fix_cities(df)
        
        return df
        
    finally:
        if driver is not None:
            try:
                driver.quit()
                print("Browser closed successfully")
            except Exception as e:
                print(f"Error closing browser: {e}")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Google Maps Place Scraper with Full Details Extraction')
    parser.add_argument('--area', type=str, default="Yogyakarta",
                      help='Target area focus (default: "Yogyakarta")')
    parser.add_argument('--max', type=int, default=5000,
                      help='Maximum number of places to collect (default: 5000)')
    parser.add_argument('--scroll-pause', type=float, default=1.5,
                      help='Pause time between scrolls in seconds (default: 1.5)')
    parser.add_argument('--max-scroll', type=int, default=20,
                      help='Maximum scroll attempts per search (default: 20)')
    parser.add_argument('--resume', type=str, default=None,
                      help='Resume from checkpoint file')
    parser.add_argument('--headless', action='store_true',
                      help='Run in headless mode (no browser UI)')
    parser.add_argument('--driver-path', type=str, default=None,
                      help='Path to custom ChromeDriver')
    parser.add_argument('--proxy', type=str, default=None,
                      help='Use proxy in format http://user:pass@host:port')
    parser.add_argument('--timeout', type=int, default=30,
                      help='Timeout for page loading in seconds (default: 30)')
    parser.add_argument('--cities-csv', type=str, default=None,
                      help='Path to CSV file containing cities to search')
    parser.add_argument('--force-next-bank', action='store_true',
                      help='Force move to next bank when resuming')
    parser.add_argument('--force-next-city', action='store_true',
                      help='Force move to next city when resuming')
    parser.add_argument('--force-next-phase', action='store_true',
                      help='Force move to next search phase (cities -> bank_city)')
    parser.add_argument('--phases', type=str, default="primary_area,cities,bank_city",
                      help='Comma-separated list of phases to run (options: primary_area,cities,bank_city)')
    # Add new argument for max cities
    parser.add_argument('--max-cities', type=int, default=100,
                      help='Maximum number of cities to process (default: 100)')
    
    args = parser.parse_args()
    
    # Parse the phases to run
    selected_phases = [phase.strip() for phase in args.phases.split(',')]
    valid_phases = ['primary_area', 'cities', 'bank_city']
    
    # Validate selected phases
    for phase in selected_phases[:]:  # Use a slice copy to avoid modifying during iteration
        if phase not in valid_phases:
            print(f"Warning: Invalid phase '{phase}'. Valid options are: {', '.join(valid_phases)}")
            selected_phases.remove(phase)
    
    if not selected_phases:
        print("Error: No valid phases selected. Using all phases as default.")
        selected_phases = valid_phases
    
    print(f"Starting Enhanced Google Maps Scraper with target area: {args.area}")
    print(f"Maximum places to collect: {args.max}")
    print(f"Search phases to run: {', '.join(selected_phases)}")
    print(f"Search keywords: KC (Kantor Cabang), KCP (Kantor Cabang Pembantu)")
    print(f"Maximum cities to process: {args.max_cities}")
    
    # Process special force arguments if present
    if args.resume:
        try:
            with open(args.resume, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
                
            modified = False
                
            # Force move to next bank
            if args.force_next_bank:
                print("Forcing move to next bank...")
                # Increment bank index, reset others
                if 'bank_index' in checkpoint:
                    checkpoint['bank_index'] = checkpoint['bank_index'] + 1
                    checkpoint['city_index'] = 0
                    checkpoint['keyword_index'] = 0
                    modified = True
                    print(f"Updated checkpoint to force next bank. New bank index: {checkpoint['bank_index']}")
            
            # Force move to next city
            if args.force_next_city:
                print("Forcing move to next city...")
                if 'city_index' in checkpoint:
                    checkpoint['city_index'] = checkpoint['city_index'] + 1
                    checkpoint['keyword_index'] = 0
                    modified = True
                    print(f"Updated checkpoint to force next city. New city index: {checkpoint['city_index']}")
            
            # Force move to next phase
            if args.force_next_phase:
                print("Forcing move to next search phase...")
                if checkpoint.get('search_phase') == 'primary_area':
                    next_phase = None
                    if 'cities' in selected_phases:
                        next_phase = 'cities'
                    elif 'bank_city' in selected_phases:
                        next_phase = 'bank_city'
                        
                    if next_phase:
                        checkpoint['search_phase'] = next_phase
                        checkpoint['city_index'] = 0
                        checkpoint['keyword_index'] = 0
                        modified = True
                        print(f"Updated checkpoint: primary_area -> {next_phase}")
                    else:
                        print("Can't force next phase - no next phase is selected")
                        
                elif checkpoint.get('search_phase') == 'cities':
                    if 'bank_city' in selected_phases:
                        checkpoint['search_phase'] = 'bank_city'
                        checkpoint['bank_index'] = 0
                        checkpoint['city_index'] = 0
                        checkpoint['keyword_index'] = 0
                        modified = True
                        print("Updated checkpoint: cities -> bank_city")
                    else:
                        print("Can't force next phase - bank_city is not selected")
            
            # Save modified checkpoint
            if modified:
                with open(args.resume, 'w', encoding='utf-8') as f:
                    json.dump(checkpoint, f, ensure_ascii=False, indent=2)
                print("Checkpoint updated successfully.")
                
        except Exception as e:
            print(f"Error processing checkpoint: {e}")
    
    if args.cities_csv:
        print(f"Using cities from: {args.cities_csv}")
    else:
        print("Using default Indonesian cities list")
    
    # Set up signal handlers with priority - do this early
    try:
        signal.signal(signal.SIGINT, save_current_progress)
        signal.signal(signal.SIGTERM, save_current_progress)
        print("Signal handlers registered for clean termination")
    except Exception as e:
        print(f"Warning: Could not set up signal handlers: {e}")
    
    # Run the scraper with comprehensive error handling
    df = None
    try:
        df = scrape_google_maps(
            target_area=args.area,
            max_results=args.max,
            scroll_pause_time=args.scroll_pause,
            max_scroll_attempts=args.max_scroll,
            resume_from=args.resume,
            headless=args.headless,
            driver_path=args.driver_path,
            proxy=args.proxy,
            timeout=args.timeout,
            cities_csv=args.cities_csv,
            selected_phases=selected_phases,  # Pass the selected phases to the scraper
            max_cities=args.max_cities  # Add the max cities parameter
        )
        
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Attempting to save progress...")
        if 'global_data' in globals() and len(global_data.get('names', [])) > 0:
            save_current_progress()
        print("Exiting after interrupt.")
        # Don't return here - let the code proceed to save any results that might be available
        
    except Exception as e:
        print(f"\nUnexpected error in main scraper: {e}")
        import traceback
        traceback.print_exc()
        if 'global_data' in globals() and len(global_data.get('names', [])) > 0:
            print("Attempting to save collected data before exit...")
            save_current_progress()
        print("\nScraper terminated with error, but we'll try to save any collected data.")
    
    # Save results even if there was an error, as long as we have some data
    try:
        # If df was never created but we have global data, create a DataFrame now
        if df is None and 'global_data' in globals() and len(global_data.get('names', [])) > 0:
            # Make sure all arrays have the same length
            max_length = len(global_data['names'])
            for key in global_data:
                if isinstance(global_data[key], list):
                    current_length = len(global_data[key])
                    if current_length < max_length:
                        global_data[key].extend([None] * (max_length - current_length))
            
            # Create DataFrame from global data
            data = {
                'Name': global_data['names'],
                'Category': global_data['categories'],
                'Bank_Name': global_data['bank_names'],
                'Address': global_data['addresses'],
                'City': global_data['cities'],
                'ZIP_Code': global_data['zip_codes'],
                'Province': global_data['provinces'],
                'Link': global_data['links'],
                'Latitude': global_data['latitudes'],
                'Longitude': global_data['longitudes'],
                'Rating': global_data['ratings'],
                'Reviews_Count': global_data['reviews_counts'],
                'Phone': global_data['phones'],
                'Website': global_data['websites'],
                'Opening_Hours': global_data['opening_hours'],
                'Star_1': global_data['stars_1'],
                'Star_2': global_data['stars_2'],
                'Star_3': global_data['stars_3'],
                'Star_4': global_data['stars_4'],
                'Star_5': global_data['stars_5']
            }
            df = pd.DataFrame(data)
            print(f"Created DataFrame from global data with {len(df)} records")
                
        if df is not None and len(df) > 0:
            os.makedirs('results', exist_ok=True)
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            formatted_area = args.area.replace(' ', '_').lower()
            
            # Include selected phases in the filename
            phases_str = '-'.join([p[:3] for p in selected_phases])
            
            # Save CSV with empty strings for missing values
            csv_filename = f"results/places_data_{formatted_area}_{phases_str}_{timestamp}.csv"
            df.to_csv(csv_filename, index=False, encoding='utf-8', na_rep='')
            print(f"Results saved to {csv_filename}")
            
            # Try to save as Excel with empty strings for missing values
            try:
                excel_filename = f"results/places_data_{formatted_area}_{phases_str}_{timestamp}.xlsx"
                df.to_excel(excel_filename, index=False, engine='openpyxl', na_rep='')
                print(f"Results also saved as Excel file: {excel_filename}")
            except Exception as e:
                print(f"Could not save Excel file: {e}")                
                
            # Generate summary
            print("\n===== SCRAPING SUMMARY =====")
            print(f"Total Places found: {len(df)}")
            print(f"Phases run: {', '.join(selected_phases)}")
            
            # Rest of the summary code remains the same...
            
        else:
            print("No results were collected or dataframe could not be created.")
    
    except Exception as save_error:
        print(f"Error saving final results: {save_error}")
        import traceback
        traceback.print_exc()
        print("Unable to save results properly.")

    print("\nScript execution finished.")

def save_current_progress(signal_number=None, frame=None):
    """Save current progress to files with proper cleanup and kecamatan support"""
    global driver  # Make sure we have access to the driver
    
    print("\nSaving progress and cleaning up...")
    
    # First close the browser if it exists to prevent hanging
    if 'driver' in globals() and driver is not None:
        try:
            print("Closing browser...")
            driver.quit()
            print("Browser closed successfully")
        except Exception as e:
            print(f"Error closing browser during interrupt: {e}")
        finally:
            # Set to None to avoid further access attempts
            driver = None
    
    if len(global_data['names']) == 0:
        print("No data to save yet.")
        if signal_number:
            print("Exiting cleanly.")
            os._exit(0)  # Force exit to avoid hanging
        return
    
    try:
        # Define which arrays are data arrays (should all have the same length)
        data_arrays = [
            'names', 'categories', 'bank_names', 'addresses', 'cities', 
            'kecamatans',  # NEW: Add kecamatans to data arrays
            'zip_codes', 'provinces', 'links', 'latitudes', 'longitudes',
            'ratings', 'reviews_counts', 'phones', 'websites', 'opening_hours',
            'stars_1', 'stars_2', 'stars_3', 'stars_4', 'stars_5'
        ]
        
        # Get the length of the names array as the reference length
        max_length = len(global_data['names'])
        
        # Create a dictionary with only data arrays, ensuring they all have the same length
        data_dict = {}
        for key in data_arrays:
            if key in global_data:
                # Make sure array exists and has data
                if isinstance(global_data[key], list):
                    current_length = len(global_data[key])
                    
                    # Pad shorter arrays to match the length of names
                    if current_length < max_length:
                        print(f"Fixing length of {key} from {current_length} to {max_length}")
                        global_data[key].extend([None] * (max_length - current_length))
                    
                    # Truncate longer arrays (shouldn't happen, but just in case)
                    elif current_length > max_length:
                        print(f"Truncating {key} from {current_length} to {max_length}")
                        global_data[key] = global_data[key][:max_length]
                    
                    # Add to our data dictionary with renamed keys for the DataFrame
                    data_dict[key.capitalize()] = global_data[key]
            else:
                # If the key doesn't exist in global_data, create an array of Nones
                print(f"Creating missing array {key} with length {max_length}")
                data_dict[key.capitalize()] = [None] * max_length
        
        # Map the keys to proper DataFrame column names
        column_mapping = {
            'Names': 'Name',
            'Categories': 'Category',
            'Bank_names': 'Bank_Name',
            'Addresses': 'Address',
            'Cities': 'City',
            'Kecamatans': 'Kecamatan',  # NEW: Add kecamatan column mapping
            'Zip_codes': 'ZIP_Code',
            'Provinces': 'Province',
            'Links': 'Link',
            'Latitudes': 'Latitude',
            'Longitudes': 'Longitude',
            'Ratings': 'Rating',
            'Reviews_counts': 'Reviews_Count',
            'Phones': 'Phone',
            'Websites': 'Website',
            'Opening_hours': 'Opening_Hours',
            'Stars_1': 'Star_1',
            'Stars_2': 'Star_2',
            'Stars_3': 'Star_3',
            'Stars_4': 'Star_4',
            'Stars_5': 'Star_5'
        }
        
        # Rename keys according to mapping
        renamed_data = {}
        for key, value in data_dict.items():
            if key in column_mapping:
                renamed_data[column_mapping[key]] = value
            else:
                renamed_data[key] = value
        
        # Create DataFrame from the renamed data
        df = pd.DataFrame(renamed_data)
        
        # Apply city validation
        df = validate_and_fix_cities(df)
        
        # Now save to CSV
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        os.makedirs('checkpoints', exist_ok=True)
        csv_filename = f"checkpoints/google_maps_results_{timestamp}.csv"
        df.to_csv(csv_filename, index=False, encoding='utf-8')
            
        # Save checkpoint - using only non-data arrays
        checkpoint_data = {
            'search_query': global_data.get('search_query', ""),
            'processed_elements': list(global_data.get('processed_elements', set())),
            'seen_name_pairs': [list(pair) for pair in global_data.get('seen_name_pairs', set())],
            'seen_links': list(global_data.get('seen_links', set())),
            'seen_coordinates': [list(coord) for coord in global_data.get('seen_coordinates', set())],
            'coordinates_searched': list(global_data.get('coordinates_searched', set())),
            'target_area': global_data.get('target_area', ""),
            'last_coords': global_data.get('last_coords'),
            'search_phase': global_data.get('search_phase', 'primary_area'),
            'bank_index': global_data.get('bank_index', 0),
            'city_index': global_data.get('city_index', 0),
            'keyword_index': global_data.get('keyword_index', 0),
            'timestamp': timestamp
        }
        
        checkpoint_filename = f"checkpoints/checkpoint_{timestamp}.json"
        with open(checkpoint_filename, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
        
        print(f"Progress saved to checkpoints/ directory with timestamp {timestamp}")
    except Exception as e:
        print(f"Error saving progress: {e}")
        import traceback
        traceback.print_exc()
    
    if signal_number:
        print("Script interrupted. Progress saved. Exiting now.")
        os._exit(0)  # Using os._exit instead of sys.exit for immediate termination

def clean_duplicates_from_dataset(csv_file):
    """Clean duplicates from an existing dataset using multiple criteria"""
    print(f"Loading dataset from {csv_file}...")
    df = pd.read_csv(csv_file)
    
    original_count = len(df)
    print(f"Original dataset has {original_count} records")
    
    # 1. Remove exact duplicates first
    df_step1 = df.drop_duplicates()
    step1_count = len(df_step1)
    print(f"After removing exact duplicates: {step1_count} records ({original_count - step1_count} removed)")
    
    # 2. Remove duplicates by URL
    df_step2 = df_step1.drop_duplicates(subset=['Link'])
    step2_count = len(df_step2)
    print(f"After removing URL duplicates: {step2_count} records ({step1_count - step2_count} removed)")
    
    # 3. Remove duplicates by coordinates with 5-decimal precision
    if 'Latitude' in df_step2.columns and 'Longitude' in df_step2.columns:
        df_step2['Lat_Rounded'] = df_step2['Latitude'].round(5)
        df_step2['Long_Rounded'] = df_step2['Longitude'].round(5)
        df_step3 = df_step2.drop_duplicates(subset=['Lat_Rounded', 'Long_Rounded'])
        step3_count = len(df_step3)
        print(f"After removing coordinate duplicates: {step3_count} records ({step2_count - step3_count} removed)")
    else:
        df_step3 = df_step2
        step3_count = step2_count
        print("No coordinate columns found, skipping coordinate-based deduplication")
    
    # 4. Save cleaned dataset
    output_file = csv_file.replace('.csv', '_cleaned.csv')
    df_final = df_step3.drop(columns=['Lat_Rounded', 'Long_Rounded'], errors='ignore')
    df_final.to_csv(output_file, index=False)
    
    print(f"Cleaned dataset saved to {output_file}")
    print(f"Total duplicates removed: {original_count - step3_count} ({(original_count - step3_count)/original_count*100:.2f}%)")
    
    return df_final

# Usage example:
# clean_duplicates_from_dataset('your_dataset.csv')

def validate_and_fix_cities(dataframe):
    """
    Memperbaiki nilai kota dalam DataFrame berdasarkan alamat
    Gunakan setelah data sudah dikumpulkan untuk mengoreksi kesalahan
    """
    print("Validating and fixing city values...")
    
    # Daftar kota besar yang perlu diprioritaskan
    major_cities = [
        "Jakarta", "Surabaya", "Bandung", "Medan", "Semarang", 
        "Makassar", "Palembang", "Tangerang", "Depok", "Bekasi",
        "Bogor", "Malang", "Solo", "Denpasar", "Yogyakarta",
        "Balikpapan", "Banjarmasin", "Padang", "Pekanbaru", "Batam"
    ]
    
    # Jika tidak ada data, kembalikan DataFrame asli
    if len(dataframe) == 0 or 'Address' not in dataframe.columns:
        return dataframe
    
    # Membuat kolom City baru untuk hasil perbaikan
    corrected_cities = []
    fixed_count = 0
    
    # Periksa setiap baris
    for index, row in dataframe.iterrows():
        address = row['Address'] if not pd.isna(row['Address']) else ""
        current_city = row['City'] if 'City' in dataframe.columns and not pd.isna(row['City']) else "Unknown"
        
        # Flag untuk menandai apakah koreksi perlu dilakukan
        needs_correction = False
        
        # Cek apakah ada kota besar dalam alamat yang tidak tercermin dalam kolom City
        for city in major_cities:
            if city in address and city not in current_city:
                corrected_cities.append(city)
                needs_correction = True
                fixed_count += 1
                break
                
        # Jika tidak perlu koreksi, pertahankan nilai asli
        if not needs_correction:
            corrected_cities.append(current_city)
    
    # Buat salinan DataFrame dengan kolom City yang telah diperbaiki
    result_df = dataframe.copy()
    result_df['City'] = corrected_cities
    
    print(f"Fixed {fixed_count} city values out of {len(dataframe)} records")
    
    return result_df

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Google Maps Place Scraper with Full Details Extraction')
    parser.add_argument('--area', type=str, default="Yogyakarta",
                      help='Target area focus (default: "Yogyakarta")')
    parser.add_argument('--max', type=int, default=5000,
                      help='Maximum number of places to collect (default: 5000)')
    parser.add_argument('--scroll-pause', type=float, default=1.5,
                      help='Pause time between scrolls in seconds (default: 1.5)')
    parser.add_argument('--max-scroll', type=int, default=20,
                      help='Maximum scroll attempts per search (default: 20)')
    parser.add_argument('--resume', type=str, default=None,
                      help='Resume from checkpoint file')
    parser.add_argument('--headless', action='store_true',
                      help='Run in headless mode (no browser UI)')
    parser.add_argument('--driver-path', type=str, default=None,
                      help='Path to custom ChromeDriver')
    parser.add_argument('--proxy', type=str, default=None,
                      help='Use proxy in format http://user:pass@host:port')
    parser.add_argument('--timeout', type=int, default=30,
                      help='Timeout for page loading in seconds (default: 30)')
    parser.add_argument('--cities-csv', type=str, default=None,
                      help='Path to CSV file containing cities to search')
    parser.add_argument('--force-next-bank', action='store_true',
                      help='Force move to next bank when resuming')
    parser.add_argument('--force-next-city', action='store_true',
                      help='Force move to next city when resuming')
    parser.add_argument('--force-next-phase', action='store_true',
                      help='Force move to next search phase (cities -> bank_city)')
    parser.add_argument('--phases', type=str, default="primary_area,cities,bank_city",
                      help='Comma-separated list of phases to run (options: primary_area,cities,bank_city)')
    
    args = parser.parse_args()
    
    # Parse the phases to run
    selected_phases = [phase.strip() for phase in args.phases.split(',')]
    valid_phases = ['primary_area', 'cities', 'bank_city']
    
    # Validate selected phases
    for phase in selected_phases:
        if phase not in valid_phases:
            print(f"Warning: Invalid phase '{phase}'. Valid options are: {', '.join(valid_phases)}")
            selected_phases.remove(phase)
    
    if not selected_phases:
        print("Error: No valid phases selected. Using all phases as default.")
        selected_phases = valid_phases
    
    print(f"Starting Enhanced Google Maps Scraper with target area: {args.area}")
    print(f"Maximum places to collect: {args.max}")
    print(f"Search phases to run: {', '.join(selected_phases)}")
    print(f"Search keywords: KC (Kantor Cabang), KCP (Kantor Cabang Pembantu)")
    
    # Process special force arguments if present
    if args.resume:
        try:
            with open(args.resume, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
                
            modified = False
                
            # Force move to next bank
            if args.force_next_bank:
                print("Forcing move to next bank...")
                # Increment bank index, reset others
                if 'bank_index' in checkpoint:
                    checkpoint['bank_index'] = checkpoint['bank_index'] + 1
                    checkpoint['city_index'] = 0
                    checkpoint['keyword_index'] = 0
                    modified = True
                    print(f"Updated checkpoint to force next bank. New bank index: {checkpoint['bank_index']}")
            
            # Force move to next city
            if args.force_next_city:
                print("Forcing move to next city...")
                if 'city_index' in checkpoint:
                    checkpoint['city_index'] = checkpoint['city_index'] + 1
                    checkpoint['keyword_index'] = 0
                    modified = True
                    print(f"Updated checkpoint to force next city. New city index: {checkpoint['city_index']}")
            
            # Force move to next phase
            if args.force_next_phase:
                print("Forcing move to next search phase...")
                if checkpoint.get('search_phase') == 'primary_area':
                    checkpoint['search_phase'] = 'cities'
                    checkpoint['city_index'] = 0
                    checkpoint['keyword_index'] = 0
                    modified = True
                    print("Updated checkpoint: primary_area -> cities")
                elif checkpoint.get('search_phase') == 'cities':
                    checkpoint['search_phase'] = 'bank_city'
                    checkpoint['bank_index'] = 0
                    checkpoint['city_index'] = 0
                    checkpoint['keyword_index'] = 0
                    modified = True
                    print("Updated checkpoint: cities -> bank_city")
            
            # Save modified checkpoint
            if modified:
                with open(args.resume, 'w', encoding='utf-8') as f:
                    json.dump(checkpoint, f, ensure_ascii=False, indent=2)
                print("Checkpoint updated successfully.")
                
        except Exception as e:
            print(f"Error processing checkpoint: {e}")
    
    if args.cities_csv:
        print(f"Using cities from: {args.cities_csv}")
    else:
        print("Using default Indonesian cities list")
    
    # Set up signal handlers with priority - do this early
    try:
        signal.signal(signal.SIGINT, save_current_progress)
        signal.signal(signal.SIGTERM, save_current_progress)
        print("Signal handlers registered for clean termination")
    except Exception as e:
        print(f"Warning: Could not set up signal handlers: {e}")
    
    # Run the scraper with comprehensive error handling
    df = None
    try:
        df = scrape_google_maps(
            target_area=args.area,
            max_results=args.max,
            scroll_pause_time=args.scroll_pause,
            max_scroll_attempts=args.max_scroll,
            resume_from=args.resume,
            headless=args.headless,
            driver_path=args.driver_path,
            proxy=args.proxy,
            timeout=args.timeout,
            cities_csv=args.cities_csv,
            selected_phases=selected_phases  # Pass the selected phases to the scraper
        )
        
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Attempting to save progress...")
        if 'global_data' in globals() and len(global_data.get('names', [])) > 0:
            save_current_progress()
        print("Exiting after interrupt.")
        # Don't return here - let the code proceed to save any results that might be available
        
    except Exception as e:
        print(f"\nUnexpected error in main scraper: {e}")
        import traceback
        traceback.print_exc()
        if 'global_data' in globals() and len(global_data.get('names', [])) > 0:
            print("Attempting to save collected data before exit...")
            save_current_progress()
        print("\nScraper terminated with error, but we'll try to save any collected data.")
    
    # Save results even if there was an error, as long as we have some data
    try:
        # If df was never created but we have global data, create a DataFrame now
        if df is None and 'global_data' in globals() and len(global_data.get('names', [])) > 0:
            # Make sure all arrays have the same length
            max_length = len(global_data['names'])
            for key in global_data:
                if isinstance(global_data[key], list):
                    current_length = len(global_data[key])
                    if current_length < max_length:
                        global_data[key].extend([None] * (max_length - current_length))
            
            # Create DataFrame from global data
            data = {
                'Name': global_data['names'],
                'Category': global_data['categories'],
                'Bank_Name': global_data['bank_names'],
                'Address': global_data['addresses'],
                'City': global_data['cities'],
                'ZIP_Code': global_data['zip_codes'],
                'Province': global_data['provinces'],
                'Link': global_data['links'],
                'Latitude': global_data['latitudes'],
                'Longitude': global_data['longitudes'],
                'Rating': global_data['ratings'],
                'Reviews_Count': global_data['reviews_counts'],
                'Phone': global_data['phones'],
                'Website': global_data['websites'],
                'Opening_Hours': global_data['opening_hours'],
                'Star_1': global_data['stars_1'],
                'Star_2': global_data['stars_2'],
                'Star_3': global_data['stars_3'],
                'Star_4': global_data['stars_4'],
                'Star_5': global_data['stars_5']
            }
            df = pd.DataFrame(data)
            print(f"Created DataFrame from global data with {len(df)} records")
                
        if df is not None and len(df) > 0:
            os.makedirs('results', exist_ok=True)
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            formatted_area = args.area.replace(' ', '_').lower()
            
            # Save CSV with empty strings for missing values
            csv_filename = f"results/places_data_{formatted_area}_{timestamp}.csv"
            df.to_csv(csv_filename, index=False, encoding='utf-8', na_rep='')
            print(f"Results saved to {csv_filename}")
            
            # Try to save as Excel with empty strings for missing values
            try:
                excel_filename = f"results/places_data_{formatted_area}_{timestamp}.xlsx"
                df.to_excel(excel_filename, index=False, engine='openpyxl', na_rep='')
                print(f"Results also saved as Excel file: {excel_filename}")
            except Exception as e:
                print(f"Could not save Excel file: {e}")                
                
            # Generate summary
            print("\n===== SCRAPING SUMMARY =====")
            print(f"Total Places found: {len(df)}")
            
            # Category summary
            if 'Category' in df.columns:
                category_counts = df['Category'].value_counts().head(10)
                print("\nTop 10 Categories:")
                for category, count in category_counts.items():
                    print(f"- {category}: {count}")
            
            # Bank name summary
            if 'Bank_Name' in df.columns:
                bank_counts = df['Bank_Name'].value_counts().head(10)
                print("\nTop 10 Banks Found:")
                for bank, count in bank_counts.items():
                    if bank and not pd.isna(bank):
                        print(f"- {bank}: {count}")
            
            # Province summary
            if 'Province' in df.columns:
                province_counts = df['Province'].value_counts()
                print("\nPlaces by Province:")
                for province, count in province_counts.items():
                    print(f"- {province}: {count}")
            
            # Rating summary
            if 'Rating' in df.columns:
                rating_avg = df['Rating'].mean()
                rating_count = df['Rating'].count()
                print(f"\nAverage Rating: {rating_avg:.2f} (from {rating_count} rated places)")
            
            # Coordinates extraction success rate
            coords_found = df[df['Latitude'].notna() & df['Longitude'].notna()].shape[0]
            coords_percent = (coords_found / len(df)) * 100 if len(df) > 0 else 0
            print(f"\nCoordinates extraction success rate: {coords_found}/{len(df)} ({coords_percent:.2f}%)")
            
            print("\nScraping session completed! Results have been saved.")
        else:
            print("No results were collected or dataframe could not be created.")
    
    except Exception as save_error:
        print(f"Error saving final results: {save_error}")
        import traceback
        traceback.print_exc()
        print("Unable to save results properly.")

    print("\nScript execution finished.")

if __name__ == "__main__":
    try:
        print("=" * 70)
        print("ENHANCED GOOGLE MAPS SCRAPER - WITH FULL DETAILS EXTRACTION")
        print("=" * 70)
        print("Version: 5.0 - Complete Data Collection")
        print("Date: May 2025")
        print("=" * 70)
        main()
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Any progress should have been saved.")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        if 'global_data' in globals() and len(global_data.get('names', [])) > 0:
            print("Attempting to save collected data before exit...")
            save_current_progress()
        print("\nScript terminated with error.")

# Usage example:
# python enhanced_scraper.py --area "Jakarta" --cities-csv cities/jakarta.csv --max 50000




