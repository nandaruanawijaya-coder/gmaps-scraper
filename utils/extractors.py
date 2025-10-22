"""
Utility functions for extracting data from Google Maps
"""
import re
from typing import Optional, Tuple


def extract_coordinates_from_link(link: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract latitude and longitude from Google Maps link
    
    Args:
        link: Google Maps URL
        
    Returns:
        Tuple of (latitude, longitude) or (None, None) if not found
    """
    try:
        # Pattern 1: !8m2!3d[latitude]!4d[longitude]
        pattern1 = r'!8m2!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)'
        match = re.search(pattern1, link)
        
        if match and len(match.groups()) == 2:
            latitude = float(match.group(1))
            longitude = float(match.group(2))
            return latitude, longitude
        
        # Pattern 2: @[latitude],[longitude]
        pattern2 = r'@(-?\d+\.\d+),(-?\d+\.\d+)'
        match = re.search(pattern2, link)
        
        if match and len(match.groups()) == 2:
            latitude = float(match.group(1))
            longitude = float(match.group(2))
            return latitude, longitude
        
        return None, None
        
    except Exception as e:
        print(f"Error extracting coordinates: {e}")
        return None, None


def parse_address(address: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Parse Indonesian address into components
    
    Args:
        address: Full address string
        
    Returns:
        Tuple of (district, city, province, zip_code)
    """
    if not address:
        return None, None, None, None
    
    district = None
    city = None
    province = None
    zip_code = None
    
    try:
        # Extract ZIP code (5 digits)
        zip_match = re.search(r'\b\d{5}\b', address)
        if zip_match:
            zip_code = zip_match.group(0)
        
        # Extract province (usually at the end)
        provinces = [
            'DKI Jakarta', 'Jawa Barat', 'Jawa Tengah', 'Jawa Timur',
            'Banten', 'Sumatera Utara', 'Sumatera Selatan', 'Sumatera Barat',
            'Kalimantan Timur', 'Kalimantan Selatan', 'Kalimantan Barat',
            'Sulawesi Selatan', 'Sulawesi Utara', 'Bali', 'Papua'
        ]
        for prov in provinces:
            if prov in address:
                province = prov
                break
        
        # Extract city (Kota/Kabupaten)
        city_match = re.search(r'(?:Kota|Kabupaten)\s+([^,]+)', address)
        if city_match:
            city = city_match.group(1).strip()
        else:
            # Try to find common city names
            cities = ['Jakarta Selatan', 'Jakarta Pusat', 'Jakarta Utara', 
                     'Jakarta Barat', 'Jakarta Timur', 'Bandung', 'Surabaya', 
                     'Medan', 'Semarang', 'Tangerang']
            for c in cities:
                if c in address:
                    city = c
                    break
        
        # Extract district (Kecamatan)
        district_match = re.search(r'(?:Kecamatan|Kec\.)\s+([^,]+)', address)
        if district_match:
            district = district_match.group(1).strip()
        
    except Exception as e:
        print(f"Error parsing address: {e}")
    
    return district, city, province, zip_code


def clean_text(text: Optional[str]) -> Optional[str]:
    """Clean and normalize text"""
    if not text:
        return None
    return text.strip().replace('\n', ' ').replace('\r', '')


def parse_rating(rating_text: Optional[str]) -> Optional[float]:
    """Extract rating from text"""
    if not rating_text:
        return None
    try:
        match = re.search(r'(\d+\.?\d*)', rating_text)
        if match:
            return float(match.group(1))
    except:
        pass
    return None


def parse_reviews_count(reviews_text: Optional[str]) -> Optional[int]:
    """Extract review count from text"""
    if not reviews_text:
        return None
    try:
        # Remove commas and extract number
        number_text = re.sub(r'[^\d]', '', reviews_text)
        if number_text:
            return int(number_text)
    except:
        pass
    return None