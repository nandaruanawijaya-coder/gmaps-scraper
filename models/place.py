"""
Data models for Google Maps places
"""
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict
from datetime import datetime


@dataclass
class Place:
    """Represents a place/merchant from Google Maps"""
    name: str
    category: Optional[str] = None
    address: Optional[str] = None
    district: Optional[str] = None  # Kecamatan
    city: Optional[str] = None
    province: Optional[str] = None
    zip_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    rating: Optional[float] = None
    reviews_count: Optional[int] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    google_maps_link: Optional[str] = None
    opening_hours: Optional[str] = None
    star_1: Optional[int] = None
    star_2: Optional[int] = None
    star_3: Optional[int] = None
    star_4: Optional[int] = None
    star_5: Optional[int] = None
    search_keyword: Optional[str] = None
    search_location: Optional[str] = None
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)
    
    def __hash__(self):
        """Hash based on name and coordinates for deduplication"""
        coord_str = f"{self.latitude},{self.longitude}" if self.latitude and self.longitude else ""
        return hash((self.name, coord_str))


@dataclass
class SearchTask:
    """Represents a search task to be processed"""
    keyword: str
    location: str
    max_results: int = 50
    
    def __str__(self):
        return f"{self.keyword} in {self.location}"
    
    def get_query(self) -> str:
        """Get the search query string"""
        return f"{self.keyword} {self.location}"