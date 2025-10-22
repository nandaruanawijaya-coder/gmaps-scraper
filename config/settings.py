"""
Configuration settings for the scraper
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class ScraperConfig:
    """Configuration for the scraper"""
    headless: bool = False
    scroll_pause_time: float = 2.0
    max_scroll_attempts: int = 10
    page_load_timeout: int = 30
    element_wait_timeout: int = 10
    max_retries: int = 3
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    language: str = "id"  # Indonesian
    proxy: Optional[str] = None
    driver_path: Optional[str] = None
    
    # Rate limiting
    min_delay: float = 1.0
    max_delay: float = 3.0
    
    # Threading
    max_workers: int = 4
    
    # Output
    output_dir: str = "results"
    checkpoint_dir: str = "checkpoints"
    csv_delimiter: str = "~"  # Custom delimiter for easier reading (avoids comma conflicts)