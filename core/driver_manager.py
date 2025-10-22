"""
WebDriver manager for Chrome browser automation
"""
import time
import os
import tempfile
import uuid
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from typing import Optional

from config.settings import ScraperConfig


class DriverManager:
    """Manages Chrome WebDriver instances for web scraping"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.driver = None
    
    def create_driver(self) -> webdriver.Chrome:
        """Create and configure Chrome WebDriver with Streamlit Cloud support"""
        chrome_options = Options()
        
        # Detect if running on Streamlit Cloud or similar environment
        is_streamlit_cloud = os.getenv('STREAMLIT_RUNTIME_ENV') == 'cloud' or \
                            os.path.exists('/mount/src')
        
        if self.config.headless or is_streamlit_cloud:
            chrome_options.add_argument('--headless=new')
        
        # Essential arguments for all environments
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--window-size=1920,1080')
        
        # Add unique user data directory for each thread (prevents conflicts)
        temp_dir = tempfile.gettempdir()
        unique_dir = f"{temp_dir}/chrome_profile_{uuid.uuid4()}"
        chrome_options.add_argument(f'--user-data-dir={unique_dir}')
        
        # Streamlit Cloud specific configuration
        if is_streamlit_cloud:
            chrome_options.add_argument('--disable-software-rasterizer')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-setuid-sandbox')
            chrome_options.binary_location = '/usr/bin/chromium'
        
        # User agent to avoid detection
        chrome_options.add_argument(
            'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        )
        
        # Additional preferences
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_options.add_experimental_option('prefs', {
            'profile.default_content_setting_values.notifications': 2,
            'profile.default_content_settings.popups': 0,
        })
        
        try:
            # Create service
            if is_streamlit_cloud:
                service = Service('/usr/bin/chromedriver')
            else:
                service = Service()
            
            # Create driver
            self.driver = webdriver.Chrome(
                service=service,
                options=chrome_options
            )
            
            # Set timeouts
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(10)
            
            return self.driver
            
        except Exception as e:
            print(f"Error creating Chrome driver: {e}")
            raise
    
    def quit(self):
        """Close the browser and clean up"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                print(f"Error closing driver: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        self.create_driver()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.quit()
