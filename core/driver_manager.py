"""
WebDriver manager for Chrome browser automation
"""
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import os
from typing import Optional

from ..config.settings import ScraperConfig


class DriverManager:
    """Manages Chrome WebDriver lifecycle"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.driver: Optional[webdriver.Chrome] = None
    
    def create_driver(self) -> webdriver.Chrome:
        """Create and configure Chrome WebDriver"""
        chrome_options = Options()
        
        # Basic options
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1280,800")
        chrome_options.add_argument(f"--lang={self.config.language}")
        chrome_options.add_argument(f"--user-agent={self.config.user_agent}")
        
        # Disable unnecessary features
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        # Performance optimizations
        chrome_options.add_argument("--disable-application-cache")
        chrome_options.add_argument("--disk-cache-size=0")
        
        if self.config.headless:
            chrome_options.add_argument("--headless=new")
        
        if self.config.proxy:
            chrome_options.add_argument(f'--proxy-server={self.config.proxy}')
        
        # Temporary profile directory
        try:
            temp_dir = os.path.join(os.getcwd(), "temp_browser_data")
            os.makedirs(temp_dir, exist_ok=True)
            chrome_options.add_argument(f"--user-data-dir={temp_dir}")
        except Exception as e:
            print(f"Warning: Could not create custom cache directory: {e}")
        
        # Create driver
        if self.config.driver_path:
            service = Service(self.config.driver_path)
            driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            driver = webdriver.Chrome(options=chrome_options)
        
        driver.set_page_load_timeout(self.config.page_load_timeout)
        driver.delete_all_cookies()
        
        self.driver = driver
        return driver
    
    def reset_to_maps_home(self) -> bool:
        """Reset browser to Google Maps homepage"""
        try:
            if not self.driver:
                return False
            
            self.driver.get('https://maps.google.com')
            time.sleep(2)
            
            # Clear search box
            try:
                search_box = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((
                        By.CSS_SELECTOR, 
                        'input[aria-label="Search Google Maps"], input[id="searchboxinput"]'
                    ))
                )
                search_box.clear()
                time.sleep(0.5)
            except TimeoutException:
                pass
            
            # Close any open panels
            try:
                close_buttons = self.driver.find_elements(
                    By.CSS_SELECTOR, 
                    'button[aria-label="Close"], button[aria-label="Back"]'
                )
                for button in close_buttons:
                    try:
                        self.driver.execute_script("arguments[0].click();", button)
                        time.sleep(0.3)
                    except:
                        pass
            except:
                pass
            
            # Press Escape to close dialogs
            try:
                from selenium.webdriver.common.action_chains import ActionChains
                ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                time.sleep(0.5)
            except:
                pass
            
            return True
            
        except Exception as e:
            print(f"Error resetting to maps home: {e}")
            return False
    
    def quit(self):
        """Close the driver"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
    
    def __enter__(self):
        """Context manager entry"""
        self.create_driver()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.quit()