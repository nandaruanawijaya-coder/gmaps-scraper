"""
Scraper orchestrator with multi-threading support
"""
import os
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import List, Optional
from datetime import datetime

from ..models.place import SearchTask, Place
from ..config.settings import ScraperConfig
from ..core.driver_manager import DriverManager
from ..core.search_engine import MapsSearchEngine


class ScraperOrchestrator:
    """Orchestrates multi-threaded scraping operations"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.results: List[Place] = []
        self.lock = Lock()
        self.seen_links = set()
        
        # Create output directories
        os.makedirs(self.config.output_dir, exist_ok=True)
        os.makedirs(self.config.checkpoint_dir, exist_ok=True)
    
    def scrape_tasks(self, tasks: List[SearchTask]) -> pd.DataFrame:
        """
        Execute multiple search tasks with multi-threading
        
        Args:
            tasks: List of SearchTask objects to execute
            
        Returns:
            DataFrame with all collected places
        """
        print(f"\n{'='*70}")
        print(f"Starting scraper with {len(tasks)} tasks")
        print(f"Using {self.config.max_workers} threads")
        print(f"{'='*70}\n")
        
        start_time = time.time()
        
        # Execute tasks in parallel
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self._execute_task, task): task 
                for task in tasks
            }
            
            # Process completed tasks
            completed = 0
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                completed += 1
                
                try:
                    places = future.result()
                    
                    # Thread-safe addition of results
                    with self.lock:
                        for place in places:
                            if place.google_maps_link not in self.seen_links:
                                self.seen_links.add(place.google_maps_link)
                                self.results.append(place)
                    
                    print(f"\n[{completed}/{len(tasks)}] Completed: {task}")
                    print(f"  Found {len(places)} new places")
                    print(f"  Total unique places so far: {len(self.results)}")
                    
                except Exception as e:
                    print(f"\n[{completed}/{len(tasks)}] Failed: {task}")
                    print(f"  Error: {e}")
        
        elapsed = time.time() - start_time
        print(f"\n{'='*70}")
        print(f"Scraping completed in {elapsed:.2f} seconds")
        print(f"Total unique places collected: {len(self.results)}")
        print(f"{'='*70}\n")
        
        # Convert to DataFrame
        return self._create_dataframe()
    
    def _execute_task(self, task: SearchTask) -> List[Place]:
        """Execute a single search task (runs in separate thread)"""
        # Each thread gets its own driver
        with DriverManager(self.config) as driver_manager:
            search_engine = MapsSearchEngine(driver_manager, self.config)
            places = search_engine.search(task)
            return places
    
    def _create_dataframe(self) -> pd.DataFrame:
        """Convert results to pandas DataFrame"""
        if not self.results:
            return pd.DataFrame()
        
        data = [place.to_dict() for place in self.results]
        df = pd.DataFrame(data)
        
        # Reorder columns for better readability
        column_order = [
            'name', 'category', 'address', 'district', 'city', 'province', 'zip_code',
            'latitude', 'longitude', 'rating', 'reviews_count',
            'phone', 'website', 'google_maps_link', 'opening_hours',
            'star_1', 'star_2', 'star_3', 'star_4', 'star_5',
            'search_keyword', 'search_location', 'scraped_at'
        ]
        
        # Only include columns that exist
        column_order = [col for col in column_order if col in df.columns]
        df = df[column_order]
        
        return df
    
    def save_results(self, df: pd.DataFrame, prefix: str = "gmaps") -> str:
        """
        Save results to CSV and Excel files
        
        Args:
            df: DataFrame to save
            prefix: Prefix for filename
            
        Returns:
            Path to saved CSV file
        """
        if df.empty:
            print("No results to save")
            return ""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save CSV with custom delimiter
        csv_filename = os.path.join(
            self.config.output_dir,
            f"{prefix}_{timestamp}.csv"
        )
        df.to_csv(
            csv_filename, 
            index=False, 
            encoding='utf-8', 
            sep=self.config.csv_delimiter
        )
        print(f"✓ Results saved to: {csv_filename}")
        print(f"  (Using delimiter: '{self.config.csv_delimiter}' for easier reading)")
        
        # Save Excel
        try:
            excel_filename = os.path.join(
                self.config.output_dir,
                f"{prefix}_{timestamp}.xlsx"
            )
            df.to_excel(excel_filename, index=False, engine='openpyxl')
            print(f"✓ Results saved to: {excel_filename}")
        except Exception as e:
            print(f"Could not save Excel file: {e}")
        
        # Print summary
        self._print_summary(df)
        
        return csv_filename
    
    def _print_summary(self, df: pd.DataFrame):
        """Print summary statistics"""
        print(f"\n{'='*70}")
        print("SCRAPING SUMMARY")
        print(f"{'='*70}")
        print(f"Total places collected: {len(df)}")
        
        if 'category' in df.columns:
            print(f"\nTop 10 Categories:")
            category_counts = df['category'].value_counts().head(10)
            for category, count in category_counts.items():
                if category:
                    print(f"  - {category}: {count}")
        
        if 'city' in df.columns:
            print(f"\nPlaces by City:")
            city_counts = df['city'].value_counts()
            for city, count in city_counts.items():
                if city:
                    print(f"  - {city}: {count}")
        
        if 'district' in df.columns:
            print(f"\nPlaces by District:")
            district_counts = df['district'].value_counts().head(10)
            for district, count in district_counts.items():
                if district:
                    print(f"  - {district}: {count}")
        
        if 'rating' in df.columns:
            ratings = df['rating'].dropna()
            if len(ratings) > 0:
                print(f"\nRating Statistics:")
                print(f"  - Average rating: {ratings.mean():.2f}")
                print(f"  - Places with ratings: {len(ratings)}/{len(df)}")
        
        if 'latitude' in df.columns and 'longitude' in df.columns:
            coords = df[(df['latitude'].notna()) & (df['longitude'].notna())]
            print(f"\nCoordinate Extraction:")
            print(f"  - Success rate: {len(coords)}/{len(df)} ({len(coords)/len(df)*100:.1f}%)")
        
        print(f"{'='*70}\n")