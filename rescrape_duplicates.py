"""
Re-scrape specific keyword-location pairs from CSV files
Useful for fixing duplicates or failed tasks
"""
import pandas as pd
from datetime import datetime
import sys
import os

from config.settings import ScraperConfig
from core.orchestrator import ScraperOrchestrator
from models.place import SearchTask

def rescrape_from_csv(csv_files: list, max_results_per_task: int = 50):
    """
    Re-scrape keyword-location pairs from CSV files
    
    Args:
        csv_files: List of CSV file paths
        max_results_per_task: Max results to scrape per task
    """
    print("=" * 80)
    print("RE-SCRAPE FROM CSV FILES")
    print("=" * 80)
    
    all_tasks = []
    
    for csv_file in csv_files:
        print(f"\n📁 Reading: {csv_file}")
        
        try:
            # Try to read CSV
            df = pd.read_csv(csv_file, sep=',')
            
            # Strip whitespace from column names
            df.columns = df.columns.str.strip()
            
            print(f"   Total rows: {len(df)}")
            print(f"   Columns: {', '.join(df.columns.tolist())}")
            
            # Check for new format (subdistrict, district, region)
            if 'search_keyword' in df.columns and 'search_subdistrict' in df.columns and 'search_district' in df.columns and 'search_region' in df.columns:
                
                # Strip whitespace from values
                df['search_keyword'] = df['search_keyword'].str.strip()
                df['search_subdistrict'] = df['search_subdistrict'].str.strip()
                df['search_district'] = df['search_district'].str.strip()
                df['search_region'] = df['search_region'].str.strip()
                
                pairs = df[['search_keyword', 'search_subdistrict', 'search_district', 'search_region']].drop_duplicates()
                
                print(f"   Found {len(pairs)} unique keyword-location pair(s):")
                
                for _, row in pairs.iterrows():
                    keyword = row['search_keyword']
                    subdistrict = row['search_subdistrict']
                    district = row['search_district']
                    region = row['search_region']
                    
                    # Build location string
                    if pd.notna(subdistrict) and subdistrict.strip() != '':
                        location = f"{subdistrict}, {district}, {region}"
                    else:
                        location = f"{district}, {region}"
                    
                    print(f"   ✓ '{keyword}' in '{location}'")
                    
                    task = SearchTask(
                        keyword=keyword,
                        location=location,  # Pass combined location string
                        max_results=max_results_per_task
                    )
                    all_tasks.append(task)
                    
            # Check for old format (search_location)
            elif 'search_keyword' in df.columns and 'search_location' in df.columns:
                
                df['search_keyword'] = df['search_keyword'].str.strip()
                df['search_location'] = df['search_location'].str.strip()
                
                pairs = df[['search_keyword', 'search_location']].drop_duplicates()
                
                print(f"   Found {len(pairs)} unique keyword-location pair(s):")
                
                for _, row in pairs.iterrows():
                    keyword = row['search_keyword']
                    location = row['search_location']
                    
                    print(f"   ✓ '{keyword}' in '{location}'")
                    
                    task = SearchTask(
                        keyword=keyword,
                        location=location,
                        max_results=max_results_per_task
                    )
                    all_tasks.append(task)
                    
            else:
                print(f"   ❌ CSV doesn't have required columns")
                print(f"      Need either: search_keyword + search_subdistrict + search_district + search_region")
                print(f"      OR: search_keyword + search_location")
                
        except Exception as e:
            print(f"   ❌ Error reading CSV: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    if not all_tasks:
        print("\n❌ No tasks to scrape!")
        return
    
    print(f"\n📊 Total tasks to re-scrape: {len(all_tasks)}")
    print(f"⚙️  Max results per task: {max_results_per_task}")
    
    # Ask for confirmation
    response = input("\n🔄 Start re-scraping? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("❌ Cancelled")
        return
    
    # Configure scraper
    config = ScraperConfig(
        headless=True,
        max_workers=1,
        scroll_pause_time=2.0,
        max_scroll_attempts=20,
        min_delay=1.0,
        max_delay=3.0,
    )
    
    print(f"\n🚀 Starting re-scraper...")
    print(f"   Workers: {config.max_workers}")
    print(f"   Headless: {config.headless}")
    print("=" * 80)
    
    # Run scraper
    orchestrator = ScraperOrchestrator(config)
    df_results = orchestrator.scrape_tasks(all_tasks)
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = f"results/rescrape_results_{timestamp}.csv"
    excel_file = f"results/rescrape_results_{timestamp}.xlsx"
    
    df_results.to_csv(csv_file, index=False, sep='|')
    df_results.to_excel(excel_file, index=False, engine='openpyxl')
    
    print("\n" + "=" * 80)
    print("✅ RE-SCRAPING COMPLETE!")
    print("=" * 80)
    print(f"📊 Results:")
    print(f"   Total places: {len(df_results)}")
    print(f"   CSV: {csv_file}")
    print(f"   Excel: {excel_file}")
    print("=" * 80)


if __name__ == "__main__":
    # Configuration
    CSV_FILES = [
        "results/duplicate_jatinegara.csv",
        "results/duplicate_east_jaktim.csv"
    ]
    
    MAX_RESULTS_PER_TASK = 150
    
    # Check if files exist
    existing_files = [f for f in CSV_FILES if os.path.exists(f)]
    missing_files = [f for f in CSV_FILES if not os.path.exists(f)]
    
    if missing_files:
        print("⚠️  Warning: Some files not found:")
        for f in missing_files:
            print(f"   - {f}")
    
    if not existing_files:
        print("\n❌ No CSV files found!")
        print("\nPlace your CSV files in:")
        for f in CSV_FILES:
            print(f"   - {f}")
        sys.exit(1)
    
    print(f"\n✓ Found {len(existing_files)} file(s):")
    for f in existing_files:
        print(f"   - {f}")
    
    # Run re-scraper
    rescrape_from_csv(existing_files, MAX_RESULTS_PER_TASK)