# Google Maps Scraper - Modular & Multi-threaded

A modular, production-ready Google Maps scraper with multi-threading support. Perfect for extracting merchant data across multiple regions.

## ✨ Features

- **Modular Architecture**: Clean separation of concerns (models, core, utils, config)
- **Multi-threaded**: Scrape multiple locations simultaneously for faster results
- **Thread-safe**: Proper locking mechanisms to prevent data corruption
- **Duplicate Detection**: Automatically filters duplicate entries
- **Comprehensive Data**: Extracts 20+ fields including ratings, coordinates, contact info
- **Error Handling**: Robust retry mechanisms and graceful failure handling
- **Progress Tracking**: Real-time progress updates and summaries
- **Multiple Output Formats**: Saves to both CSV and Excel

## 📁 Project Structure

```
gmaps_scraper/
├── config/
│   └── settings.py          # Configuration settings
├── core/
│   ├── driver_manager.py    # Browser automation manager
│   ├── search_engine.py     # Google Maps search and extraction
│   └── orchestrator.py      # Multi-threaded orchestration
├── models/
│   └── place.py             # Data models (Place, SearchTask)
├── utils/
│   ├── extractors.py        # Data extraction utilities
│   └── task_generator.py    # Task generation helpers
├── example_jaksel.py        # Example: Jakarta Selatan districts
├── example_custom.py        # Example: Custom keywords/locations
└── requirements.txt         # Dependencies
```

## 🚀 Installation

### 1. Clone or Download

```bash
# If you have the code in a directory
cd gmaps_scraper
```

### 2. Create Virtual Environment (Recommended)

```bash
# macOS/Linux
python3 -m venv venv
source venv/bin/activate

# Windows
python -m venv venv
venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Install ChromeDriver

The scraper uses Chrome WebDriver. You can either:

**Option A: Automatic (Recommended)**
```bash
pip install webdriver-manager
```

**Option B: Manual**
1. Download ChromeDriver from: https://chromedriver.chromium.org/
2. Place it in your PATH or specify path in config

## 📖 Usage

### Example 1: Scrape Warung Kelontong in Jakarta Selatan

```bash
python example_jaksel.py
```

This will scrape:
- Keywords: warung kelontong, toko kelontong, minimarket
- Locations: All 10 districts in Jakarta Selatan
- Total tasks: 30 (3 keywords × 10 districts)

### Example 2: Custom Keywords and Locations

```bash
python example_custom.py
```

Or create your own script:

```python
from gmaps_scraper.config.settings import ScraperConfig
from gmaps_scraper.core.orchestrator import ScraperOrchestrator
from gmaps_scraper.utils.task_generator import TaskGenerator

# Configure
config = ScraperConfig(
    headless=False,      # Set True to hide browser
    max_workers=4,       # Number of threads
    scroll_pause_time=2.0,
    max_scroll_attempts=10
)

# Define what to search
keywords = ["warung kelontong", "minimarket"]
locations = [
    "Kebayoran Baru, Jakarta Selatan",
    "Cilandak, Jakarta Selatan"
]

# Generate tasks
tasks = TaskGenerator.generate_tasks(
    keywords=keywords,
    locations=locations,
    max_results_per_task=50
)

# Run scraper
orchestrator = ScraperOrchestrator(config)
df = orchestrator.scrape_tasks(tasks)

# Save results
orchestrator.save_results(df, prefix="my_scrape")
```

## ⚙️ Configuration Options

```python
ScraperConfig(
    headless=False,              # Run browser in background
    scroll_pause_time=2.0,       # Pause between scrolls (seconds)
    max_scroll_attempts=10,      # Max scrolls per search
    page_load_timeout=30,        # Page load timeout (seconds)
    element_wait_timeout=10,     # Element wait timeout (seconds)
    max_retries=3,               # Retry attempts for failed searches
    min_delay=1.0,               # Min delay between actions
    max_delay=3.0,               # Max delay between actions
    max_workers=4,               # Number of parallel threads
    output_dir="results",        # Output directory
    language="id",               # Browser language
    proxy=None                   # Proxy server (optional)
)
```

## 📊 Output Data

Each place contains:

| Field | Description |
|-------|-------------|
| name | Business name |
| category | Business category |
| address | Full address |
| district | Kecamatan |
| city | City name |
| province | Province |
| zip_code | Postal code |
| latitude | GPS latitude |
| longitude | GPS longitude |
| rating | Average rating (1-5) |
| reviews_count | Number of reviews |
| phone | Phone number |
| website | Website URL |
| google_maps_link | Google Maps URL |
| opening_hours | Opening hours |
| star_1 to star_5 | Star distribution |
| search_keyword | Keyword used |
| search_location | Location searched |
| scraped_at | Timestamp |

## 🎯 Common Use Cases

### 1. Scrape All Districts in a City

```python
from gmaps_scraper.utils.task_generator import (
    TaskGenerator, 
    JAKARTA_SELATAN_DISTRICTS
)

tasks = TaskGenerator.generate_district_tasks(
    keywords=["apotek", "klinik"],
    city="Jakarta Selatan",
    districts=JAKARTA_SELATAN_DISTRICTS,
    max_results_per_task=30
)
```

### 2. Scrape Specific Areas

```python
tasks = TaskGenerator.generate_tasks(
    keywords=["cafe", "coffee shop"],
    locations=[
        "Kemang, Jakarta Selatan",
        "Senopati, Jakarta Selatan",
        "Blok M, Jakarta Selatan"
    ],
    max_results_per_task=50
)
```

### 3. Load Tasks from CSV

Create a CSV file:
```csv
keyword,location
warung kelontong,Kebayoran Baru
warung kelontong,Cilandak
toko kelontong,Kebayoran Baru
```

Then:
```python
tasks = TaskGenerator.generate_from_csv(
    'my_tasks.csv',
    keyword_column='keyword',
    location_column='location'
)
```

## 🔧 Advanced Features

### Multi-threading Control

```python
# Use more threads for faster scraping (but higher resource usage)
config = ScraperConfig(max_workers=8)

# Use fewer threads for more stable scraping
config = ScraperConfig(max_workers=2)
```

### Headless Mode

```python
# For running on servers without display
config = ScraperConfig(headless=True)
```

### Rate Limiting

```python
# Add delays to avoid detection
config = ScraperConfig(
    min_delay=2.0,  # Wait at least 2 seconds
    max_delay=5.0   # Wait at most 5 seconds
)
```

## 📝 Predefined District Lists

The package includes predefined district lists:
- `JAKARTA_SELATAN_DISTRICTS` (10 districts)
- `JAKARTA_PUSAT_DISTRICTS` (8 districts)
- `JAKARTA_UTARA_DISTRICTS` (6 districts)
- `JAKARTA_TIMUR_DISTRICTS` (10 districts)
- `JAKARTA_BARAT_DISTRICTS` (8 districts)

## ⚠️ Important Notes

1. **Rate Limiting**: Don't set `max_workers` too high to avoid being blocked
2. **Headless Mode**: More stable for long-running scrapes
3. **Memory**: Each thread uses ~500MB RAM. Monitor your resources
4. **Legal**: Respect Google's Terms of Service and robots.txt
5. **Data Accuracy**: Always verify critical data from original sources

## 🐛 Troubleshooting

### "ChromeDriver not found"
```bash
pip install webdriver-manager
```

### "Connection refused" or "Timeout"
- Reduce `max_workers`
- Increase `page_load_timeout`
- Check internet connection

### Getting duplicate results
- The scraper has built-in deduplication
- Duplicates within same run are automatically removed
- Compare results from different runs manually

### Browser not closing
```python
# Use context manager for automatic cleanup
with DriverManager(config) as dm:
    # Your code here
    pass
# Browser automatically closes
```

## 📈 Performance Tips

1. **Start Small**: Test with 1-2 tasks first
2. **Optimize Threads**: 4-6 threads is usually optimal
3. **Use Headless**: Faster and uses less memory
4. **Filter Results**: Process data after scraping to remove unwanted entries
5. **Batch Processing**: Split large jobs into smaller batches

## 📄 License

This is a tool for educational purposes. Always respect:
- Google Maps Terms of Service
- Website robots.txt files
- Local data protection laws
- Rate limiting best practices

## 🤝 Contributing

Feel free to:
- Report bugs
- Suggest features
- Submit pull requests
- Share improvements

## 📮 Support

For issues or questions:
1. Check the examples
2. Review configuration options
3. Read error messages carefully
4. Test with reduced `max_workers`

---

**Happy Scraping! 🚀**# gmaps-scraper
