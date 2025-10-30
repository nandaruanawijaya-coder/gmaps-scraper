"""
Google Maps Scraper - Streamlit Web Interface
==============================================

A web interface for the Google Maps scraper that allows users to:
1. Upload keywords file (CSV)
2. Upload locations file (CSV)
3. Configure scraping parameters
4. Run scraper and download results
"""

import streamlit as st
import pandas as pd
import os
import sys
import time
from datetime import datetime
from io import BytesIO
import tempfile
import shutil

# Add the parent directory to the path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import scraper components
try:
    from config.settings import ScraperConfig
    from core.orchestrator import ScraperOrchestrator
    from utils.task_generator import TaskGenerator
except ImportError:
    # Alternative import for different structures
    from config.settings import ScraperConfig
    from core.orchestrator import ScraperOrchestrator
    from utils.task_generator import TaskGenerator

# Page configuration
st.set_page_config(
    page_title="Google Maps Scraper",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #ff7f0e;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
    .success-box {
        padding: 1rem;
        background-color: #d4edda;
        border-left: 4px solid #28a745;
        border-radius: 4px;
        margin: 1rem 0;
    }
    .info-box {
        padding: 1rem;
        background-color: #d1ecf1;
        border-left: 4px solid #17a2b8;
        border-radius: 4px;
        margin: 1rem 0;
    }
    .warning-box {
        padding: 1rem;
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
        border-radius: 4px;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'scraping_complete' not in st.session_state:
    st.session_state.scraping_complete = False
if 'results_df' not in st.session_state:
    st.session_state.results_df = None
if 'csv_path' not in st.session_state:
    st.session_state.csv_path = None
if 'excel_path' not in st.session_state:
    st.session_state.excel_path = None


def validate_keywords_file(df):
    """Validate keywords CSV file format"""
    if 'keyword' not in df.columns:
        return False, "CSV must have a 'keyword' column"
    if len(df) == 0:
        return False, "CSV file is empty"
    return True, "Valid"


def validate_locations_file(df):
    """Validate locations CSV file format"""
    required_columns = ['district', 'city']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return False, f"CSV must have columns: {', '.join(required_columns)}. Missing: {', '.join(missing_columns)}"
    if len(df) == 0:
        return False, "CSV file is empty"
    
    # Check if subdistrict column exists (optional)
    has_subdistrict = 'subdistrict' in df.columns
    if has_subdistrict:
        info_msg = "Valid (with subdistrict column)"
    else:
        info_msg = "Valid (without subdistrict - will use district only)"
    
    return True, info_msg


def create_sample_keywords_csv():
    """Create sample keywords CSV for download"""
    sample_data = pd.DataFrame({
        'keyword': [
            'warung kelontong',
            'toko kelontong',
            'minimarket',
            'cafe',
            'restoran',
            'apotek'
        ]
    })
    return sample_data.to_csv(index=False).encode('utf-8')


def create_sample_locations_csv():
    """Create sample locations CSV for download"""
    sample_data = pd.DataFrame({
        'district': [
            'Kebayoran Baru',
            'Kebayoran Baru',
            'Kebayoran Baru',
            'Cilandak',
            'Cilandak'
        ],
        'subdistrict': [
            'Gunung',
            'Melawai',
            'Kramat Pela',
            'Cilandak Barat',
            'Cipete Selatan'
        ],
        'city': [
            'Jakarta Selatan',
            'Jakarta Selatan',
            'Jakarta Selatan',
            'Jakarta Selatan',
            'Jakarta Selatan'
        ]
    })
    return sample_data.to_csv(index=False).encode('utf-8')


def run_scraper(keywords_df, locations_df, config):
    """Run the scraper with given parameters"""
    # Generate tasks using the new dataframe method
    tasks = TaskGenerator.generate_from_dataframe(
        keywords_df=keywords_df,
        locations_df=locations_df,
        max_results_per_task=config['max_results_per_task']
    )
    
    # Configure scraper
    scraper_config = ScraperConfig(
        headless=True,  # Always headless in web app
        max_workers=config['max_workers'],
        scroll_pause_time=config['scroll_pause_time'],
        max_scroll_attempts=config['max_scroll_attempts'],
        min_delay=config['min_delay'],
        max_delay=config['max_delay'],
        csv_delimiter="|"
    )
    
    # Run scraper
    orchestrator = ScraperOrchestrator(scraper_config)
    df = orchestrator.scrape_tasks(tasks)
    
    return df, orchestrator


# Main App
def main():
    # Header
    st.markdown('<div class="main-header">🗺️ Google Maps Scraper</div>', unsafe_allow_html=True)
    st.markdown("Extract merchant data from Google Maps by uploading keywords and locations")
    
    # Sidebar - Configuration
    with st.sidebar:
        st.markdown("### ⚙️ Configuration")
        
        st.markdown("#### Scraping Parameters")
        max_workers = st.slider(
            "Number of Threads",
            min_value=1,
            max_value=10,
            value=4,
            help="More threads = faster, but uses more resources"
        )
        
        max_results_per_task = st.number_input(
            "Max Results per Task",
            min_value=5,
            max_value=500,
            value=30,
            help="Maximum places to collect per keyword-location combination"
        )
        
        with st.expander("Advanced Settings"):
            scroll_pause_time = st.slider(
                "Scroll Pause Time (seconds)",
                min_value=0.5,
                max_value=5.0,
                value=2.0,
                step=0.5
            )
            
            max_scroll_attempts = st.slider(
                "Max Scroll Attempts",
                min_value=3,
                max_value=20,
                value=10
            )
            
            min_delay = st.slider(
                "Min Delay (seconds)",
                min_value=0.5,
                max_value=5.0,
                value=1.0,
                step=0.5
            )
            
            max_delay = st.slider(
                "Max Delay (seconds)",
                min_value=1.0,
                max_value=10.0,
                value=3.0,
                step=0.5
            )
        
        st.markdown("---")
        st.markdown("#### 📖 Documentation")
        st.markdown("""
        **CSV Format Requirements:**
        
        **Keywords File:**
        - Column: `keyword`
        - One keyword per row
        
        **Locations File:**
        - Required: `district`, `city`
        - Optional: `subdistrict` (for more specific searches)
        - One location per row
        
        [Download sample files below] ↓
        """)
    
    # Main content
    col1, col2 = st.columns(2)
    
    # Column 1 - Keywords Upload
    with col1:
        st.markdown('<div class="sub-header">📝 Keywords</div>', unsafe_allow_html=True)
        
        # Sample download
        st.download_button(
            label="📥 Download Sample Keywords CSV",
            data=create_sample_keywords_csv(),
            file_name="sample_keywords.csv",
            mime="text/csv"
        )
        
        # Upload keywords
        keywords_file = st.file_uploader(
            "Upload Keywords CSV",
            type=['csv'],
            key="keywords_uploader",
            help="CSV file with 'keyword' column"
        )
        
        if keywords_file:
            try:
                keywords_df = pd.read_csv(keywords_file)
                is_valid, message = validate_keywords_file(keywords_df)
                
                if is_valid:
                    st.success(f"✅ Valid! {len(keywords_df)} keywords loaded")
                    with st.expander("Preview Keywords"):
                        st.dataframe(keywords_df, use_container_width=True)
                else:
                    st.error(f"❌ {message}")
                    keywords_df = None
            except Exception as e:
                st.error(f"❌ Error reading file: {e}")
                keywords_df = None
        else:
            keywords_df = None
            st.info("👆 Upload a keywords CSV file to begin")
    
    # Column 2 - Locations Upload
    with col2:
        st.markdown('<div class="sub-header">📍 Locations</div>', unsafe_allow_html=True)
        
        # Sample download
        st.download_button(
            label="📥 Download Sample Locations CSV",
            data=create_sample_locations_csv(),
            file_name="sample_locations.csv",
            mime="text/csv"
        )
        
        # Upload locations
        locations_file = st.file_uploader(
            "Upload Locations CSV",
            type=['csv'],
            key="locations_uploader",
            help="CSV file with 'district' and 'city' columns"
        )
        
        if locations_file:
            try:
                locations_df = pd.read_csv(locations_file)
                is_valid, message = validate_locations_file(locations_df)
                
                if is_valid:
                    st.success(f"✅ Valid! {len(locations_df)} locations loaded")
                    with st.expander("Preview Locations"):
                        st.dataframe(locations_df, use_container_width=True)
                else:
                    st.error(f"❌ {message}")
                    locations_df = None
            except Exception as e:
                st.error(f"❌ Error reading file: {e}")
                locations_df = None
        else:
            locations_df = None
            st.info("👆 Upload a locations CSV file to begin")
    
    # Task Summary
    if keywords_df is not None and locations_df is not None:
        st.markdown("---")
        total_tasks = len(keywords_df) * len(locations_df)
        estimated_time = total_tasks * 10 / max_workers  # Rough estimate: 10 seconds per task
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Keywords", len(keywords_df))
        col2.metric("Locations", len(locations_df))
        col3.metric("Total Tasks", total_tasks)
        col4.metric("Est. Time (min)", f"{estimated_time/60:.1f}")
        
        st.markdown('<div class="info-box">', unsafe_allow_html=True)
        st.markdown(f"""
        **📊 Scraping Summary:**
        - {len(keywords_df)} keywords × {len(locations_df)} locations = **{total_tasks} search tasks**
        - Using **{max_workers} threads**
        - Maximum **{max_results_per_task}** places per task
        - Estimated total: **{total_tasks * max_results_per_task:,}** places (before deduplication)
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Start Scraping Button
        st.markdown("---")
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            start_button = st.button(
                "🚀 Start Scraping",
                use_container_width=True,
                type="primary"
            )
        
        if start_button:
            # Reset state
            st.session_state.scraping_complete = False
            st.session_state.results_df = None
            
            # Show progress
            progress_container = st.container()
            
            with progress_container:
                st.markdown("### 🔄 Scraping in Progress...")
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Configuration
                config = {
                    'max_workers': max_workers,
                    'max_results_per_task': max_results_per_task,
                    'scroll_pause_time': scroll_pause_time,
                    'max_scroll_attempts': max_scroll_attempts,
                    'min_delay': min_delay,
                    'max_delay': max_delay
                }
                
                try:
                    # Run scraper
                    start_time = time.time()
                    status_text.text("Initializing scraper...")
                    progress_bar.progress(10)
                    
                    status_text.text("Starting scraping tasks...")
                    progress_bar.progress(20)
                    
                    df, orchestrator = run_scraper(keywords_df, locations_df, config)
                    
                    progress_bar.progress(90)
                    status_text.text("Saving results...")
                    
                    # Save results
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    os.makedirs("results", exist_ok=True)
                    
                    csv_filename = f"results/scrape_results_{timestamp}.csv"
                    excel_filename = f"results/scrape_results_{timestamp}.xlsx"
                    
                    df.to_csv(csv_filename, index=False, sep='|')
                    df.to_excel(excel_filename, index=False, engine='openpyxl')
                    
                    progress_bar.progress(100)
                    elapsed_time = time.time() - start_time
                    
                    # Store in session state
                    st.session_state.scraping_complete = True
                    st.session_state.results_df = df
                    st.session_state.csv_path = csv_filename
                    st.session_state.excel_path = excel_filename
                    
                    status_text.text(f"✅ Complete! ({elapsed_time:.1f} seconds)")
                    
                    # Success message
                    st.success(f"""
                    🎉 **Scraping Complete!**
                    - Collected: **{len(df):,}** unique places
                    - Time taken: **{elapsed_time/60:.1f}** minutes
                    - Results saved successfully
                    """)
                    
                except Exception as e:
                    st.error(f"❌ Error during scraping: {e}")
                    import traceback
                    with st.expander("Error Details"):
                        st.code(traceback.format_exc())
    
    # Results Section
    if st.session_state.scraping_complete and st.session_state.results_df is not None:
        st.markdown("---")
        st.markdown('<div class="sub-header">📊 Results</div>', unsafe_allow_html=True)
        
        df = st.session_state.results_df
        
        # Summary statistics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Places", len(df))
        col2.metric("Avg Rating", f"{df['rating'].mean():.2f}" if 'rating' in df.columns else "N/A")
        col3.metric("With Phone", df['phone'].notna().sum() if 'phone' in df.columns else "N/A")
        col4.metric("With Website", df['website'].notna().sum() if 'website' in df.columns else "N/A")
        
        # Data preview
        st.markdown("#### Data Preview")
        st.dataframe(df.head(50), use_container_width=True)
        
        # Download buttons
        st.markdown("#### Download Results")
        col1, col2 = st.columns(2)
        
        with col1:
            # CSV download
            csv_data = df.to_csv(index=False, sep='|').encode('utf-8')
            st.download_button(
                label="📥 Download CSV",
                data=csv_data,
                file_name=f"gmaps_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col2:
            # Excel download
            buffer = BytesIO()
            df.to_excel(buffer, index=False, engine='openpyxl')
            excel_data = buffer.getvalue()
            
            st.download_button(
                label="📥 Download Excel",
                data=excel_data,
                file_name=f"gmaps_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        
        # Detailed statistics
        with st.expander("📈 Detailed Statistics"):
            col1, col2 = st.columns(2)
            
            with col1:
                if 'category' in df.columns:
                    st.markdown("**Top 10 Categories:**")
                    category_counts = df['category'].value_counts().head(10)
                    st.bar_chart(category_counts)
            
            with col2:
                if 'city' in df.columns:
                    st.markdown("**Places by City:**")
                    city_counts = df['city'].value_counts()
                    st.bar_chart(city_counts)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 2rem;'>
        <p>Google Maps Scraper | Built with Streamlit</p>
        <p><small>⚠️ Use responsibly and respect Google's Terms of Service</small></p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
