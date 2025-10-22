"""
Parallel Staggered Runner untuk Multiple City Scraping
Menjalankan scraper secara parallel tapi dengan delay startup (staggered start)
Fixed untuk Windows unicode issues dan path problems
"""


import multiprocessing as mp
import subprocess
import time
import os
import sys
import threading
from datetime import datetime, timedelta
import logging
from pathlib import Path


class ParallelStaggeredScraper:
    def __init__(self, script_path="scrape_gmaps_fix.py", startup_delay=300):
        """
        Initialize parallel staggered scraper
       
        Args:
            script_path: Path to the scraper script
            startup_delay: Delay in seconds between starting each city (default: 5 minutes)
        """
        self.script_path = Path(script_path).resolve()  # Get absolute path
        self.startup_delay = startup_delay
        self.results = {}
        self.running_processes = {}
       
        # Setup logging WITHOUT emoji for Windows compatibility
        log_file = f'parallel_staggered_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        self.setup_logging(log_file)
       
    def setup_logging(self, log_file):
        """Setup logging with Windows-compatible encoding"""
        # Create logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
       
        # Clear any existing handlers
        self.logger.handlers.clear()
       
        # Create formatters
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(asctime)s - %(message)s')
       
        # File handler with UTF-8 encoding
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.INFO)
       
        # Console handler with Windows-safe encoding
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)
       
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
       
        # Prevent propagation to root logger
        self.logger.propagate = False
   
    def run_single_city_async(self, city_config, start_delay=0):
        """Run scraper for single city with optional start delay"""
       
        city_name = city_config['name']
       
        # Wait for start delay
        if start_delay > 0:
            self.logger.info(f"[{city_name}] Waiting {start_delay} seconds before starting...")
            time.sleep(start_delay)
       
        process_id = os.getpid()
        thread_id = threading.get_ident()
       
        try:
            self.logger.info(f"[{city_name}] Starting scraper (PID: {process_id}, Thread: {thread_id})")
            self.logger.info(f"[{city_name}] CSV: {city_config['csv_path']}")
            self.logger.info(f"[{city_name}] Max results: {city_config['max_results']}")
           
            # Check if files exist
            if not self.script_path.exists():
                raise FileNotFoundError(f"Script not found: {self.script_path}")
               
            csv_path = Path(city_config['csv_path']).resolve()
            if not csv_path.exists():
                raise FileNotFoundError(f"CSV file not found: {csv_path}")
           
            # Prepare command with absolute paths
            cmd = [
                sys.executable, str(self.script_path),
                "--area", city_name,
                "--cities-csv", str(csv_path),
                "--max", str(city_config['max_results']),
                "--headless"
            ]
           
            self.logger.info(f"[{city_name}] Command: {' '.join(cmd)}")
           
            # Create working directory for this city
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            work_dir = Path(f"scraping_{city_name.lower()}_{timestamp}").resolve()
            work_dir.mkdir(exist_ok=True)
            self.logger.info(f"[{city_name}] Working directory: {work_dir}")
           
            # Set environment to avoid conflicts
            env = os.environ.copy()
            env['PYTHONUNBUFFERED'] = '1'
            env['CITY_NAME'] = city_name
           
            start_time = datetime.now()
            self.logger.info(f"[{city_name}] Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
           
            # Run the subprocess from the working directory
            process = subprocess.Popen(
                cmd,
                cwd=str(work_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env
            )
           
            # Store process for monitoring
            self.running_processes[city_name] = {
                'process': process,
                'start_time': start_time,
                'work_dir': work_dir
            }
           
            # Monitor process with timeout
            timeout_seconds = None  
            try:
                stdout, stderr = process.communicate(timeout=timeout_seconds)
                end_time = datetime.now()
                duration = end_time - start_time
               
                # Remove from running processes
                if city_name in self.running_processes:
                    del self.running_processes[city_name]
               
                result = {
                    'city': city_name,
                    'csv_path': str(csv_path),
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration': duration,
                    'return_code': process.returncode,
                    'work_dir': str(work_dir),
                    'process_id': process_id,
                    'thread_id': thread_id
                }
               
                if process.returncode == 0:
                    self.logger.info(f"[{city_name}] COMPLETED successfully!")
                    self.logger.info(f"[{city_name}] Duration: {duration}")
                    result['status'] = 'success'
                   
                    # Count result files
                    result_files = list(work_dir.glob("**/*.csv")) + list(work_dir.glob("**/*.xlsx"))
                    result['result_files'] = [str(f) for f in result_files]
                    self.logger.info(f"[{city_name}] Found {len(result_files)} result files")
                   
                else:
                    self.logger.error(f"[{city_name}] FAILED with return code {process.returncode}")
                    result['status'] = 'failed'
                    if stderr:
                        error_preview = stderr[-500:] if len(stderr) > 500 else stderr
                        self.logger.error(f"[{city_name}] Error: {error_preview}")
                        result['error'] = stderr[-1000:]  # Store more error details
               
                # Save logs
                log_file = work_dir / f'{city_name.lower()}_stdout.log'
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.write(stdout or "No stdout output")
                   
                error_file = work_dir / f'{city_name.lower()}_stderr.log'
                with open(error_file, 'w', encoding='utf-8') as f:
                    f.write(stderr or "No stderr output")
               
                self.results[city_name] = result
                return result
               
            except subprocess.TimeoutExpired:
                self.logger.error(f"[{city_name}] TIMEOUT after {timeout_seconds/3600:.1f} hours")
                process.kill()
               
                # Remove from running processes
                if city_name in self.running_processes:
                    del self.running_processes[city_name]
               
                result = {
                    'city': city_name,
                    'start_time': start_time,
                    'end_time': datetime.now(),
                    'status': 'timeout',
                    'error': f'Process timed out after {timeout_seconds/3600:.1f} hours',
                    'work_dir': str(work_dir)
                }
                self.results[city_name] = result
                return result
               
        except Exception as e:
            self.logger.error(f"[{city_name}] EXCEPTION: {e}")
           
            # Remove from running processes
            if city_name in self.running_processes:
                del self.running_processes[city_name]
           
            result = {
                'city': city_name,
                'start_time': datetime.now(),
                'end_time': datetime.now(),
                'status': 'exception',
                'error': str(e),
                'work_dir': 'N/A'
            }
            self.results[city_name] = result
            return result
   
    def monitor_progress(self):
        """Monitor progress of running processes"""
        while self.running_processes:
            self.logger.info(f"MONITORING: {len(self.running_processes)} cities still running")
           
            for city_name, proc_info in list(self.running_processes.items()):
                elapsed = datetime.now() - proc_info['start_time']
                self.logger.info(f"  - {city_name}: running for {elapsed} (PID: {proc_info['process'].pid})")
           
            time.sleep(60)  # Check every minute
       
        self.logger.info("MONITORING: All cities completed")
   
    def run_parallel_staggered(self, cities_config):
        """
        Run multiple cities in parallel with staggered start
       
        Args:
            cities_config: List of city configurations
        """
        self.logger.info("="*60)
        self.logger.info("PARALLEL STAGGERED SCRAPER STARTING")
        self.logger.info("="*60)
        self.logger.info(f"Cities to process: {len(cities_config)}")
        self.logger.info(f"Startup delay between cities: {self.startup_delay} seconds")
       
        total_start_time = datetime.now()
       
        # Start monitoring thread
        monitor_thread = threading.Thread(target=self.monitor_progress, daemon=True)
        monitor_thread.start()
       
        # Create threads for each city with staggered start
        threads = []
        for i, city_config in enumerate(cities_config):
            city_name = city_config['name']
            start_delay = i * self.startup_delay  # Staggered start
           
            self.logger.info(f"SCHEDULING: {city_name} to start in {start_delay} seconds")
           
            thread = threading.Thread(
                target=self.run_single_city_async,
                args=(city_config, start_delay),
                name=f"City-{city_name}"
            )
            threads.append((city_name, thread))
       
        # Start all threads
        for city_name, thread in threads:
            thread.start()
            self.logger.info(f"THREAD STARTED: {city_name}")
       
        self.logger.info("All threads started. Waiting for completion...")
       
        # Wait for all threads to complete
        for city_name, thread in threads:
            try:
                thread.join()  # Wait indefinitely for thread to complete
                self.logger.info(f"THREAD COMPLETED: {city_name}")
            except Exception as e:
                self.logger.error(f"THREAD ERROR: {city_name} - {e}")
       
        total_end_time = datetime.now()
        total_duration = total_end_time - total_start_time
       
        # Generate final report
        self.generate_final_report(cities_config, total_duration)
       
        return list(self.results.values())
   
    def generate_final_report(self, cities_config, total_duration):
        """Generate comprehensive final report"""
       
        self.logger.info("")
        self.logger.info("="*60)
        self.logger.info("PARALLEL STAGGERED SCRAPING FINAL REPORT")
        self.logger.info("="*60)
       
        successful = [r for r in self.results.values() if r.get('status') == 'success']
        failed = [r for r in self.results.values() if r.get('status') in ['failed', 'exception', 'timeout']]
       
        self.logger.info(f"Total cities processed: {len(cities_config)}")
        self.logger.info(f"Successful: {len(successful)}")
        self.logger.info(f"Failed: {len(failed)}")
        self.logger.info(f"Success rate: {len(successful)/len(cities_config)*100:.1f}%")
        self.logger.info(f"Total duration: {total_duration}")
       
        if successful:
            self.logger.info("")
            self.logger.info("SUCCESSFUL CITIES:")
            for result in successful:
                duration_str = str(result.get('duration', 'N/A'))
                files_count = len(result.get('result_files', []))
                self.logger.info(f"  SUCCESS: {result['city']} - {duration_str} - {files_count} files")
                self.logger.info(f"           Working dir: {result['work_dir']}")
       
        if failed:
            self.logger.info("")
            self.logger.info("FAILED CITIES:")
            for result in failed:
                error = result.get('error', 'Unknown error')[:200]
                self.logger.info(f"  FAILED: {result['city']} - {result.get('status', 'unknown')}")
                self.logger.info(f"          Error: {error}")
                if 'work_dir' in result:
                    self.logger.info(f"          Working dir: {result['work_dir']}")


def main():
    """Main function"""
    print("PARALLEL STAGGERED CITY SCRAPER")
    print("This will run cities in parallel but with staggered startup")
    print()
   
    # Ask for startup delay preference
    try:
        delay_input = input("Enter startup delay between cities in minutes (default: 5): ").strip()
        if delay_input:
            delay_minutes = float(delay_input)
        else:
            delay_minutes = 5
       
        delay_seconds = int(delay_minutes * 60)
        print(f"Using startup delay: {delay_minutes} minutes ({delay_seconds} seconds)")
       
    except ValueError:
        print("Invalid input, using default 5 minutes")
        delay_seconds = 300
   
    print()
   
    # Configuration for both cities
    cities_config = [
        {
            'name': 'Aceh',
            'csv_path': r'Provinsi\W01.csv',
            'max_results': 50000
        },
        {
            'name': 'Jakarta Selatan',
            'csv_path': r'Provinsi\W14.csv',
            'max_results': 50000
         }#,
        # {
        #     'name': 'Jakarta Timur',
        #     'csv_path': r'Provinsi\W15.csv',
        #     'max_results': 50000
        # },
        # {
        #     'name': 'Jayapura',
        #     'csv_path': r'Provinsi\W16.csv',
        #     'max_results': 50000
        # }
    ]
   
    print("Cities to process:")
    for i, city in enumerate(cities_config):
        start_time = datetime.now() + timedelta(seconds=i * delay_seconds)
        print(f"  {i+1}. {city['name']} - starts at {start_time.strftime('%H:%M:%S')}")
   
    print()
   
    # Confirm before starting
    response = input("Ready to start parallel staggered scraping? (y/n): ").strip().lower()
    if response != 'y':
        print("Scraping cancelled by user")
        return
   
    # Initialize and run scraper
    scraper = ParallelStaggeredScraper(
        script_path="scrape_gmaps_fix.py",
        startup_delay=delay_seconds
    )
   
    try:
        results = scraper.run_parallel_staggered(cities_config)
       
        successful_count = len([r for r in results if r.get('status') == 'success'])
       
        if successful_count > 0:
            print(f"\nPARALLEL SCRAPING COMPLETED!")
            print(f"Successful cities: {successful_count}/{len(cities_config)}")
        else:
            print(f"\nPARALLEL SCRAPING COMPLETED with issues.")
            print(f"Check the logs for detailed error information.")
           
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        print("Attempting to stop running processes...")
       
        # Try to terminate running processes
        for city_name, proc_info in scraper.running_processes.items():
            try:
                proc_info['process'].terminate()
                print(f"Terminated {city_name}")
            except:
                pass
               
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Ensure proper multiprocessing context for Windows
    if sys.platform.startswith('win'):
        mp.set_start_method('spawn', force=True)
   
    main()



