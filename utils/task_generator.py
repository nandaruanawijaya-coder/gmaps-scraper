"""
Utility for generating search tasks from keywords and locations
"""
import pandas as pd
from typing import List
from ..models.place import SearchTask


class TaskGenerator:
    """Generate search tasks from keywords and locations"""
    
    @staticmethod
    def generate_tasks(
        keywords: List[str],
        locations: List[str],
        max_results_per_task: int = 50
    ) -> List[SearchTask]:
        """
        Generate search tasks from keywords and locations
        
        Args:
            keywords: List of search keywords (e.g., ["warung kelontong", "minimarket"])
            locations: List of locations (e.g., ["Kebayoran Baru", "Cilandak"])
            max_results_per_task: Maximum results to collect per task
            
        Returns:
            List of SearchTask objects
        """
        tasks = []
        
        for keyword in keywords:
            for location in locations:
                task = SearchTask(
                    keyword=keyword,
                    location=location,
                    max_results=max_results_per_task
                )
                tasks.append(task)
        
        return tasks
    
    @staticmethod
    def generate_from_csv(
        csv_path: str,
        keyword_column: str = 'keyword',
        location_column: str = 'location',
        max_results_per_task: int = 50
    ) -> List[SearchTask]:
        """
        Generate tasks from CSV file
        
        Args:
            csv_path: Path to CSV file
            keyword_column: Name of keyword column
            location_column: Name of location column
            max_results_per_task: Maximum results per task
            
        Returns:
            List of SearchTask objects
        """
        df = pd.read_csv(csv_path)
        
        tasks = []
        for _, row in df.iterrows():
            task = SearchTask(
                keyword=str(row[keyword_column]),
                location=str(row[location_column]),
                max_results=max_results_per_task
            )
            tasks.append(task)
        
        return tasks
    
    @staticmethod
    def generate_district_tasks(
        keywords: List[str],
        city: str,
        districts: List[str],
        max_results_per_task: int = 50
    ) -> List[SearchTask]:
        """
        Generate tasks for districts in a city
        
        Args:
            keywords: List of keywords to search
            city: City name (e.g., "Jakarta Selatan")
            districts: List of district names (e.g., ["Kebayoran Baru", "Cilandak"])
            max_results_per_task: Maximum results per task
            
        Returns:
            List of SearchTask objects
        """
        tasks = []
        
        for keyword in keywords:
            for district in districts:
                # Format: "warung kelontong Kebayoran Baru Jakarta Selatan"
                location = f"{district}, {city}"
                task = SearchTask(
                    keyword=keyword,
                    location=location,
                    max_results=max_results_per_task
                )
                tasks.append(task)
        
        return tasks


# Predefined district lists for major cities
JAKARTA_SELATAN_DISTRICTS = [
    "Kebayoran Baru",
    "Kebayoran Lama",
    "Pesanggrahan",
    "Cilandak",
    "Pasar Minggu",
    "Jagakarsa",
    "Mampang Prapatan",
    "Pancoran",
    "Tebet",
    "Setiabudi"
]

JAKARTA_PUSAT_DISTRICTS = [
    "Tanah Abang",
    "Menteng",
    "Senen",
    "Johar Baru",
    "Cempaka Putih",
    "Kemayoran",
    "Sawah Besar",
    "Gambir"
]

JAKARTA_UTARA_DISTRICTS = [
    "Penjaringan",
    "Pademangan",
    "Tanjung Priok",
    "Koja",
    "Kelapa Gading",
    "Cilincing"
]

JAKARTA_TIMUR_DISTRICTS = [
    "Matraman",
    "Pulo Gadung",
    "Jatinegara",
    "Cakung",
    "Duren Sawit",
    "Kramat Jati",
    "Makasar",
    "Pasar Rebo",
    "Ciracas",
    "Cipayung"
]

JAKARTA_BARAT_DISTRICTS = [
    "Tambora",
    "Taman Sari",
    "Cengkareng",
    "Grogol Petamburan",
    "Kebon Jeruk",
    "Kalideres",
    "Palmerah",
    "Kembangan"
]