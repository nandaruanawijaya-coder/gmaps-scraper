import pandas as pd
import numpy as np
import re

class DataCleaner:
    """
    Utility class to clean and fix common data issues in POI detection datasets
    """
    
    def __init__(self):
        self.original_shape = None
        self.cleaned_shape = None
        self.issues_found = []
    
    def diagnose_coordinate_issues(self, df, lat_col='latitude', lon_col='longitude'):
        """
        Diagnose common coordinate data issues
        """
        print("DIAGNOSING COORDINATE DATA ISSUES")
        print("=" * 40)
        
        self.original_shape = df.shape
        print(f"Original dataset: {df.shape[0]} rows, {df.shape[1]} columns")
        
        issues = []
        
        # Check if columns exist
        if lat_col not in df.columns:
            issues.append(f"Missing latitude column: '{lat_col}'")
        if lon_col not in df.columns:
            issues.append(f"Missing longitude column: '{lon_col}'")
            
        if issues:
            print("\n❌ COLUMN ISSUES:")
            for issue in issues:
                print(f"  - {issue}")
            print(f"\nAvailable columns: {list(df.columns)}")
            return issues
        
        # Check data types
        print(f"\nData types:")
        print(f"  {lat_col}: {df[lat_col].dtype}")
        print(f"  {lon_col}: {df[lon_col].dtype}")
        
        # Check for string/object types (problematic)
        if df[lat_col].dtype == 'object':
            issues.append(f"{lat_col} column is object/string type")
        if df[lon_col].dtype == 'object':
            issues.append(f"{lon_col} column is object/string type")
        
        # Sample problematic values
        if df[lat_col].dtype == 'object':
            sample_vals = df[lat_col].dropna().head(3).tolist()
            print(f"\nSample {lat_col} values: {sample_vals}")
            
        if df[lon_col].dtype == 'object':
            sample_vals = df[lon_col].dropna().head(3).tolist()
            print(f"Sample {lon_col} values: {sample_vals}")
        
        # Check for null values
        lat_nulls = df[lat_col].isnull().sum()
        lon_nulls = df[lon_col].isnull().sum()
        
        if lat_nulls > 0:
            issues.append(f"{lat_nulls} null values in {lat_col}")
        if lon_nulls > 0:
            issues.append(f"{lon_nulls} null values in {lon_col}")
        
        # Check for concatenated values (your specific issue)
        if df[lat_col].dtype == 'object':
            sample_val = str(df[lat_col].dropna().iloc[0]) if len(df[lat_col].dropna()) > 0 else ""
            if len(sample_val) > 15:  # Normal lat/lon shouldn't be this long
                issues.append(f"Suspected concatenated values in {lat_col} (length: {len(sample_val)})")
        
        if df[lon_col].dtype == 'object':
            sample_val = str(df[lon_col].dropna().iloc[0]) if len(df[lon_col].dropna()) > 0 else ""
            if len(sample_val) > 15:
                issues.append(f"Suspected concatenated values in {lon_col} (length: {len(sample_val)})")
        
        # Check coordinate ranges (basic validation)
        try:
            lat_vals = pd.to_numeric(df[lat_col], errors='coerce')
            lon_vals = pd.to_numeric(df[lon_col], errors='coerce')
            
            lat_min, lat_max = lat_vals.min(), lat_vals.max()
            lon_min, lon_max = lon_vals.min(), lon_vals.max()
            
            print(f"\nCoordinate ranges:")
            print(f"  Latitude: {lat_min:.6f} to {lat_max:.6f}")
            print(f"  Longitude: {lon_min:.6f} to {lon_max:.6f}")
            
            # Check if ranges are reasonable
            if not (-90 <= lat_min <= lat_max <= 90):
                issues.append(f"Invalid latitude range: {lat_min} to {lat_max}")
            if not (-180 <= lon_min <= lon_max <= 180):
                issues.append(f"Invalid longitude range: {lon_min} to {lon_max}")
                
        except:
            issues.append("Cannot convert coordinates to numeric for range validation")
        
        self.issues_found = issues
        
        if issues:
            print(f"\n❌ ISSUES FOUND:")
            for i, issue in enumerate(issues, 1):
                print(f"  {i}. {issue}")
        else:
            print(f"\n✅ No major issues detected!")
        
        return issues
    
    def fix_concatenated_coordinates(self, df, lat_col='latitude', lon_col='longitude', 
                                   separator='-', method='first'):
        """
        Fix concatenated coordinate values
        
        Parameters:
        - df: DataFrame with issues
        - lat_col, lon_col: column names
        - separator: character used to separate values (default: '-')  
        - method: 'first', 'last', 'middle', 'average' - which value to use
        """
        print(f"\nFIXING CONCATENATED COORDINATES")
        print(f"Method: {method}, Separator: '{separator}'")
        print("-" * 30)
        
        df_cleaned = df.copy()
        
        def extract_coordinate(value, method='first'):
            """Extract single coordinate from concatenated string"""
            if pd.isna(value):
                return np.nan
            
            # Convert to string and clean
            str_val = str(value).strip()
            
            # If it's already a number, return it
            try:
                float_val = float(str_val)
                return float_val
            except:
                pass
            
            # Split by separator
            parts = str_val.split(separator)
            
            # Remove empty parts
            parts = [p.strip() for p in parts if p.strip()]
            
            if not parts:
                return np.nan
            
            # Convert parts to float
            try:
                numeric_parts = [float(p) for p in parts]
            except:
                return np.nan
            
            # Apply extraction method
            if method == 'first':
                return numeric_parts[0]
            elif method == 'last':
                return numeric_parts[-1]
            elif method == 'middle':
                mid_idx = len(numeric_parts) // 2
                return numeric_parts[mid_idx]
            elif method == 'average':
                return np.mean(numeric_parts)
            else:
                return numeric_parts[0]  # default to first
        
        # Fix latitude
        if df[lat_col].dtype == 'object':
            print(f"Processing {lat_col}...")
            df_cleaned[lat_col] = df[lat_col].apply(lambda x: extract_coordinate(x, method))
            
            # Show some examples
            before_sample = df[lat_col].dropna().head(3)
            after_sample = df_cleaned[lat_col].dropna().head(3)
            print(f"  Before: {before_sample.tolist()}")
            print(f"  After:  {after_sample.tolist()}")
        
        # Fix longitude  
        if df[lon_col].dtype == 'object':
            print(f"Processing {lon_col}...")
            df_cleaned[lon_col] = df[lon_col].apply(lambda x: extract_coordinate(x, method))
            
            # Show some examples
            before_sample = df[lon_col].dropna().head(3)
            after_sample = df_cleaned[lon_col].dropna().head(3)
            print(f"  Before: {before_sample.tolist()}")
            print(f"  After:  {after_sample.tolist()}")
        
        return df_cleaned
    
    def clean_dataset(self, df, lat_col='latitude', lon_col='longitude', 
                     auto_fix=True, separator='-', method='first'):
        """
        Comprehensive dataset cleaning
        """
        print("COMPREHENSIVE DATASET CLEANING")
        print("=" * 40)
        
        df_cleaned = df.copy()
        
        # Step 1: Diagnose issues
        issues = self.diagnose_coordinate_issues(df_cleaned, lat_col, lon_col)
        
        if not issues:
            print("\n✅ Dataset is already clean!")
            return df_cleaned
        
        # Step 2: Auto-fix if requested
        if auto_fix:
            print(f"\n🔧 AUTO-FIXING DETECTED ISSUES...")
            
            # Fix concatenated coordinates
            if any('concatenated' in issue.lower() for issue in issues):
                df_cleaned = self.fix_concatenated_coordinates(
                    df_cleaned, lat_col, lon_col, separator, method
                )
            
            # Remove null coordinates
            initial_rows = len(df_cleaned)
            df_cleaned = df_cleaned.dropna(subset=[lat_col, lon_col])
            removed_nulls = initial_rows - len(df_cleaned)
            
            if removed_nulls > 0:
                print(f"Removed {removed_nulls} rows with null coordinates")
            
            # Convert to numeric
            df_cleaned[lat_col] = pd.to_numeric(df_cleaned[lat_col], errors='coerce')
            df_cleaned[lon_col] = pd.to_numeric(df_cleaned[lon_col], errors='coerce')
            
            # Remove any remaining invalid coordinates
            initial_rows = len(df_cleaned)
            df_cleaned = df_cleaned.dropna(subset=[lat_col, lon_col])
            removed_invalid = initial_rows - len(df_cleaned)
            
            if removed_invalid > 0:
                print(f"Removed {removed_invalid} rows with invalid coordinates")
            
            # Validate coordinate ranges
            lat_mask = (df_cleaned[lat_col] >= -90) & (df_cleaned[lat_col] <= 90)
            lon_mask = (df_cleaned[lon_col] >= -180) & (df_cleaned[lon_col] <= 180)
            valid_mask = lat_mask & lon_mask
            
            initial_rows = len(df_cleaned)
            df_cleaned = df_cleaned[valid_mask]
            removed_invalid_range = initial_rows - len(df_cleaned)
            
            if removed_invalid_range > 0:
                print(f"Removed {removed_invalid_range} rows with out-of-range coordinates")
        
        # Step 3: Final validation
        print(f"\nFINAL VALIDATION:")
        final_issues = self.diagnose_coordinate_issues(df_cleaned, lat_col, lon_col)
        
        self.cleaned_shape = df_cleaned.shape
        
        print(f"\nCLEANING SUMMARY:")
        print(f"  Original: {self.original_shape[0]} rows")
        print(f"  Cleaned:  {self.cleaned_shape[0]} rows")
        print(f"  Removed:  {self.original_shape[0] - self.cleaned_shape[0]} rows")
        print(f"  Success:  {'✅' if not final_issues else '⚠️'}")
        
        return df_cleaned
    
    def suggest_fixes(self, df, lat_col='latitude', lon_col='longitude'):
        """
        Suggest specific fixes based on detected issues
        """
        issues = self.diagnose_coordinate_issues(df, lat_col, lon_col)
        
        if not issues:
            return "No fixes needed!"
        
        suggestions = []
        
        for issue in issues:
            if 'concatenated' in issue.lower():
                suggestions.append("""
# Fix concatenated coordinates:
cleaner = DataCleaner()
df_cleaned = cleaner.fix_concatenated_coordinates(
    df, 
    lat_col='latitude', 
    lon_col='longitude',
    separator='-',  # or whatever separator you see
    method='first'  # or 'average', 'middle', 'last'
)""")
            
            elif 'object' in issue.lower():
                suggestions.append("""
# Convert object columns to numeric:
df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')""")
                
            elif 'null' in issue.lower():
                suggestions.append("""
# Remove null coordinates:
df_cleaned = df.dropna(subset=['latitude', 'longitude'])""")
                
            elif 'range' in issue.lower():
                suggestions.append("""
# Filter valid coordinate ranges:
lat_mask = (df['latitude'] >= -90) & (df['latitude'] <= 90)
lon_mask = (df['longitude'] >= -180) & (df['longitude'] <= 180)
df_cleaned = df[lat_mask & lon_mask]""")
        
        return "\n".join(suggestions)


# Quick fix function for your specific error
def quick_fix_concatenated_coords(df, lat_col='latitude', lon_col='longitude'):
    """
    Quick fix for the specific error you encountered
    """
    print("QUICK FIX FOR CONCATENATED COORDINATES")
    print("=" * 40)
    
    cleaner = DataCleaner()
    df_cleaned = cleaner.clean_dataset(
        df, 
        lat_col=lat_col, 
        lon_col=lon_col,
        auto_fix=True,
        separator='-',  # Based on your error message
        method='first'
    )
    
    return df_cleaned


# Example usage
def example_fix():
    """
    Example of how to fix your data
    """
    
    print("EXAMPLE: FIXING YOUR COORDINATE DATA")
    print("=" * 40)
    
    example_code = '''
import pandas as pd
from data_cleaner import DataCleaner, quick_fix_concatenated_coords

# Load your data
df = pd.read_csv('your_data.csv')  # or however you load it

# Method 1: Quick fix (automatic)
df_cleaned = quick_fix_concatenated_coords(df, 'latitude', 'longitude')

# Method 2: Manual control
cleaner = DataCleaner()

# Diagnose first
issues = cleaner.diagnose_coordinate_issues(df, 'latitude', 'longitude')

# Fix concatenated coordinates
df_cleaned = cleaner.fix_concatenated_coordinates(
    df,
    lat_col='latitude',
    lon_col='longitude', 
    separator='-',      # Change this based on your data
    method='first'      # Options: 'first', 'last', 'middle', 'average'
)

# Full cleaning (recommended)
df_cleaned = cleaner.clean_dataset(df, 'latitude', 'longitude', auto_fix=True)

# Now use with POI detection
from poi_detector_optimized import OptimizedPOIDetector

detector = OptimizedPOIDetector(df_cleaned, 'latitude', 'longitude')
pois = detector.detect_pois_fast_dbscan(radius_meters=400, min_merchants=15)
'''
    
    print(example_code)


if __name__ == "__main__":
    example_fix()