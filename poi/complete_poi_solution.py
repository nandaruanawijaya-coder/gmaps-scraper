import pandas as pd
import numpy as np
from data_cleaner import DataCleaner, quick_fix_concatenated_coords
from poi_define import OptimizedPOIDetector, quick_poi_analysis

def complete_poi_solution(df, lat_col='latitude', lon_col='longitude'):
    """
    Complete solution that handles data cleaning AND POI detection
    
    This function:
    1. Diagnoses and fixes coordinate data issues
    2. Runs optimized POI detection
    3. Returns cleaned data and POI results
    """
    
    print("🚀 COMPLETE POI DETECTION SOLUTION")
    print("=" * 50)
    
    print(f"Input data: {len(df)} rows")
    print(f"Columns: {list(df.columns)}")
    
    # STEP 1: Clean the data
    print(f"\n📋 STEP 1: DATA CLEANING")
    print("-" * 25)
    
    cleaner = DataCleaner()
    
    # Check if cleaning is needed
    issues = cleaner.diagnose_coordinate_issues(df, lat_col, lon_col)
    
    if issues:
        print(f"\n🔧 Fixing {len(issues)} detected issues...")
        df_cleaned = cleaner.clean_dataset(df, lat_col, lon_col, auto_fix=True)
    else:
        print(f"\n✅ Data is already clean!")
        df_cleaned = df.copy()
    
    if len(df_cleaned) == 0:
        print("❌ No valid data remaining after cleaning!")
        return None, None
    
    # STEP 2: POI Detection
    print(f"\n🎯 STEP 2: POI DETECTION")
    print("-" * 25)
    
    print(f"Running POI detection on {len(df_cleaned)} clean records...")
    
    try:
        # Use the quick analysis to find best parameters
        results_df, best_result = quick_poi_analysis(df_cleaned, lat_col, lon_col)
        
        if best_result and best_result['pois_found'] > 0:
            print(f"\n🎉 SUCCESS!")
            print(f"Best method: {best_result['method']}")
            print(f"POIs found: {best_result['pois_found']}")
            print(f"Coverage: {best_result['coverage']:.1f}%")
            
            # Get the final detector with results
            detector = OptimizedPOIDetector(df_cleaned, lat_col, lon_col)
            
            if best_result['method'] == 'DBSCAN':
                pois = detector.detect_pois_fast_dbscan(
                    radius_meters=best_result['radius'],
                    min_merchants=best_result['min_merchants']
                )
            elif best_result['method'] == 'Adaptive':
                pois = detector.detect_pois_adaptive_density(
                    initial_radius=best_result['radius'],
                    min_merchants=best_result['min_merchants']
                )
            else:  # KMeans
                pois = detector.detect_pois_kmeans_optimized(
                    max_radius=best_result['radius'],
                    min_merchants=best_result['min_merchants']
                )
            
            return df_cleaned, detector, pois, best_result
        
        else:
            print(f"\n⚠️ No POIs found with standard parameters")
            print(f"Trying relaxed parameters...")
            
            # Try more relaxed parameters
            detector = OptimizedPOIDetector(df_cleaned, lat_col, lon_col)
            
            relaxed_attempts = [
                ('DBSCAN', 'detect_pois_fast_dbscan', {'radius_meters': 600, 'min_merchants': 10}),
                ('DBSCAN', 'detect_pois_fast_dbscan', {'radius_meters': 800, 'min_merchants': 8}),
                ('Adaptive', 'detect_pois_adaptive_density', {'initial_radius': 1000, 'min_merchants': 8}),
                ('DBSCAN', 'detect_pois_fast_dbscan', {'radius_meters': 1000, 'min_merchants': 5}),
            ]
            
            for method_name, method_func, params in relaxed_attempts:
                print(f"  Trying {method_name} with {params}")
                
                try:
                    detector_temp = OptimizedPOIDetector(df_cleaned, lat_col, lon_col)
                    method = getattr(detector_temp, method_func)
                    pois = method(**params)
                    
                    if len(pois) > 0:
                        stats = detector_temp.get_statistics()
                        print(f"    ✅ Found {len(pois)} POIs! Coverage: {stats['coverage_percentage']:.1f}%")
                        
                        result = {
                            'method': method_name,
                            'pois_found': len(pois),
                            'coverage': stats['coverage_percentage'],
                            'parameters': params
                        }
                        
                        return df_cleaned, detector_temp, pois, result
                    else:
                        print(f"    ❌ No POIs found")
                        
                except Exception as e:
                    print(f"    ❌ Error: {str(e)[:50]}...")
            
            print(f"\n📊 Data characteristics might not be suitable for POI detection:")
            print(f"   - Total merchants: {len(df_cleaned)}")
            print(f"   - Try manual inspection or different approaches")
            
            return df_cleaned, None, None, None
    
    except Exception as e:
        print(f"\n❌ Error during POI detection: {str(e)}")
        print(f"Returning cleaned data only...")
        return df_cleaned, None, None, None


def handle_your_specific_error(df, lat_col='latitude', lon_col='longitude'):
    """
    Specifically handle the concatenated coordinate error you encountered
    """
    
    print("🔧 FIXING YOUR SPECIFIC COORDINATE ERROR")
    print("=" * 45)
    
    # Your error suggests coordinates like: '-8.7096028-8.7096583-8.7114426...'
    
    print("Detected issue: Concatenated coordinates separated by '-'")
    print("Example problematic value:", df[lat_col].iloc[0] if len(df) > 0 else "N/A")
    
    # Clean the data
    cleaner = DataCleaner()
    df_cleaned = cleaner.fix_concatenated_coordinates(
        df,
        lat_col=lat_col,
        lon_col=lon_col,
        separator='-',
        method='first'  # Take the first coordinate from each concatenated string
    )
    
    # Remove any remaining invalid data
    df_cleaned = df_cleaned.dropna(subset=[lat_col, lon_col])
    df_cleaned[lat_col] = pd.to_numeric(df_cleaned[lat_col], errors='coerce')
    df_cleaned[lon_col] = pd.to_numeric(df_cleaned[lon_col], errors='coerce')
    df_cleaned = df_cleaned.dropna(subset=[lat_col, lon_col])
    
    print(f"\nCleaning results:")
    print(f"  Original rows: {len(df)}")
    print(f"  Cleaned rows: {len(df_cleaned)}")
    print(f"  Removed: {len(df) - len(df_cleaned)}")
    
    if len(df_cleaned) > 0:
        print(f"  Lat range: {df_cleaned[lat_col].min():.6f} to {df_cleaned[lat_col].max():.6f}")
        print(f"  Lon range: {df_cleaned[lon_col].min():.6f} to {df_cleaned[lon_col].max():.6f}")
        print(f"  Sample coordinates:")
        for i in range(min(3, len(df_cleaned))):
            row = df_cleaned.iloc[i]
            print(f"    {row[lat_col]:.6f}, {row[lon_col]:.6f}")
    
    return df_cleaned


# Create test data to demonstrate the fix
def create_test_data_with_concatenated_coords():
    """
    Create test data that mimics your concatenated coordinate problem
    """
    
    # Simulate your problematic data
    problematic_data = {
        'merchant_id': [f'M_{i:03d}' for i in range(20)],
        'latitude': [
            '-8.7096028-8.7096583-8.7114426',
            '-8.7055983-8.7075263-8.7064947', 
            '-8.7108147-8.7116563-8.7076849',
            '-8.7108262-8.7108914-8.7075621',
            '-8.7107953-8.7076736-8.7081228',
            # Some normal values mixed in
            '-8.7105679',
            '-8.7106134', 
            '-8.7093778-8.7066775-8.7073352',
            '-8.7080000',
            '-8.7090000',
            # More concatenated values
            '-8.7096028-8.7096583-8.7114426-8.7114058',
            '-8.7055983-8.7055983-8.7075263-8.7064947',
            '-8.7108147-8.7116563-8.7116563-8.7076849',
            '-8.7108262-8.7108914-8.7075621-8.7107953',
            '-8.7076736-8.7081228-8.7105679-8.7106134',
            '-8.7093778-8.7066775-8.7073352',
            '-8.7100000',
            '-8.7110000',
            '-8.7120000',
            '-8.7130000'
        ],
        'longitude': [
            '115.1234567-115.1234890-115.1235123',
            '115.1240000-115.1241000-115.1242000',
            '115.1250000-115.1251000-115.1252000',
            '115.1260000-115.1261000-115.1262000',
            '115.1270000-115.1271000-115.1272000',
            '115.1280000',
            '115.1290000',
            '115.1300000-115.1301000-115.1302000',
            '115.1310000',
            '115.1320000',
            '115.1330000-115.1331000-115.1332000-115.1333000',
            '115.1340000-115.1341000-115.1342000-115.1343000',
            '115.1350000-115.1351000-115.1352000-115.1353000',
            '115.1360000-115.1361000-115.1362000-115.1363000',
            '115.1370000-115.1371000-115.1372000-115.1373000',
            '115.1380000-115.1381000-115.1382000',
            '115.1390000',
            '115.1400000',
            '115.1410000',
            '115.1420000'
        ],
        'city': ['Jakarta'] * 20,
        'district': [f'District_{i%3+1}' for i in range(20)]
    }
    
    return pd.DataFrame(problematic_data)


def demo_complete_solution():
    """
    Demonstrate the complete solution with problematic data
    """
    
    print("🎬 DEMONSTRATING COMPLETE SOLUTION")
    print("=" * 50)
    
    # Create test data with your exact problem
    df_problematic = create_test_data_with_concatenated_coords()
    
    print("Created test data that mimics your coordinate problem:")
    print(f"Sample latitude value: {df_problematic['latitude'].iloc[0]}")
    print(f"Sample longitude value: {df_problematic['longitude'].iloc[0]}")
    
    # Run complete solution
    result = complete_poi_solution(df_problematic, 'latitude', 'longitude')
    
    if result[0] is not None:  # df_cleaned
        df_cleaned, detector, pois, best_result = result
        
        print(f"\n🎉 FINAL RESULTS:")
        print(f"  Clean data: {len(df_cleaned)} merchants")
        
        if detector and pois is not None:
            print(f"  POIs found: {len(pois)}")
            print(f"  Method used: {best_result['method']}")
            
            if len(pois) > 0:
                stats = detector.get_statistics()
                print(f"  Coverage: {stats['coverage_percentage']:.1f}%")
                print(f"  Merchants in POIs: {stats['total_merchants_in_pois']}")
                
                # Create map
                detector.visualize_pois('/home/claude/demo_poi_results.html')
                print(f"  Map saved: demo_poi_results.html")
        
        return df_cleaned, detector
    
    else:
        print("❌ Solution failed")
        return None, None


if __name__ == "__main__":
    # Run demonstration
    df_cleaned, detector = demo_complete_solution()
    
    print(f"\n" + "="*60)
    print("HOW TO USE WITH YOUR ACTUAL DATA:")
    print("="*60)
    
    usage_code = '''
# SOLUTION FOR YOUR EXACT ERROR:

import pandas as pd
from complete_poi_solution import complete_poi_solution, handle_your_specific_error

# Load your data
df = pd.read_csv('your_file.csv')  # or however you load it

# OPTION 1: Complete automatic solution
df_cleaned, detector, pois, result = complete_poi_solution(df, 'latitude', 'longitude')

if detector and pois is not None:
    print(f"Success! Found {len(pois)} POIs")
    detector.visualize_pois('my_poi_results.html')
else:
    print("Try manual parameter adjustment")

# OPTION 2: Just fix the coordinate issue first
df_cleaned = handle_your_specific_error(df, 'latitude', 'longitude')

# Then run POI detection manually
from poi_detector_optimized import OptimizedPOIDetector
detector = OptimizedPOIDetector(df_cleaned, 'latitude', 'longitude')
pois = detector.detect_pois_fast_dbscan(radius_meters=400, min_merchants=15)

# OPTION 3: Quick fix function
from data_cleaner import quick_fix_concatenated_coords
df_cleaned = quick_fix_concatenated_coords(df, 'latitude', 'longitude')
'''
    
    print(usage_code)