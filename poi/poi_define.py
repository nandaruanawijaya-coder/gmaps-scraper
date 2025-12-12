import pandas as pd
import numpy as np
from sklearn.cluster import KMeans, DBSCAN
from sklearn.neighbors import NearestNeighbors
from geopy.distance import geodesic
import folium
from folium.plugins import MarkerCluster
import matplotlib.pyplot as plt
from tqdm import tqdm
from scipy.spatial.distance import cdist
from scipy.spatial import cKDTree
import warnings
warnings.filterwarnings('ignore')

class OptimizedPOIDetector:
    def __init__(self, df, lat_col='latitude', lon_col='longitude'):
        """
        Optimized POI Detector with performance improvements
        
        Parameters:
        - df: DataFrame with merchant data
        - lat_col: Column name for latitude
        - lon_col: Column name for longitude
        """
        self.df = df.copy()
        self.lat_col = lat_col
        self.lon_col = lon_col
        self.pois = None
        
        
        self.coords = self.df[[lat_col, lon_col]].values
        
        
        self.tree = cKDTree(self.coords)
        
        print(f"Initialized POI Detector with {len(df)} merchants")
        
    def haversine_distance_vectorized(self, lat1, lon1, lat2_arr, lon2_arr):
        """Vectorized haversine distance calculation"""
        
        lat2_arr = np.array(lat2_arr)
        lon2_arr = np.array(lon2_arr)
        
        
        lat1_rad = np.radians(lat1)
        lon1_rad = np.radians(lon1)
        lat2_rad = np.radians(lat2_arr)
        lon2_rad = np.radians(lon2_arr)
        
        
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        a = np.sin(dlat/2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        
        
        R = 6371000
        return R * c
    
    def meters_to_degrees_approx(self, meters, lat):
        """Approximate conversion from meters to degrees for KDTree queries"""
        
        
        lat_deg_to_meters = 111320
        lon_deg_to_meters = 111320 * np.cos(np.radians(lat))
        
        lat_tolerance = meters / lat_deg_to_meters
        lon_tolerance = meters / lon_deg_to_meters
        
        return lat_tolerance, lon_tolerance
    
    def find_neighbors_fast(self, center_idx, radius_meters):
        """Fast neighbor finding using KDTree approximation + exact distance verification"""
        center_lat, center_lon = self.coords[center_idx]
        
        
        lat_tol, lon_tol = self.meters_to_degrees_approx(radius_meters, center_lat)
        max_tol = max(lat_tol, lon_tol)
        
        
        neighbor_indices = self.tree.query_ball_point([center_lat, center_lon], r=max_tol)
        
        if len(neighbor_indices) <= 1:  
            return []
        
        
        neighbor_coords = self.coords[neighbor_indices]
        exact_distances = self.haversine_distance_vectorized(
            center_lat, center_lon,
            neighbor_coords[:, 0], neighbor_coords[:, 1]
        )
        
        
        within_radius = exact_distances <= radius_meters
        final_indices = [neighbor_indices[i] for i, within in enumerate(within_radius) if within]
        
        return final_indices
    
    def detect_pois_fast_dbscan(self, radius_meters=250, min_merchants=30):
        """
        Fast POI detection using DBSCAN with optimized distance calculation
        """
        print(f"\nDetecting POIs using Fast DBSCAN...")
        print(f"Radius: {radius_meters}m, Min merchants: {min_merchants}")
        
        
        print("Computing distance matrix...")
        
        
        
        sample_lat = self.df[self.lat_col].mean()
        _, degree_radius = self.meters_to_degrees_approx(radius_meters, sample_lat)
        
        
        coords_rad = np.radians(self.coords)
        dbscan = DBSCAN(
            eps=radius_meters/6371000,  
            min_samples=min_merchants,
            metric='haversine'
        ).fit(coords_rad)
        
        
        self.df['poi_cluster'] = dbscan.labels_
        self.df['distance_to_center'] = np.nan
        
        unique_clusters = np.unique(dbscan.labels_)
        unique_clusters = unique_clusters[unique_clusters != -1]  
        
        pois_list = []
        
        for cluster_id in unique_clusters:
            cluster_mask = self.df['poi_cluster'] == cluster_id
            cluster_merchants = self.df[cluster_mask]
            
            
            center_lat = cluster_merchants[self.lat_col].mean()
            center_lon = cluster_merchants[self.lon_col].mean()
            
            
            distances = self.haversine_distance_vectorized(
                center_lat, center_lon,
                cluster_merchants[self.lat_col].values,
                cluster_merchants[self.lon_col].values
            )
            
            
            self.df.loc[cluster_mask, 'distance_to_center'] = distances
            
            
            poi_info = {
                'poi_id': f'POI_{cluster_id:03d}',
                'center_lat': center_lat,
                'center_lon': center_lon,
                'merchant_count': len(cluster_merchants),
                'max_distance': distances.max(),
                'avg_distance': distances.mean(),
                'min_distance': distances.min(),
                'radius_meters': radius_meters,
                'min_merchants': min_merchants
            }
            
            
            for col in ['subdistrict', 'district', 'city']:
                if col in cluster_merchants.columns:
                    mode_val = cluster_merchants[col].mode()
                    poi_info[col] = mode_val.iloc[0] if not mode_val.empty else ''
            
            pois_list.append(poi_info)
        
        self.pois = pd.DataFrame(pois_list)
        
        
        print(f"\nResults:")
        print(f"- Total POIs found: {len(self.pois)}")
        print(f"- Merchants in POIs: {len(self.df[self.df['poi_cluster'] != -1])}")
        print(f"- Coverage: {len(self.df[self.df['poi_cluster'] != -1])/len(self.df)*100:.1f}%")
        
        if len(self.pois) > 0:
            print(f"\nPOI Details:")
            for _, poi in self.pois.iterrows():
                print(f"  {poi['poi_id']}: {poi['merchant_count']} merchants, "
                      f"max dist: {poi['max_distance']:.0f}m")
        
        return self.pois
    
    def detect_pois_adaptive_density(self, initial_radius=500, min_merchants=30, 
                                   density_threshold=0.8):
        """
        Adaptive density-based POI detection that starts with larger radius
        and refines to find optimal clusters
        """
        print(f"\nDetecting POIs using Adaptive Density method...")
        print(f"Initial radius: {initial_radius}m, Min merchants: {min_merchants}")
        
        
        density_centers = []
        
        print("Finding density centers...")
        for i in tqdm(range(0, len(self.df), 10), desc="Sampling for density"):  
            neighbors = self.find_neighbors_fast(i, initial_radius)
            if len(neighbors) >= min_merchants:
                center_lat, center_lon = self.coords[i]
                density_centers.append({
                    'idx': i,
                    'lat': center_lat,
                    'lon': center_lon,
                    'density': len(neighbors),
                    'neighbors': neighbors
                })
        
        if not density_centers:
            print("No density centers found. Try reducing min_merchants or increasing radius.")
            self.pois = pd.DataFrame()
            return self.pois
        
        print(f"Found {len(density_centers)} potential density centers")
        
        
        density_centers = sorted(density_centers, key=lambda x: x['density'], reverse=True)
        
        assigned_merchants = set()
        pois_list = []
        poi_id = 0
        
        for center in density_centers:
            
            available_neighbors = [idx for idx in center['neighbors'] if idx not in assigned_merchants]
            
            if len(available_neighbors) < min_merchants:
                continue
            
            
            poi_merchants_idx = available_neighbors
            poi_merchants = self.df.iloc[poi_merchants_idx]
            
            
            center_lat = poi_merchants[self.lat_col].mean()
            center_lon = poi_merchants[self.lon_col].mean()
            
            
            distances_from_center = self.haversine_distance_vectorized(
                center_lat, center_lon,
                poi_merchants[self.lat_col].values,
                poi_merchants[self.lon_col].values
            )
            
            
            adaptive_radius = np.percentile(distances_from_center, 80)  
            adaptive_radius = min(adaptive_radius, initial_radius * 0.6)  
            adaptive_radius = max(adaptive_radius, 100)  
            
            
            within_adaptive = distances_from_center <= adaptive_radius
            final_merchants_idx = [poi_merchants_idx[i] for i, within in enumerate(within_adaptive) if within]
            final_merchants = self.df.iloc[final_merchants_idx]
            
            if len(final_merchants) >= min_merchants:
                
                center_lat = final_merchants[self.lat_col].mean()
                center_lon = final_merchants[self.lon_col].mean()
                
                final_distances = self.haversine_distance_vectorized(
                    center_lat, center_lon,
                    final_merchants[self.lat_col].values,
                    final_merchants[self.lon_col].values
                )
                
                
                self.df.loc[final_merchants.index, 'poi_cluster'] = poi_id
                self.df.loc[final_merchants.index, 'distance_to_center'] = final_distances
                
                
                poi_info = {
                    'poi_id': f'POI_{poi_id:03d}',
                    'center_lat': center_lat,
                    'center_lon': center_lon,
                    'merchant_count': len(final_merchants),
                    'max_distance': final_distances.max(),
                    'avg_distance': final_distances.mean(),
                    'min_distance': final_distances.min(),
                    'radius_meters': adaptive_radius,
                    'min_merchants': min_merchants
                }
                
                
                for col in ['subdistrict', 'district', 'city']:
                    if col in final_merchants.columns:
                        mode_val = final_merchants[col].mode()
                        poi_info[col] = mode_val.iloc[0] if not mode_val.empty else ''
                
                pois_list.append(poi_info)
                assigned_merchants.update(final_merchants.index.tolist())
                poi_id += 1
        
        
        if 'poi_cluster' not in self.df.columns:
            self.df['poi_cluster'] = -1
            self.df['distance_to_center'] = np.nan
        
        self.pois = pd.DataFrame(pois_list)
        
        
        print(f"\nResults:")
        print(f"- Total POIs found: {len(self.pois)}")
        print(f"- Merchants in POIs: {len(self.df[self.df['poi_cluster'] != -1])}")
        print(f"- Coverage: {len(self.df[self.df['poi_cluster'] != -1])/len(self.df)*100:.1f}%")
        
        if len(self.pois) > 0:
            print(f"\nPOI Details:")
            for _, poi in self.pois.iterrows():
                print(f"  {poi['poi_id']}: {poi['merchant_count']} merchants, "
                      f"adaptive radius: {poi['radius_meters']:.0f}m, "
                      f"max dist: {poi['max_distance']:.0f}m")
        
        return self.pois
    
    def detect_pois_kmeans_optimized(self, n_clusters=None, max_radius=300, min_merchants=30):
        """
        Optimized KMeans approach with automatic cluster number detection
        """
        print(f"\nDetecting POIs using Optimized KMeans...")
        
        if n_clusters is None:
            
            total_merchants = len(self.df)
            estimated_clusters = max(1, total_merchants // (min_merchants * 2))
            n_clusters = min(estimated_clusters, 20)  
            print(f"Auto-estimated clusters: {n_clusters}")
        
        print(f"Clusters: {n_clusters}, Max radius: {max_radius}m, Min merchants: {min_merchants}")
        
        
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        cluster_labels = kmeans.fit_predict(self.coords)
        
        
        self.df['poi_cluster'] = -1  
        self.df['distance_to_center'] = np.nan
        
        pois_list = []
        valid_poi_id = 0
        
        for cluster_id in range(n_clusters):
            cluster_mask = cluster_labels == cluster_id
            cluster_merchants = self.df[cluster_mask]
            
            if len(cluster_merchants) < min_merchants:
                continue
            
            
            center_lat = cluster_merchants[self.lat_col].mean()
            center_lon = cluster_merchants[self.lon_col].mean()
            
            
            distances = self.haversine_distance_vectorized(
                center_lat, center_lon,
                cluster_merchants[self.lat_col].values,
                cluster_merchants[self.lon_col].values
            )
            
            
            within_radius = distances <= max_radius
            final_merchants = cluster_merchants[within_radius]
            final_distances = distances[within_radius]
            
            if len(final_merchants) >= min_merchants:
                
                center_lat = final_merchants[self.lat_col].mean()
                center_lon = final_merchants[self.lon_col].mean()
                
                
                final_distances = self.haversine_distance_vectorized(
                    center_lat, center_lon,
                    final_merchants[self.lat_col].values,
                    final_merchants[self.lon_col].values
                )
                
                
                self.df.loc[final_merchants.index, 'poi_cluster'] = valid_poi_id
                self.df.loc[final_merchants.index, 'distance_to_center'] = final_distances
                
                
                poi_info = {
                    'poi_id': f'POI_{valid_poi_id:03d}',
                    'center_lat': center_lat,
                    'center_lon': center_lon,
                    'merchant_count': len(final_merchants),
                    'max_distance': final_distances.max(),
                    'avg_distance': final_distances.mean(),
                    'min_distance': final_distances.min(),
                    'radius_meters': max_radius,
                    'min_merchants': min_merchants,
                    'actual_max_radius': final_distances.max()
                }
                
                
                for col in ['subdistrict', 'district', 'city']:
                    if col in final_merchants.columns:
                        mode_val = final_merchants[col].mode()
                        poi_info[col] = mode_val.iloc[0] if not mode_val.empty else ''
                
                pois_list.append(poi_info)
                valid_poi_id += 1
        
        self.pois = pd.DataFrame(pois_list)
        
        
        print(f"\nResults:")
        print(f"- Total POIs found: {len(self.pois)}")
        print(f"- Merchants in POIs: {len(self.df[self.df['poi_cluster'] != -1])}")
        print(f"- Coverage: {len(self.df[self.df['poi_cluster'] != -1])/len(self.df)*100:.1f}%")
        
        if len(self.pois) > 0:
            print(f"\nPOI Details:")
            for _, poi in self.pois.iterrows():
                print(f"  {poi['poi_id']}: {poi['merchant_count']} merchants, "
                      f"max dist: {poi['max_distance']:.0f}m")
        
        return self.pois
    
    def visualize_pois(self, save_path='poi_map.html', show_radius=True):
        """Create interactive map visualization of POIs"""
        if self.pois is None or len(self.pois) == 0:
            print("No POIs detected yet. Run a detection method first.")
            return
        
        
        center_lat = self.df[self.lat_col].mean()
        center_lon = self.df[self.lon_col].mean()
        
        m = folium.Map(location=[center_lat, center_lon], zoom_start=12)
        
        
        for _, poi in self.pois.iterrows():
            
            folium.Marker(
                [poi['center_lat'], poi['center_lon']],
                popup=f"""
                <b>{poi['poi_id']}</b><br>
                Merchants: {poi['merchant_count']}<br>
                Max distance: {poi['max_distance']:.0f}m<br>
                Avg distance: {poi['avg_distance']:.0f}m<br>
                Radius: {poi['radius_meters']}m
                """,
                icon=folium.Icon(color='red', icon='star', prefix='fa'),
                tooltip=f"{poi['poi_id']}: {poi['merchant_count']} merchants"
            ).add_to(m)
            
            if show_radius:
                
                folium.Circle(
                    [poi['center_lat'], poi['center_lon']],
                    radius=poi['radius_meters'],
                    color='red',
                    weight=2,
                    fill=True,
                    fillOpacity=0.1,
                    popup=f"{poi['poi_id']} boundary ({poi['radius_meters']}m radius)"
                ).add_to(m)
        
        
        for _, merchant in self.df.iterrows():
            if merchant['poi_cluster'] != -1 and not pd.isna(merchant['poi_cluster']):
                color = 'blue'
                poi_label = f"POI_{int(merchant['poi_cluster']):03d}"
                if not pd.isna(merchant['distance_to_center']):
                    distance_label = f"Distance: {merchant['distance_to_center']:.0f}m"
                else:
                    distance_label = "Distance: N/A"
            else:
                color = 'gray'
                poi_label = 'No POI'
                distance_label = ''
            
            folium.CircleMarker(
                [merchant[self.lat_col], merchant[self.lon_col]],
                radius=3,
                popup=f"Merchant<br>{poi_label}<br>{distance_label}",
                color=color,
                fill=True,
                fillOpacity=0.7,
                weight=1
            ).add_to(m)
        
        
        legend_html = '''
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: 120px; 
                    background-color: white; z-index:9999; font-size:14px;
                    border:2px solid grey; border-radius: 5px; padding: 10px">
        <p style="margin: 0;"><b>Legend</b></p>
        <p style="margin: 5px;">⭐ POI Center</p>
        <p style="margin: 5px;">🔵 Merchant in POI</p>
        <p style="margin: 5px;">⚫ Merchant not in POI</p>
        <p style="margin: 5px;">⭕ POI Radius</p>
        </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))
        
        m.save(save_path)
        print(f"Map saved to {save_path}")
        
        return m
    
    def get_statistics(self):
        """Get detailed statistics about POIs"""
        if self.pois is None or len(self.pois) == 0:
            return {
                'total_pois': 0,
                'total_merchants_in_pois': 0,
                'coverage_percentage': 0
            }
        
        merchants_in_pois = len(self.df[self.df['poi_cluster'] != -1])
        
        stats = {
            'total_pois': len(self.pois),
            'total_merchants_in_pois': merchants_in_pois,
            'total_merchants_outside_pois': len(self.df) - merchants_in_pois,
            'coverage_percentage': (merchants_in_pois / len(self.df)) * 100,
            'avg_merchants_per_poi': self.pois['merchant_count'].mean(),
            'std_merchants_per_poi': self.pois['merchant_count'].std(),
            'min_merchants_in_poi': self.pois['merchant_count'].min(),
            'max_merchants_in_poi': self.pois['merchant_count'].max(),
            'avg_max_distance': self.pois['max_distance'].mean(),
            'avg_avg_distance': self.pois['avg_distance'].mean(),
        }
        
        return stats


def quick_poi_analysis(df, lat_col='latitude', lon_col='longitude'):
    """
    Quick analysis to test multiple approaches and find the best one
    """
    print("="*60)
    print("QUICK POI ANALYSIS - TESTING MULTIPLE APPROACHES")
    print("="*60)
    
    detector = OptimizedPOIDetector(df, lat_col, lon_col)
    results = {}
    
    
    print("\n" + "="*40)
    print("TEST 1: Fast DBSCAN")
    print("="*40)
    
    for radius in [200, 300, 500]:
        for min_merchants in [15, 20, 30]:
            print(f"\nTesting DBSCAN: radius={radius}m, min_merchants={min_merchants}")
            try:
                pois = detector.detect_pois_fast_dbscan(radius, min_merchants)
                stats = detector.get_statistics()
                results[f'DBSCAN_r{radius}_m{min_merchants}'] = {
                    'method': 'DBSCAN',
                    'radius': radius,
                    'min_merchants': min_merchants,
                    'pois_found': len(pois),
                    'coverage': stats['coverage_percentage']
                }
            except Exception as e:
                print(f"Error: {e}")
                results[f'DBSCAN_r{radius}_m{min_merchants}'] = {
                    'method': 'DBSCAN',
                    'radius': radius,
                    'min_merchants': min_merchants,
                    'pois_found': 0,
                    'coverage': 0,
                    'error': str(e)
                }
    
    
    print("\n" + "="*40)
    print("TEST 2: Adaptive Density")
    print("="*40)
    
    for initial_radius in [400, 600, 800]:
        for min_merchants in [15, 20, 30]:
            print(f"\nTesting Adaptive: initial_radius={initial_radius}m, min_merchants={min_merchants}")
            try:
                detector_adaptive = OptimizedPOIDetector(df, lat_col, lon_col)
                pois = detector_adaptive.detect_pois_adaptive_density(initial_radius, min_merchants)
                stats = detector_adaptive.get_statistics()
                results[f'Adaptive_r{initial_radius}_m{min_merchants}'] = {
                    'method': 'Adaptive',
                    'radius': initial_radius,
                    'min_merchants': min_merchants,
                    'pois_found': len(pois),
                    'coverage': stats['coverage_percentage']
                }
            except Exception as e:
                print(f"Error: {e}")
                results[f'Adaptive_r{initial_radius}_m{min_merchants}'] = {
                    'method': 'Adaptive',
                    'radius': initial_radius,
                    'min_merchants': min_merchants,
                    'pois_found': 0,
                    'coverage': 0,
                    'error': str(e)
                }
    
    
    print("\n" + "="*40)
    print("TEST 3: Optimized KMeans")
    print("="*40)
    
    for max_radius in [250, 350, 500]:
        for min_merchants in [15, 20, 30]:
            print(f"\nTesting KMeans: max_radius={max_radius}m, min_merchants={min_merchants}")
            try:
                detector_kmeans = OptimizedPOIDetector(df, lat_col, lon_col)
                pois = detector_kmeans.detect_pois_kmeans_optimized(
                    max_radius=max_radius, min_merchants=min_merchants
                )
                stats = detector_kmeans.get_statistics()
                results[f'KMeans_r{max_radius}_m{min_merchants}'] = {
                    'method': 'KMeans',
                    'radius': max_radius,
                    'min_merchants': min_merchants,
                    'pois_found': len(pois),
                    'coverage': stats['coverage_percentage']
                }
            except Exception as e:
                print(f"Error: {e}")
                results[f'KMeans_r{max_radius}_m{min_merchants}'] = {
                    'method': 'KMeans',
                    'radius': max_radius,
                    'min_merchants': min_merchants,
                    'pois_found': 0,
                    'coverage': 0,
                    'error': str(e)
                }
    
    
    print("\n" + "="*60)
    print("ANALYSIS SUMMARY")
    print("="*60)
    
    results_df = pd.DataFrame(list(results.values()))
    
    if len(results_df) > 0:
        print("\nTop 10 Results by POI Count:")
        top_results = results_df.nlargest(10, 'pois_found')
        print(top_results[['method', 'radius', 'min_merchants', 'pois_found', 'coverage']].to_string())
        
        print("\nTop 5 Results by Coverage:")
        top_coverage = results_df.nlargest(5, 'coverage')
        print(top_coverage[['method', 'radius', 'min_merchants', 'pois_found', 'coverage']].to_string())
        
        
        best_result = results_df.loc[results_df['pois_found'].idxmax()]
        print(f"\nBest performing configuration:")
        print(f"Method: {best_result['method']}")
        print(f"Radius: {best_result['radius']}m")
        print(f"Min merchants: {best_result['min_merchants']}")
        print(f"POIs found: {best_result['pois_found']}")
        print(f"Coverage: {best_result['coverage']:.1f}%")
        
        return results_df, best_result
    else:
        print("No successful results found.")
        return None, None



def run_poi_detection_example():
    """
    Example of how to use the optimized POI detector
    """
    
    np.random.seed(42)
    n_merchants = 1000
    
    
    centers = [(106.8456, -6.2088), (106.8500, -6.2100), (106.8400, -6.2050)]
    data = []
    
    for center_lon, center_lat in centers:
        n_cluster = np.random.randint(80, 150)
        for _ in range(n_cluster):
            
            lat = center_lat + np.random.normal(0, 0.005)
            lon = center_lon + np.random.normal(0, 0.005)
            data.append({
                'latitude': lat,
                'longitude': lon,
                'merchant_id': f'M_{len(data):04d}',
                'city': 'Jakarta',
                'district': f'District_{np.random.randint(1,4)}',
                'subdistrict': f'Subdistrict_{np.random.randint(1,10)}'
            })
    
    
    for i in range(n_merchants - len(data)):
        data.append({
            'latitude': -6.2088 + np.random.normal(0, 0.02),
            'longitude': 106.8456 + np.random.normal(0, 0.02),
            'merchant_id': f'M_{len(data):04d}',
            'city': 'Jakarta',
            'district': f'District_{np.random.randint(1,4)}',
            'subdistrict': f'Subdistrict_{np.random.randint(1,10)}'
        })
    
    df = pd.DataFrame(data)
    print(f"Created sample dataset with {len(df)} merchants")
    
    
    results_df, best_result = quick_poi_analysis(df)
    
    if best_result is not None:
        
        detector = OptimizedPOIDetector(df)
        
        if best_result['method'] == 'DBSCAN':
            pois = detector.detect_pois_fast_dbscan(
                best_result['radius'], 
                best_result['min_merchants']
            )
        elif best_result['method'] == 'Adaptive':
            pois = detector.detect_pois_adaptive_density(
                best_result['radius'], 
                best_result['min_merchants']
            )
        else:  
            pois = detector.detect_pois_kmeans_optimized(
                max_radius=best_result['radius'], 
                min_merchants=best_result['min_merchants']
            )
        
        
        detector.visualize_pois('sample_poi_map.html')
        
        return detector, pois
    
    return None, None


if __name__ == "__main__":
    run_poi_detection_example()