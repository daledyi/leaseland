import ee
import geojson
import requests
import json
import time
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

def create_session():
    session = requests.Session()
    retries = Retry(total=3,
                   backoff_factor=0.5,
                   status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def get_webmap_data(webmap_id):
    webmap_url = f"https://www.arcgis.com/sharing/rest/content/items/{webmap_id}/data"
    params = {
        'f': 'json'
    }
    
    try:
        session = create_session()
        response = session.get(webmap_url, params=params, timeout=30)
        response.raise_for_status()
        webmap_data = response.json()
        
        layers = []
        for layer in webmap_data.get('operationalLayers', []):
            if 'url' in layer:
                layers.append({
                    'title': layer.get('title', 'Unnamed Layer'),
                    'url': layer['url']
                })
        
        return layers
    except requests.exceptions.RequestException as e:
        print(f"Network error getting webmap data: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON decode error in webmap data: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error getting webmap data: {e}")
        return None

def get_mapserver_layers(mapserver_url):
    try:
        session = create_session()
        response = session.get(f"{mapserver_url}?f=json", timeout=30)
        response.raise_for_status()
        data = response.json()
        
        layers = []
        if 'layers' in data:
            for layer in data['layers']:
                layers.append({
                    'id': layer.get('id'),
                    'name': layer.get('name'),
                    'url': f"{mapserver_url}/{layer.get('id')}"
                })
        return layers
    except Exception as e:
        print(f"Error getting MapServer layers: {e}")
        return None

def download_layer(layer_url):
    # Check if it's a MapServer URL
    if 'MapServer' in layer_url:
        # Get individual layers from MapServer
        mapserver_layers = get_mapserver_layers(layer_url)
        if mapserver_layers:
            all_features = {
                'type': 'FeatureCollection',
                'features': []
            }
            
            for sublayer in mapserver_layers:
                print(f"Downloading sublayer: {sublayer['name']}")
                data = download_feature_layer(sublayer['url'])
                if data and 'features' in data:
                    all_features['features'].extend(data['features'])
            
            return all_features if all_features['features'] else None
        
    else:
        # Handle as regular Feature Service
        return download_feature_layer(layer_url)

def download_feature_layer(layer_url):
    params = {
        'f': 'geojson',
        'where': '1=1',
        'outFields': '*',
        'returnGeometry': 'true',
        'maxRecordCount': 2000
    }
    
    try:
        session = create_session()
        response = session.get(f"{layer_url}/query", params=params, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        if 'type' not in data or 'features' not in data:
            raise ValueError("Invalid GeoJSON structure")
            
        return data
    except requests.exceptions.RequestException as e:
        print(f"Network error downloading layer: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON decode error in layer data: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error downloading layer: {e}")
        return None

def main():
    webmap_id = "f96e475a649a4ef1a8b45f74f9476adf"
    
    layers = get_webmap_data(webmap_id)
    
    if not layers:
        print("No layers found in webmap")
        return
    
    for i, layer in enumerate(layers):
        print(f"\nProcessing layer: {layer['title']}")
        
        if i > 0:
            time.sleep(2)
        
        data = download_layer(layer['url'])
        if data and data.get('features'):
            filename = f"layer_{i}_{layer['title'].replace(' ', '_')}.geojson"
            
            try:
                with open(filename, 'w') as f:
                    json.dump(data, f)
                
                print(f"Saved as {filename}")
                print(f"Number of features: {len(data['features'])}")
                if data['features']:
                    print("Properties:", list(data['features'][0]['properties'].keys()))
                print(f"GeoJSON type: {data.get('type', 'Not specified')}")
                print(f"CRS info: {data.get('crs', 'Not specified')}")
                
            except IOError as e:
                print(f"Error saving file {filename}: {e}")
        else:
            print(f"No features found in layer: {layer['title']}")

if __name__ == "__main__":
    main()