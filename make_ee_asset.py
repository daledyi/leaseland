# Initialize Earth Engine with your specific project
ee.Authenticate()
ee.Initialize(project='dnr-ag-remotesense')

def clean_property_name(name):
    """Clean property names to be Earth Engine compatible"""
    return name.replace('.', '_').replace(' ', '_')

# Read your GeoJSON file
with open('layer_2_DNR_Layers.geojson', 'r') as f:
    geojson_data = geojson.load(f)

# Convert to Earth Engine FeatureCollection with cleaned property names
features = []
for feature in geojson_data['features']:
    # Clean up property names
    cleaned_properties = {}
    for key, value in feature['properties'].items():
        cleaned_key = clean_property_name(key)
        cleaned_properties[cleaned_key] = value
    
    features.append(ee.Feature(
        ee.Geometry(feature['geometry']),
        cleaned_properties
    ))
    
ee_features = ee.FeatureCollection(features)

# Export to Earth Engine Asset
task = ee.batch.Export.table.toAsset(
    collection=ee_features,
    description='dnr_leases_upload',
    assetId='projects/dnr-ag-remotesense/assets/dnr_leases'
)

# Start the export task
task.start()

def check_task_status(task):
    status = task.status()['state']
    print(f'Current task status: {status}')
    return status

# Monitor task with timeout
timeout = 600  # 10 minutes timeout
start_time = time.time()
while True:
    status = check_task_status(task)
    if status in ['COMPLETED', 'FAILED', 'CANCELLED']:
        print(f'Task finished with status: {status}')
        break
    if time.time() - start_time > timeout:
        print('Task monitoring timed out. Check status in the Earth Engine Code Editor')
        break
    time.sleep(10)  # Check every 10 seconds

print('Monitor your task at: https://code.earthengine.google.com/tasks')