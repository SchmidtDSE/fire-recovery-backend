# STAC Test Assets for MinIO

This directory contains example STAC catalog files and assets for testing. These were uploaded manually to the GCP bucket `fire-recovery-temp` and are used for testing. 

## Directory Structure

Upload to your MinIO bucket at `https://test-temp-bucket.storage.googleapis.com/`:

```
/
├── catalog.json                                    # Root STAC catalog
├── collections/
│   ├── fire-severity/
│   │   └── collection.json                         # Fire severity collection
│   ├── fire-boundaries/  
│   │   └── collection.json                         # Fire boundaries collection
│   └── vegetation-matrices/
│       └── collection.json                         # Vegetation matrices collection
└── assets/
    ├── test_fire-boundary.geojson                  # Sample boundary GeoJSON
    ├── test_fire-veg-matrix.csv                    # Sample vegetation matrix CSV
    ├── test_fire-veg-matrix.json                   # Sample vegetation matrix JSON
    ├── test_fire-rbr.tif                           # RBR COG (you need to provide)
    ├── test_fire-dnbr.tif                          # dNBR COG (you need to provide) 
    └── test_fire-rdnbr.tif                         # RdNBR COG (you need to provide)
```
