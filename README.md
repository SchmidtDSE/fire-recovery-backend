# Fire Recovery Backend API

A FastAPI-based service that performs fire severity calculations, boundary refinement, and vegetation impact analysis for fire recovery assessment applications.

## Overview

The Fire Recovery Backend API serves as the computational engine for a fire recovery assessment application. It processes satellite imagery to calculate fire severity metrics, enables boundary refinement based on those metrics, and generates vegetation impact statistics. The API integrates with STAC (SpatioTemporal Asset Catalog) endpoints for satellite data access and provides Cloud Optimized GeoTIFF (COG) outputs for efficient web visualization.

## API Responsibilities

### 1. Fire Severity Analysis
- **Endpoint**: `POST /fire-recovery/process/analyze_fire_severity`
- **Function**: Processes satellite imagery to calculate multiple fire severity indices
- **Input**: Approximate boundary polygon, fire event name, pre/post-fire date ranges
- **Processing**: 
  - Queries STAC endpoints (Sentinel-2 data via Planetary Computer or Earth Engine)
  - Calculates spectral indices: NBR (Normalized Burn Ratio), dNBR, RdNBR, RBR
  - Creates Cloud Optimized GeoTIFFs for each metric
  - Stores results in Google Cloud Storage with STAC metadata
- **Output**: URLs to fire severity COGs for visualization in the frontend

### 2. Boundary Refinement
- **Endpoint**: `POST /fire-recovery/process/refine`
- **Function**: Refines fire boundaries based on user-drawn polygons over severity data
- **Input**: Refined boundary geometry, original job ID
- **Processing**:
  - Downloads original fire severity COGs
  - Crops COGs to refined boundary using exact geometry
  - Creates new COGs with refined extents
  - Updates STAC catalog with refined assets
- **Output**: URLs to boundary-cropped fire severity COGs and boundary GeoJSON

### 3. Vegetation Impact Analysis  
- **Endpoint**: `POST /fire-recovery/process/resolve_against_veg_map`
- **Function**: Generates statistical analysis of fire impacts on vegetation communities
- **Input**: Vegetation map (GeoPackage), fire severity COG, severity classification breaks
- **Processing**:
  - Downloads and processes vegetation map data
  - Performs zonal statistics to calculate area impacts by vegetation type and severity class
  - Generates comprehensive statistics (hectares affected, percentages, mean severity values)
- **Output**: CSV and JSON files containing vegetation impact matrices

### 4. File Upload Services
- **GeoJSON Upload**: `POST /fire-recovery/upload/geojson` - Direct upload of boundary files
- **Shapefile Upload**: `POST /fire-recovery/upload/shapefile` - Upload zipped shapefiles for boundary definition

### 5. Result Retrieval
- **Fire Severity Results**: `GET /fire-recovery/result/analyze_fire_severity/{fire_event_name}/{job_id}`
- **Boundary Refinement Results**: `GET /fire-recovery/result/refine/{fire_event_name}/{job_id}`  
- **Vegetation Analysis Results**: `GET /fire-recovery/result/resolve_against_veg_map/{fire_event_name}/{job_id}`

## User Workflows

### Standard Workflow
1. **Initial Analysis**: User uploads approximate boundary → API generates fire severity COGs
2. **Boundary Refinement**: User draws refined boundary over severity visualization → API crops COGs to refined area
3. **Impact Assessment**: User requests vegetation analysis → API generates statistical reports

### Direct Upload Workflow
1. **Shapefile Upload**: User uploads precise boundary shapefile → API stores boundary
2. **Impact Assessment**: User requests vegetation analysis using uploaded boundary

## Generated Assets

### Cloud Optimized GeoTIFFs (COGs)
- **Pre-fire NBR**: Baseline vegetation health index
- **Post-fire NBR**: Post-fire vegetation condition 
- **dNBR (Delta NBR)**: Simple difference showing burn severity
- **RdNBR (Relative dNBR)**: Relativized burn severity accounting for pre-fire conditions
- **RBR (Relativized Burn Ratio)**: Alternative relativized severity metric

### Boundary Files
- **Coarse Boundary GeoJSON**: Initial user-provided boundary
- **Refined Boundary GeoJSON**: User-refined boundary based on severity visualization
- **Shapefile Archives**: Direct uploads of boundary shapefiles (.zip format)

### Statistical Reports
- **Vegetation Impact CSV**: Tabular data showing hectares affected by vegetation type and severity class
- **Vegetation Impact JSON**: Structured data for web visualization including:
  - Vegetation community names and colors
  - Total hectares and percentages
  - Severity breakdown by class
  - Statistical measures (mean, standard deviation)

### STAC Metadata
- **Fire Severity Items**: Catalog entries linking to severity COGs with spatial/temporal metadata
- **Boundary Items**: Catalog entries for boundary files with classification metadata  
- **Vegetation Matrix Items**: Catalog entries for statistical analysis results

## Integration Points

### Frontend Application Touchpoints
- **Map Visualization**: Serves COG tiles for interactive fire severity mapping
- **Boundary Drawing**: Accepts refined polygon geometries from map interface
- **Data Tables**: Provides CSV/JSON data for vegetation impact visualization
- **Progress Tracking**: Returns job status and completion notifications

### External Data Sources
- **Microsoft Planetary Computer**: Primary STAC endpoint for Sentinel-2 imagery
- **National Park Service**: Vegetation cover class datasets

### Storage Systems  
- **Google Cloud Storage**: Primary storage for all generated assets
- **Temporary Storage**: Local processing workspace with automatic cleanup
- **STAC Catalog**: GeoParquet-based metadata storage for asset discovery

## Development Setup

### .env Configuration
Expected `.env` file content for local development (place in `.devcontainer` folder):

```dotenv
# S3-compatible storage credentials
S3_ACCESS_KEY_ID=[YOUR ACCESS KEY]
S3_SECRET_ACCESS_KEY=[YOUR SECRET KEY]
RUN_LOCAL=True
```

For testing, we also expect a `test.env`:
```dotenv
# S3-compatible storage configuration
S3_ENDPOINT=storage.googleapis.com
S3_ACCESS_KEY_ID=[YOUR ACCESS KEY]
S3_SECRET_ACCESS_KEY=[YOUR SECRET KEY]
S3_SECURE=True
S3_BUCKET=fire-recovery-temp
```