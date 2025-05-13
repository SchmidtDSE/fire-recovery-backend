#### Testing

### Process endpoint (AOI method)

This endpoint uses an approximate AOI to get an 'intermediate burn COG', which is assumed to contain the 'real burn boundary' but needs to be further refined to fit the 'real boundary'. 

#### Inputs 

- 'geometry' - a `Polygon` with coorindates as defined below (validated by `geojson-pydantic`)
- 'prefire_date_range' - a list of two ISO format dates
- 'postfire_data_range'

### Output

- JSON response with `job_id`, triggering processing

### Example usage

```bash
curl -X POST "http://localhost:8000/process-test/" \
  -H "Content-Type: application/json" \
  -d '{"geometry": {"type": "Polygon", "coordinates": [[[-120.0, 38.0], [-120.0, 39.0], [-119.0, 39.0], [-119.0, 38.0], [-120.0, 38.0]]]}, "prefire_date_range": ["2023-01-01", "2023-06-30"], "postfire_date_range": ["2023-07-01", "2023-12-31"]}'
```

### Process endpoint (shapefile method)

This endpoint uses an 'intermediate' boundary, defined in a shapefile, to query the burn metrics within. The intermediate boundary is assumed to contain the 'real burn boundary' but needs to be further refined to fit the 'real boundary'. 


#### Inputs 

- 'shapefile' - a `Bytestream` of the shapefile contents
- 'prefire_date_range' - a list of two ISO format dates
- 'postfire_data_range'

### Output

- JSON response with `job_id`, triggering processing

### Result endpoint (polling strategy)

This endpoint simply tells us the status of a process job.

#### Inputs
- `job_id` to request status for

#### Outputs
- `JSON` of `status`, `job_id`, and when `status` == "complete", the `cog_url` for the intermediate burn COG

NOTE: Need to replace `JOB_ID` with the response from the original processing request (for now it doesn't actually matter since it's just a dummy test endpoint, but that is the eventual workflow)

```bash
curl -X GET "http://localhost:8000/result-test/JOB_ID"
```

With a valid `JOB_ID`:

```bash
curl -X GET "http://localhost:8000/result-test/8b6cf846-3a9b-43fa-a0c5-604e98b1d732"
```

### Refine endpoint

Inputs:
- a GeoJSON of the 'refined' area, drawn by the user

Outputs:
- a JSON of the `refined_cog_url`

NOTE: the original burn severity tool uses a 'seed' point (or optionally many seed points) to use image segmentation to seperate out 'real' boundary. Additionally, they could specify a 'restriction boundary' (another geojson) to restrict the derived boundary (as a maximum extent). This was a little bit inconsistent in performance, but may be preferable to forcing the user to refine the boundary themselves (though refining the boundary themselves leaves us less open to potential errors forced by segmentation).

### Minio

- endpoint getting data from STAC and saving it in a location to GCP to display the COG_URL on the map requires a secret and key for fire-recovery-backend in a .env file.

```
GCP_ACCESS_KEY_ID=[YourAccessKey]
GCP_SECRET_ACCESS_KEY=[YourSecret]
RUN_LOCAL=True
```
