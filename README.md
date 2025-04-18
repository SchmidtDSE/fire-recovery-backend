#### Testing

### Process endpoint

```bash
curl -X POST "http://localhost:8000/process-test/" \
  -H "Content-Type: application/json" \
  -d '{"geometry": {"type": "Polygon", "coordinates": [[[-120.0, 38.0], [-120.0, 39.0], [-119.0, 39.0], [-119.0, 38.0], [-120.0, 38.0]]]}, "prefire_date_range": ["2023-01-01", "2023-06-30"], "posfire_date_range": ["2023-07-01", "2023-12-31"]}'
```

### Result endpoint (polling strategy)
WW
NOTE: Need to replace `JOB_ID` with the response from the original processing request (for now it doesn't actually matter since it's just a dummy test endpoint, but that is the eventual workflow)

```bash
curl -X GET "http://localhost:8000/result-test/JOB_ID"
```

With a valid `JOB_ID`:

```bash
curl -X GET "http://localhost:8000/result-test/a319924a-ed09-42a3-b4ef-e9e5e34f8890"
```