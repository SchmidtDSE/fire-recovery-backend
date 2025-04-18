#### Testing

```bash
curl -X POST "http://localhost:8000/process-test/" \
  -H "Content-Type: application/json" \
  -d '{"geometry": {"type": "Polygon", "coordinates": [[[-120.0, 38.0], [-120.0, 39.0], [-119.0, 39.0], [-119.0, 38.0], [-120.0, 38.0]]]}, "prefire_date_range": ["2023-01-01", "2023-06-30"], "posfire_date_range": ["2023-07-01", "2023-12-31"]}'
```