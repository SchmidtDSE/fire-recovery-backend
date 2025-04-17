#### Testing

```bash
curl -X 'POST' \
  'http://localhost:8000/process/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "geometry": {
    "type": "Polygon",
    "coordinates": [
      [
        [-116.07846322825276, 33.92840733251654],
        [-116.07846322825276, 33.87821772335607],
        [-116.01143420678488, 33.87821772335607],
        [-116.01143420678488, 33.92840733251654],
        [-116.07846322825276, 33.92840733251654]
      ]
    ]
  },
  "prefire_date_range": ["2023-05-01", "2023-06-01"],
  "posfire_date_range": ["2023-07-01", "2023-08-01"]
}'
```