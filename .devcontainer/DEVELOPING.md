### CORS permissions for bucket access

Cors Config:
```
[
  {
    "origin": ["http://localhost:*", "http://127.0.0.1:*"],
    "responseHeader": ["Content-Type", "Content-Length", "ETag", "Access-Control-Allow-Origin"],
    "method": ["GET", "HEAD", "OPTIONS"],
    "maxAgeSeconds": 3600
  }
]
```

For GCP:

