from fastapi import Request
import hashlib
import json


# Custom request hash function for POST requests
async def request_key_builder(
    func, namespace: str = "", request: Request = None, response=None, *args, **kwargs
):
    # For POST requests, include the body in the cache key
    if request.method == "POST":
        body = await request.body()
        key_dict = {
            "namespace": namespace,
            "function": func.__name__,
            "request": {
                "method": request.method,
                "path": str(request.url.path),
                "body": hashlib.md5(body).hexdigest(),
            },
        }
        return json.dumps(key_dict)

    # For other methods, just use the default implementation
    return f"{namespace}:{func.__name__}:{str(args)}:{str(kwargs)}"
