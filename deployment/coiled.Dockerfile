# Using pixi image as a base
FROM ghcr.io/prefix-dev/pixi:0.45.0-bullseye-slim

# Copy pixi environment files
COPY pixi.toml pixi.toml
COPY pixi.lock pixi.lock

# Copy the entire project (not just src)
COPY . .

# Configure pixi as our entrypoint
RUN pixi install --locked
RUN pixi shell-hook -s bash > /shell-hook
RUN echo "#!/bin/bash" > /entrypoint.sh
RUN cat /shell-hook >> /entrypoint.sh
RUN echo 'exec "$@"' >> /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose the FastAPI port
EXPOSE 8000

# Set entrypoint to use pixi
ENTRYPOINT ["/entrypoint.sh"]
