# Using pixi image as a base
FROM ghcr.io/prefix-dev/pixi:0.45.0-bullseye-slim

# Copy pixi environment files
COPY pixi.toml /app/pixi.toml
COPY pixi.lock /app/pixi.lock

# Configure pixi as our entrypoint (https://pixi.sh/dev/deployment/container/#example-usage)
RUN pixi install --locked -e prod
RUN pixi shell-hook -e prod -s bash > /shell-hook
RUN echo "#!/bin/bash" > /app/entrypoint.sh
RUN cat /shell-hook >> /app/entrypoint.sh
RUN echo 'exec "$@"' >> /app/entrypoint.sh

# Set entrypoint to use pixi
WORKDIR /src
ENTRYPOINT [ "/app/entrypoint.sh" ]
