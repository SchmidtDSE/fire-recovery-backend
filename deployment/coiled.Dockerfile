# Using pixi image as a base
FROM ghcr.io/prefix-dev/pixi:0.45.0-bullseye-slim

# Copy pixi environment files
COPY pixi.toml pixi.toml
COPY pixi.lock pixi.lock

# Copy src directory
COPY src src

# Configure pixi as our entrypoint (https://pixi.sh/dev/deployment/container/#example-usage)
RUN pixi install --locked
RUN pixi shell-hook -s bash > /shell-hook
RUN echo "#!/bin/bash" > /src/entrypoint.sh
RUN cat /shell-hook >> /src/entrypoint.sh
RUN echo 'exec "$@"' >> /src/entrypoint.sh
RUN chmod +x /src/entrypoint.sh

# Set entrypoint to use pixi
ENTRYPOINT [ "/src/entrypoint.sh" ]
