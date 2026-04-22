FROM python:3.11-slim

# libzim PyPI wheel is self-contained (bundles libzim + libicu).
# No system-level apt deps needed for x86_64 / aarch64 glibc images.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN pip install --no-cache-dir -e .

# Volumes expected at runtime:
#   /data/input   — place ZIM file here
#   /data/build   — pipeline writes all outputs here
VOLUME ["/data/input", "/data/build"]

ENTRYPOINT ["monolith-wiki"]
CMD ["--help"]
