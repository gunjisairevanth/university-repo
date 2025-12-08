# # Astro
# FROM astrocrpublic.azurecr.io/runtime:3.1-3

# Agent
FROM images.astronomer.cloud/baseimages/astro-remote-execution-agent:3.0-5-python-3.12-astro-agent-1.0.3
USER root
COPY requirements.txt .
RUN pip install --no-cache-dir --requirement requirements.txt
USER astro