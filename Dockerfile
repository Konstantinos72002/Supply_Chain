# Image of the Team 4 Supply Chain API (src/ilias_api_code), based on the instructor's Dockerfile.
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.32 /uv /uvx /bin/

WORKDIR /app

# Dependencies first (cached layer): exact versions from uv.lock, without the dev tools.
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project --no-cache

# Application code: the API, the knowledge graph package it imports and the graph file it reads.
# .dockerignore keeps .env, tests and caches out of the image.
COPY src/ilias_api_code ./src/ilias_api_code
COPY src/knowledge_graph ./src/knowledge_graph
COPY data/graph/supply_graph.json ./data/graph/supply_graph.json

ENV PATH="/app/.venv/bin:$PATH" PYTHONPATH="/app/src"
EXPOSE 8000

# Credentials come from the Container App settings at run time, never from the image.
CMD ["uvicorn", "supply_chain_api:app", "--app-dir", "src/ilias_api_code", "--host", "0.0.0.0", "--port", "8000"]