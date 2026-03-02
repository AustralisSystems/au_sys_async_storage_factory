FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir mcp motor beanie pydantic-settings

COPY . .

CMD ["python", "mcp_server.py"]
