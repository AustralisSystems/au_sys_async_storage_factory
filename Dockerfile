FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY . /app/

RUN pip install --upgrade pip && \
    pip install . uvicorn[standard]>=0.30.0

EXPOSE 8082

CMD ["uvicorn", "au_sys_storage.app:app", "--host", "0.0.0.0", "--port", "8082"]
