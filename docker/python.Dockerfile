# One image for the generator, the scoring consumer, the API and training.
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY libs libs
COPY services services
COPY ml ml
COPY tools tools
COPY models models
ENV PYTHONPATH=/app/libs:/app PYTHONUNBUFFERED=1
RUN useradd --create-home app && chown -R app /app
USER app
