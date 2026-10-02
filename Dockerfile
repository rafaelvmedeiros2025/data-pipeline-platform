FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline ./pipeline
COPY samples ./samples
RUN useradd --create-home worker && mkdir /lake && chown worker /lake
USER worker
ENV SPARK_LOCAL_IP=127.0.0.1
ENTRYPOINT ["python", "-m", "pipeline.run"]
CMD ["--source", "samples/2026-01-01.jsonl", "--lake", "/lake", "--date", "2026-01-01"]
