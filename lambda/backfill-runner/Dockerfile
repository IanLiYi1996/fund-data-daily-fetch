FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY backfill-runner/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# shared/ holds schemas, storage, utils — same package layout the
# fund-fetcher Lambda uses, imported via PYTHONPATH=/app.
COPY shared/ /app/shared/
COPY backfill-runner/entrypoint.sh /app/entrypoint.sh
COPY backfill-runner/backfill_fund_history.py /app/backfill_fund_history.py

RUN chmod +x /app/entrypoint.sh

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/entrypoint.sh"]
