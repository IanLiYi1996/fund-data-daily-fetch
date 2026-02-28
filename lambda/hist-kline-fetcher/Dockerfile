FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    gcc g++ make curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /var/task

RUN pip install --no-cache-dir awslambdaric

COPY hist-kline-fetcher/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY hist-kline-fetcher/handler.py .

ENTRYPOINT ["python", "-m", "awslambdaric"]
CMD ["handler.lambda_handler"]
