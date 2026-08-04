FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    gcc g++ make curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /var/task

RUN pip install --no-cache-dir awslambdaric

COPY export-fund-history/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY export-fund-history/handler.py .

# Pre-install DuckDB extensions into an in-image cache: Lambda has no
# egress for extension downloads, so anything missing here fails at runtime.
# avro is needed to walk the Iceberg manifest list when resolving the live
# data-file set.
ENV DUCKDB_EXT_DIR=/opt/duckdb_ext
RUN mkdir -p ${DUCKDB_EXT_DIR} && \
    python -c "import duckdb, os; c=duckdb.connect(); \
    c.sql(f\"SET extension_directory='{os.environ['DUCKDB_EXT_DIR']}'\"); \
    c.sql('INSTALL httpfs'); c.sql('INSTALL aws'); c.sql('INSTALL avro')"

ENTRYPOINT ["python", "-m", "awslambdaric"]
CMD ["handler.lambda_handler"]
