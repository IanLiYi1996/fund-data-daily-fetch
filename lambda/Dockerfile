FROM python:3.11-slim

# Install system dependencies and AWS Lambda RIC
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /var/task

# Install AWS Lambda Runtime Interface Client
RUN pip install --no-cache-dir awslambdaric

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ .

# Set the entrypoint to AWS Lambda RIC
ENTRYPOINT ["python", "-m", "awslambdaric"]

# Set the handler
CMD ["handler.lambda_handler"]
