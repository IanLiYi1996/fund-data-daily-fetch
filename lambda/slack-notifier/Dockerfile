FROM python:3.11-slim

WORKDIR /var/task

RUN pip install --no-cache-dir awslambdaric

COPY slack-notifier/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY slack-notifier/handler.py .

ENTRYPOINT ["python", "-m", "awslambdaric"]
CMD ["handler.lambda_handler"]
