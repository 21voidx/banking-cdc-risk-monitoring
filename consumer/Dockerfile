FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY rules.py consumer.py ./
CMD ["python", "consumer.py"]
