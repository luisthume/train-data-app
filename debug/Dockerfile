FROM python:3.9-slim

WORKDIR /app

RUN pip install confluent_kafka

COPY debug_consumer.py .

CMD ["python", "debug_consumer.py"]
