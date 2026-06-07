FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
COPY main.py .
COPY gateway/ gateway/

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8001

CMD ["python", "main.py"]
