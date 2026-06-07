FROM python:3.12-slim

WORKDIR /app

COPY main.py .
COPY gateway/ gateway/

RUN pip install --no-cache-dir fastapi uvicorn websockets

EXPOSE 8001

CMD ["python", "main.py"]
