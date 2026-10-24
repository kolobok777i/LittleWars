FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY room_server.py .

EXPOSE 8765

CMD ["python", "room_server.py"]
