FROM python:3.8-slim-buster

WORKDIR /app

COPY main.py .

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["streamlit", "run", "main.py"]
