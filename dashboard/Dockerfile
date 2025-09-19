
FROM python:3.12-slim

WORKDIR /app

#Wordcloud dependencies 
RUN apt-get update && apt-get install -y \
      gcc \
      g++ \
      && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get remove -y gcc g++ && apt-get autoremove -y

COPY . .
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address", "0.0.0.0", "--server.port", "8501"]