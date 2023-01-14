FROM python:3.10
RUN apt-get update && apt-get install -y default-mysql-client
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY . /app
WORKDIR /app
RUN mkdir -p /data
CMD ["python", "main.py"]