FROM python:3.10.12-bookworm
RUN apt-get update && apt-get install -y default-mysql-client
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY . /app
RUN rm -rf /app/data
WORKDIR /app
RUN rm -rf .env
RUN mkdir -p /data
CMD ["python", "main.py"]