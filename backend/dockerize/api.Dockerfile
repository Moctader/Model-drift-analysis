FROM python:3.8

# SET WORKDIT & CLONE OVER NECESSARI FILES
COPY . /app
WORKDIR /app

# INSTALL DEPENDENCIES
RUN apt-get update
RUN pip install --no-cache-dir -r /app/requirements.txt

# RUN CONSUMER
CMD ["python", "main.py"]