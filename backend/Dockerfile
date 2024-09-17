# Use an official Python runtime (based on slim Debian) as a parent image
FROM python:3.11-slim

# Set environment variables for Python and FastAPI
ENV PYTHONUNBUFFERED=1
ENV FASTAPI_PORT=3003

# Install system dependencies and clean up
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the PYTHONPATH to ensure Python can locate modules in /app
ENV PYTHONPATH=/app

# Expose the port for FastAPI
EXPOSE $FASTAPI_PORT

# Command to run FastAPI server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3003", "--reload"]
