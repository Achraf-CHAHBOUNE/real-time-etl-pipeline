# Use a Python base image
FROM python:3.8-slim

# Set the working directory
WORKDIR /app

# Install necessary dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the application code
COPY src /app/src

# Set the entry point
CMD ["python", "src/main.py"]
