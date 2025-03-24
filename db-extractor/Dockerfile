# Use Python 3.9 for better performance and longer support
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Install necessary dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY src /app/src

# Set the entry point
CMD ["python", "src/main.py"]
