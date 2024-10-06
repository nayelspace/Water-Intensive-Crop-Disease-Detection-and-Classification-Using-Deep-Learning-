# Use an official Python runtime as a base image
FROM python:3.11-slim

# Install necessary tools
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the 'ai_gcp' directory contents into the container at /app
COPY ai_gcp/ /app/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable
ENV FLASK_APP app.py

# Optionally define a command for health checks
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# Run Gunicorn to serve the Flask app
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]