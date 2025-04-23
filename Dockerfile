# Use slim Python image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy everything into the container
COPY . .

# Install system dependencies (for psycopg2 or sqlite if needed)
RUN apt-get update && apt-get install -y build-essential libsqlite3-dev

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port (Flask default is 5000, but change if needed)
EXPOSE 5002

# Run the app
CMD ["python", "-m", "portal.run"]
