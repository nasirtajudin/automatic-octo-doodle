# Use Microsoft's official Playwright Python image
FROM mcr.microsoft.com/playwright/python:v1.41.0-jammy

# Set the working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN playwright install chromium

# Copy the application code
COPY . .

# Run the application
CMD ["python", "main.py"]
