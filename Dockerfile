# Use Python 3.12 (Slim Bookworm - smallest modern base)
FROM python:3.12-slim-bookworm

# Set working directory
WORKDIR /app

# One-liner to install GCC, build tgcrypto, and then delete GCC instantly
COPY requirements.txt .
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt && \
    apt-get purge -y --auto-remove gcc python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Forces Python to print logs immediately
ENV PYTHONUNBUFFERED=1

# Copy the rest of your files
COPY . .

# Run the bot
CMD ["python3", "restrict_bot.py"]
