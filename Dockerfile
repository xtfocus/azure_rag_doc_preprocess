FROM python:3.11

WORKDIR /app

# Install LibreOffice (for Debian-based images)
RUN apt-get update && apt-get install -y libreoffice-core libreoffice-writer && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /app

EXPOSE 3100

ENTRYPOINT ["gunicorn", "-c", "src/gunicorn.conf.py", "src:create_app()"]
# Use Gunicorn with reload mode for hot reloading
# CMD ["gunicorn", "-c", "src/gunicorn.conf.py", "--reload", "src:create_app()"]
