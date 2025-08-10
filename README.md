# ASR Application

This project is an Automated Speech Recognition (ASR) system that handles audio recordings, processes them, and updates a database using Flask and Celery.

## Setup Instructions

### 1️⃣ Create and Activate Virtual Environment
```sh
python3 -m venv asrenv
source asrenv/bin/activate  # On macOS/Linux
asrenv\Scripts\activate  # On Windows
```

### 2️⃣ Install Dependencies
```sh
pip install -r Requirements.txt
```

### 3️⃣ Start Redis Server
```sh
sudo systemctl start redis
```
Check if Redis is running:
```sh
redis-cli ping  # Should return 'PONG'
```

### 4️⃣ Run Flask App
```sh
nohup python3 web_aud.py > output.log 2>&1 &
```

### 5️⃣ Start Celery Worker
```sh
celery -A celery_worker worker --loglevel=info
```

### 6️⃣ Stop Celery Worker
To gracefully stop Celery:
```sh
pkill -f "celery"
```
Or using Celery control:
```sh
celery -A celery_worker control shutdown
```

## Project Structure
```
├── asrenv/                 # Virtual environment
├── uploads/                # Directory for storing uploaded audio files
├── web_aud.py              # Flask application
├── celery_worker.py        # Celery worker file
├── db.py                   # PostgreSQL connection module
├── requirements.txt        # Required dependencies
├── README.md               # Project documentation
```

## Troubleshooting
- If Redis is not running, restart it:
  ```sh
  sudo systemctl restart redis
  ```
- If Celery tasks are not running, ensure Redis is working and restart Celery:
  ```sh
  nohup celery -A celery_worker worker --loglevel=info --concurrency=10 > celery.log 2>&1 &
  #celery -A celery_worker worker --loglevel=info --pool=solo
  ```