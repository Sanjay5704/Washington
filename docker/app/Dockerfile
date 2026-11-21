FROM python:3.11-slim-bookworm

RUN pip install --no-cache-dir "streamlit==1.45.1" "pandas==2.2.3" "altair==5.5.0" "psycopg2-binary==2.9.10"

WORKDIR /opt/washington
EXPOSE 8501
CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true", "--browser.gatherUsageStats=false"]
