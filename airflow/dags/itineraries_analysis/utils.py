import os

def get_file_path(filename):
    return os.path.join('/opt/airflow/data', filename)

def log_info(message):
    print(f"INFO: {message}")

def log_error(message):
    print(f"ERROR: {message}")
