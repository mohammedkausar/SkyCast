import os
from dotenv import load_dotenv

load_dotenv()


def get_config():
    required = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    for key in required:
        if key not in os.environ:
            raise ValueError(f"Missing environment variable: {key}")

    return {
        "host": os.environ["DB_HOST"],
        "port": int(os.environ["DB_PORT"]),
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
    }
