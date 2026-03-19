import os
import urllib.parse
from dotenv import load_dotenv

load_dotenv()


def get_connection_string():
    """Create connection string."""
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    driver = os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server')

    if not all([server, database, user, password]):
        raise ValueError("Faltan variables en el .env")

    params = urllib.parse.quote_plus(
        f'DRIVER={{{driver}}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={user};'
        f'PWD={password};'
        'TrustServerCertificate=yes;'
    )
    return f"mssql+pyodbc:///?odbc_connect={params}"
