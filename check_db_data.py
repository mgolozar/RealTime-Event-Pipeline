#!/usr/bin/env python3
"""Quick script to check database data"""

from src.config import Config
from src.storage.database import DatabaseManager

db = DatabaseManager(
    host=Config.DB_HOST, port=Config.DB_PORT, database=Config.DB_NAME,
    user=Config.DB_USER, password=Config.DB_PASSWORD
)

print("Database table counts:")
counts = db.get_table_counts()
for table, count in counts.items():
    print(f"  {table}: {count} rows")

db.close()

