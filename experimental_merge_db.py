import sqlite3
import os

# Paths to your databases
main_db_path = "indafoto.db"
other_db_path = "indafoto_other.db"

# Safety check
if not os.path.exists(main_db_path) or not os.path.exists(other_db_path):
    print("One or both database files are missing.")
    exit(1)

# Connect to the main database
conn = sqlite3.connect(main_db_path)
cursor = conn.cursor()

# Attach the secondary database
cursor.execute(f"ATTACH DATABASE '{other_db_path}' AS other_db")

# Merge the data, avoiding duplicates
cursor.execute("""
    INSERT OR IGNORE INTO images
    SELECT * FROM other_db.images
""")

# Commit changes and close
conn.commit()
cursor.execute("DETACH DATABASE other_db")
conn.close()

print("Merge completed successfully.")