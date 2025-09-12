import mysql.connector
import time

# Connect to MySQL
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Sravanipeddi',
    database='python_wine'
)
if conn:
    print("✅ Connection successful")

CHECK_INTERVAL = 5          # seconds between checks
SULPHUR_TARGET = 9          # total_sulfur_dioxide threshold
ALCOHOL_TARGET = 20         # alcohol threshold

def main():
    cursor = conn.cursor(dictionary=True)

    # Find the highest current Id so we ignore older rows
    cursor.execute("SELECT COALESCE(MAX(Id), 0) AS max_id FROM wine_data")
    last_id = cursor.fetchone()["max_id"]

    print(f"🚀 Wine alert service started... (Watching for Id > {last_id})")

    while True:
        cursor.execute("SELECT * FROM wine_data WHERE Id > %s", (last_id,))
        new_rows = cursor.fetchall()

        for row in new_rows:
            print(f"🆕 New row detected! Id={row['Id']}, Data={row}")
            # If you want to filter by SULPHUR_TARGET and ALCOHOL_TARGET, uncomment below:
            # if (row["total_sulfur_dioxide"] == SULPHUR_TARGET and
            #     row["alcohol"] == ALCOHOL_TARGET):
            #     print(f"⚠️ ALERT: New matching wine inserted! Id={row['Id']}, Data={row}")

            # Update checkpoint to the highest Id seen so far
            last_id = max(last_id, row["Id"])

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()