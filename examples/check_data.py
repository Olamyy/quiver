import psycopg2

conn = psycopg2.connect(
    host="127.0.0.1",
    user="quiver",
    password="quiver_test",
    database="quiver_test"
)
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM ecommerce_data")
print(f"Total records: {cursor.fetchone()[0]}")

cursor.execute("SELECT entity, spend_30d, feature_ts FROM ecommerce_data LIMIT 5")
print("\nFirst 5 records:")
for row in cursor.fetchall():
    print(f"  {row}")

cursor.execute("SELECT entity FROM ecommerce_data WHERE entity IN ('user:101', 'user:102', 'user:103')")
print(f"\nRecords for user:101/102/103: {len(cursor.fetchall())}")

cursor.close()
conn.close()
