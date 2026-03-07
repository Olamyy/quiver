import psycopg2

conn = psycopg2.connect(
    host="127.0.0.1",
    user="quiver",
    password="quiver_test",
    database="quiver_test"
)
cursor = conn.cursor()

# Check seed data (user:101, 102, 103)
cursor.execute("SELECT entity, spend_30d FROM ecommerce_data WHERE entity IN ('user:101', 'user:102', 'user:103')")
print("Seed records:")
for row in cursor.fetchall():
    print(f"  {row}")

# Check generated data range
cursor.execute("SELECT MIN(entity), MAX(entity) FROM ecommerce_data WHERE entity LIKE 'user:%'")
print(f"\nEntity range: {cursor.fetchone()}")

cursor.close()
conn.close()
