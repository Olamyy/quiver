from clickhouse_driver import Client

client = Client('localhost', port=9000, database='quiver_test')

# Check table structure
tables = client.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='quiver_test'")
print("Tables in quiver_test:")
for table in tables:
    print(f"  {table[0]}")

# Check data
result = client.execute("SELECT COUNT(*) FROM ecommerce_data")
print(f"\nTotal records in ecommerce_data: {result[0][0]}")

# Check first few rows
result = client.execute("SELECT * FROM ecommerce_data LIMIT 3")
print("\nFirst 3 rows:")
for row in result:
    print(f"  {row}")
