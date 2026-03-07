from quiver import Client
import pyarrow as pa

client = Client("localhost:8815")
result = client.get_features("ecommerce_features", ["user:101", "user:102"], ["spend_30d", "purchase_count"])

print("Schema:")
print(result.schema)
print("\nRaw Arrow Table:")
print(result._table)

print("\nColumn 0 (entity):")
print(result._table.column(0))

print("\nColumn 1 (spend_30d):")
print(result._table.column(1))

print("\nColumn 2 (purchase_count):")
print(result._table.column(2))

print("\nAs pandas:")
print(result.to_pandas())
