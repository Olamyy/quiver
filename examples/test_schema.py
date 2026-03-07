from quiver import Client

client = Client("localhost:8815")
result = client.get_features("ecommerce_features", ["user:101", "user:102"], ["spend_30d", "is_premium"])

print("Schema:", result.schema)
print("\nColumn names:", result.column_names)
print("\nData:")
print(result.to_pandas())
print("\nRaw Arrow table:")
print(result._table)
