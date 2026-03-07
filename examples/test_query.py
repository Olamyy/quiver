import psycopg2

conn = psycopg2.connect(
    host="127.0.0.1",
    user="quiver",
    password="quiver_test",
    database="quiver_test"
)
cursor = conn.cursor()

# Check table structure
cursor.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'ecommerce_data'
    ORDER BY ordinal_position
""")
print("Table structure:")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]}")

# Try the query that the adapter would generate
cursor.execute("""
    WITH requested_entities AS (
        SELECT unnest(%s::text[]) AS entity
    ),
    ranked_features AS (
        SELECT
            t.entity,
            t.spend_30d as feature_value,
            t.feature_ts,
            ROW_NUMBER() OVER (PARTITION BY t.entity ORDER BY t.feature_ts DESC) as rn
        FROM ecommerce_data t
        INNER JOIN requested_entities r ON t.entity = r.entity
        WHERE t.feature_ts <= %s
          AND t.spend_30d IS NOT NULL
    )
    SELECT entity, feature_value
    FROM ranked_features
    WHERE rn = 1
""", [['user:101', 'user:102'], '2026-03-07T19:50:00'])

print("\nQuery results:")
for row in cursor.fetchall():
    print(f"  {row}")

cursor.close()
conn.close()
