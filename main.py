
import csv
from clickhouse_driver import Client

# Step 1: Connect to ClickHouse
client = Client(host='localhost', port=9000)
client.execute('USE prefixspan')

# Step 2: Load CSV data into ClickHouse and preprocess
with open('data.csv', 'r') as f:
    data = list(csv.reader(f))

# Step 2.1: Insert data into the 'data' table
insert_query = 'INSERT INTO data (CLIENT, INDEX, PRODUCT) VALUES'
insert_query += ' VALUES {}'.format(', '.join(['(%s, %s, %s)'] * len(data)))
client.execute(insert_query, data)

# Step 2.2: Preprocess the data and create 'preprocessed_data' table
preprocess_query = '''
    CREATE TABLE IF NOT EXISTS prefixspan.preprocessed_data AS
    SELECT
        CLIENT,
        groupArray(PRODUCT) AS ALL_PRODUCTS
    FROM data
    GROUP BY CLIENT
'''

client.execute(preprocess_query)

# Step 3: Implement PrefixSpan using ClickHouse SQL
prefixspan_query = '''
    WITH RECURSIVE
        prefixes AS (
            SELECT
                arrayJoin(ALL_PRODUCTS) AS prefix,
                arraySlice(ALL_PRODUCTS, arrayPosition(ALL_PRODUCTS, prefix) + 1) AS suffix
            FROM preprocessed_data
        ),
        frequent_prefixes AS (
            SELECT
                prefix,
                count() AS freq
            FROM prefixes
            GROUP BY prefix
            HAVING freq >= 2
        )
    SELECT
        prefix,
        suffix,
        freq
    FROM prefixes
    JOIN frequent_prefixes USING prefix
'''

# Step 4: Query the PrefixSpan results using the clickhouse-driver
results = client.execute(prefixspan_query)

for row in results:
    prefix = row[0]
    suffix = row[1]
    freq = row[2]
    print(f"Prefix: {prefix}, Suffix: {suffix}, Frequency: {freq}")
