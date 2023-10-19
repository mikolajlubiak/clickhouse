
import pandas as pd
from clickhouse_driver import Client

# Read the CSV data
df = pd.read_csv('data.csv')

# Group the data by 'CLIENT' and join the 'PRODUCT' values into a list
df_grouped = df.groupby('CLIENT')['PRODUCT'].apply(list).reset_index(name='ALL_PRODUCTS')

# Connect to Clickhouse
client = Client('localhost')

# Create a table in Clickhouse
client.execute('CREATE TABLE IF NOT EXISTS products (ID Int32, ALL_PRODUCTS Array(String)) ENGINE = Memory')

# Insert the data into the table
for index, row in df_grouped.iterrows():
    client.execute('INSERT INTO products VALUES', [(row['CLIENT'], row['ALL_PRODUCTS'])])

# Fetch the data from the table
data = client.execute('SELECT ALL_PRODUCTS FROM products')

# Convert the data into a list of lists
sequences = [list(row[0]) for row in data]

# PrefixSpan implementation
def prefix_span(pattern, S):
    count = sum(1 for seq in S if is_subsequence(pattern, seq))
    if count >= min_support:
        frequent_patterns.append((pattern, count))
    for item in set(x for sublist in S for x in sublist):
        pattern_ = pattern + [item]
        S_ = [s[s.index(item)+1:] for s in S if is_subsequence(pattern_, s)]
        prefix_span(pattern_, S_)

def is_subsequence(pattern, sequence):
    if not pattern:
        return True
    if not sequence:
        return False
    if pattern[0] == sequence[0]:
        return is_subsequence(pattern[1:], sequence[1:])
    return is_subsequence(pattern, sequence[1:])

# Set the minimum support and find the frequent patterns
min_support = 2
frequent_patterns = []
prefix_span([], sequences)
print(frequent_patterns)
