import sqlite3
from datetime import datetime

# Function to normalize date format to "yyyy-mm-dd"
from datetime import datetime

from datetime import datetime

# Connect to the index.sqlite database (or create it if it doesn't exist)
conn_index = sqlite3.connect('index.sqlite')
cur_index = conn_index.cursor()

# Create the ExchangeRates table in index.sqlite if it doesn't exist
cur_index.execute('''DROP TABLE IF EXISTS ExchangeRates''')
cur_index.execute('''
    CREATE TABLE ExchangeRates (
        id INTEGER PRIMARY KEY,
        country TEXT,
        unit TEXT,
        value REAL,
        initDate TEXT
    )
''')

# List of database filenames
database_files = ['argentina.sqlite', 'brasil.sqlite', 'chile.sqlite', 'colombia.sqlite', 'peru.sqlite']

# Dictionary to store exchange rates data for each country
exchange_rates_data = {}

# Iterate over each database
for db_file in database_files:
    print("Processing database:", db_file)
    # Connect to the current database
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()

    # Fetch information from the database table
    cur.execute('''SELECT country, unit, value, initDate FROM Currency''')

    # Process the fetched data
    for row in cur.fetchall():
        country, unit, value, initDate = row
        
        normalized_date = initDate
        if normalized_date:
            # Store the data in the dictionary using the country name as the key
            if country not in exchange_rates_data:
                exchange_rates_data[country] = []
            exchange_rates_data[country].append((country, unit, value, normalized_date))
        else:
            print("Could not parse date:", initDate)

    # Close the current database connection
    conn.close()

# Insert aggregated data into the ExchangeRates table in index.sqlite
for country, data in exchange_rates_data.items():
    cur_index.executemany('''INSERT INTO ExchangeRates (country, unit, value, initDate)
                             VALUES (?, ?, ?, ?)''', data)

# Commit changes and close the index.sqlite database connection
conn_index.commit()
conn_index.close()
