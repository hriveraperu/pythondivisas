import sqlite3

# Function to validate the year input
def validate_year_input(year):
    try:
        year = int(year)
        if 2020 <= year <= 2023:
            return True
        else:
            print("Please enter a year between 2020 and 2023.")
            return False
    except ValueError:
        print("Invalid input. Please enter a valid year.")
        return False

# Prompt the user to enter the starting year
while True:
    start_year = input("Enter the starting year (2020 - 2023): ")
    if validate_year_input(start_year):
        start_year = int(start_year)
        break

# Prompt the user to enter the ending year
while True:
    end_year = input("Enter the ending year (2020 - 2023): ")
    if validate_year_input(end_year):
        end_year = int(end_year)
        break

# Connect to the database
conn = sqlite3.connect('index.sqlite')
cur = conn.cursor()

# Fetch data from the Currency table in index.sqlite
cur.execute('SELECT initDate, country, value FROM ExchangeRates WHERE initDate BETWEEN ? AND ?', (f'{start_year}-01-01', f'{end_year}-12-31'))
currency_data = cur.fetchall()

# Close the database connection
conn.close()

# Process the data to group by day and country
counts = dict()
days = set()
countries = set()
last_values = dict()  # Dictionary to store the last value for each country
for row in currency_data:
    initDate, country, value = row
    day = initDate[:10]  # Extracting the day part
    days.add(day)
    countries.add(country)

    # Update the last value for the country
    last_values[country] = value

    # Initialize the count for the day and country if not present
    counts.setdefault(day, {})
    # Use the last known value if the current value is missing
    counts[day][country] = value if value else last_values.get(country, 0)

# Fill missing values with values from previous days
previous_day = None
for day in sorted(days):
    if previous_day is not None:
        for country in countries:
            if country not in counts[day]:
                counts[day][country] = counts[previous_day].get(country, 0)
    previous_day = day

# Sort countries
countries = sorted(countries)

# Write data to gline.js
with open('gline.js', 'w') as fhand:
    fhand.write("gline = [ ['Day'")
    for country in countries:
        fhand.write(",'" + country + "'")
    fhand.write("]")

    for day in sorted(days):
        fhand.write(",\n['" + day + "'")
        for country in countries:
            val = counts[day].get(country, 0)
            fhand.write("," + str(val))
        fhand.write("]");

    fhand.write("\n];\n")

print("Output written to gline.js")
print("Open gline.htm to visualize the data")
