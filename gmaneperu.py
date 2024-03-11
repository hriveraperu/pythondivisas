import sqlite3
import ssl
import urllib.request, urllib.parse, urllib.error
from urllib.parse import urljoin
from urllib.parse import urlparse
import re
from datetime import datetime, timedelta
import json

def parsemaildate(md):
    months = {
        'Ene': '01', 'Feb': '02', 'Mar': '03', 'Abr': '04',
        'May': '05', 'Jun': '06', 'Jul': '07', 'Ago': '08',
        'Set': '09', 'Oct': '10', 'Nov': '11', 'Dic': '12'
    }
    pieces = md.split('.')
    day = pieces[0]
    month_str = pieces[1]
    year = pieces[2]

    # Convert month abbreviation to month number
    month = months.get(month_str)
    if not month:
        return None

    # Pad day and year with zeros if needed
    day = day.zfill(2)
    year = year.zfill(2)
    if int(year) < 50:
        year = '20' + year
    else:
        year = '19' + year

    # Construct the date string in yyyy-mm-dd format
    date_str = f"{year}-{month}-{day}"

    return date_str


# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('peru.sqlite')
cur = conn.cursor()

baseurl = "https://estadisticas.bcrp.gob.pe/estadisticas/series/api/PD04637PD-PD04638PD/json/2020-01-01/2023-12-31/"
# baseurl = "http://mbox.dr-chuck.net/sakai.devel/"

cur.execute('''CREATE TABLE IF NOT EXISTS Currency
    (id INTEGER UNIQUE, country TEXT, unit TEXT, value DECIMAL,
    initDate DATETIME)''')

url = baseurl
text = "None"
try:
    # Open with a timeout of 30 seconds
    document = urllib.request.urlopen(url, None, 30, context=ctx)
    text = document.read().decode()
    js = json.loads(text)
    if document.getcode() != 200 :
        print("Error code=",document.getcode(), url)
        sys.exit(1)
except KeyboardInterrupt:
    print('')
    print('Program interrupted by user...')
    sys.exit(1)
except Exception as e:
    print("Unable to retrieve or parse page",url)
    print("Error",e)
    sys.exit(1)
country = "Peru"
unit = "PEN"
value = None
initDate = None

index = 0

for index, period in enumerate(js["periods"]):
    value = period["values"][1] if len(period["values"]) > 1 else None
    if value == "n.d.":
        continue  # Skip this row if value is "n.d."
    initDate = parsemaildate(period["name"])
    value = round(float(value), 3)
    print("   ", value, initDate)
    cur.execute('''INSERT OR IGNORE INTO Currency (id, country, unit, value, initDate)
        VALUES ( ?, ?, ?, ?, ? )''', (index + 1, country, unit, value, initDate))

conn.commit()
cur.close()
