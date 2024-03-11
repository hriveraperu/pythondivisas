import sqlite3
import time
import ssl
import urllib.request, urllib.parse, urllib.error
from urllib.parse import urljoin
from urllib.parse import urlparse
import re
from datetime import datetime, timedelta
import json

def parsemaildate(md) :
    md = md[0:10]
    return md


# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('argentina.sqlite')
cur = conn.cursor()

baseurl = "https://api.argentinadatos.com/v1/cotizaciones/dolares/blue/"
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

country = "Argentina"
unit = "ARS"
value = None
initDate = None

index = 0

for index, period in enumerate(js):
    if js[index]["compra"] > 0:
        value = js[index]["compra"]

    if len(js[index]["fecha"]) > 0:
        initDate = js[index]["fecha"]

    if int(initDate[0:4]) <= 2019 or int(initDate[0:4]) == 2024:
        continue  # Skip this row if value is "n.d."
    initDate = parsemaildate(initDate)
    value = round(float(value), 3)
    print("   ", value, initDate)
    cur.execute('''INSERT OR IGNORE INTO Currency (id, country, unit, value, initDate)
        VALUES ( ?, ?, ?, ?, ? )''', (index + 1, country, unit, value, initDate))
    # if count % 50 == 0 : conn.commit()
    # if count % 100 == 0 : time.sleep(1)

conn.commit()
cur.close()
