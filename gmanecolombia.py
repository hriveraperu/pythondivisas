import sqlite3
import sys
import ssl
import urllib.request, urllib.parse, urllib.error
import json

def parsemaildate(md) :
    md = md[0:10]
    return md

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('colombia.sqlite')
cur = conn.cursor()

baseurl = "https://www.datos.gov.co/resource/mcec-87by.json"
# baseurl = "http://mbox.dr-chuck.net/sakai.devel/"

cur.execute('''DROP TABLE IF EXISTS Currency''')
cur.execute('''CREATE TABLE Currency
    (id INTEGER PRIMARY KEY, country TEXT, unit TEXT, value DECIMAL,
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
country = "Colombia"
unit = "COP (/100)"
value = None
initDate = None

index = 0

for index, period in enumerate(js):

    if len(js[index]["unidad"]) > 0:
        unit = js[index]["unidad"]

    if len(js[index]["valor"]) > 0:
        value = js[index]["valor"]

    if len(js[index]["vigenciadesde"]) > 0:
        initDate = js[index]["vigenciadesde"]
    if initDate[0:4] == "2019" or initDate[0:4] == "2024":
        continue  # Skip this row if value is "n.d."
    initDate = parsemaildate(initDate)
    value = round(float(value)/100, 4)
    print("   ", value, initDate)
    cur.execute('''INSERT OR IGNORE INTO Currency (id, country, unit, value, initDate)
        VALUES ( ?, ?, ?, ?, ? )''', (index + 1, country, unit, value, initDate))
# if count % 50 == 0 : conn.commit()
# if count % 100 == 0 : time.sleep(1)

conn.commit()
cur.close()
