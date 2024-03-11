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

conn = sqlite3.connect('chile.sqlite')
cur = conn.cursor()

baseurl = "https://mindicador.cl/api/dolar/" # You can upload more data changing the year
# baseurl = "http://mbox.dr-chuck.net/sakai.devel/"

cur.execute('''DROP TABLE IF EXISTS Currency''')
cur.execute('''CREATE TABLE Currency
    (id INTEGER PRIMARY KEY AUTOINCREMENT, country TEXT, unit TEXT, value DECIMAL,
    initDate DATETIME)''')

for year in range(2020, 2024):
    url = baseurl + str(year)
    print("Fetching data from:", url)  # Add this line


    text = "None"
    try:
        # Open with a timeout of 30 seconds
        document = urllib.request.urlopen(url, None, 30, context=ctx)
        text = document.read().decode()
        js = json.loads(text)
        print("Data length:", len(js["serie"]))  # Add this line
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

    # cur.execute('SELECT MAX(id) FROM Currency WHERE strftime("%Y", initDate) = ?', (str(year),))
    # max_id = cur.fetchone()[0]
    # start_index = max_id + 1 if max_id else 1

    country = "Chile"
    unit = "CLP (/10)"


    for period in js["serie"]:
        if period["valor"] > 0:
            value = round(float(period["valor"])/10, 3)
            initDate = parsemaildate(period["fecha"])
            print("Inserting data:", country, unit, value, initDate)
            cur.execute('''INSERT INTO Currency (country, unit, value, initDate)
                        VALUES (?, ?, ?, ?)''', (country, unit, value, initDate))
            

        # if count % 50 == 0 : conn.commit()
        # if count % 100 == 0 : time.sleep(1)

conn.commit()
cur.close()