import sqlite3
import ssl
import urllib.request, urllib.parse, urllib.error
import json
import sys

def parsemaildate(md) :
    md = md[0:10]
    return md

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('brasil.sqlite')
cur = conn.cursor()

baseurl = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial=%2701-01-2020%27&@dataFinalCotacao=%2712-31-2023%27&$top=1100&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao"
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
country = "Brazil"
unit = "BRL"
value = None
initDate = None

index = 0

for index, period in enumerate(js["value"]):
    if js["value"][index]["cotacaoCompra"] > 0:
        value = js["value"][index]["cotacaoCompra"]

    if len(js["value"][index]["dataHoraCotacao"]) > 0:
        initDate = js["value"][index]["dataHoraCotacao"]
    initDate = parsemaildate(initDate)
    value = round(float(value), 3)
    print("   ", value, initDate)
    cur.execute('''INSERT OR IGNORE INTO Currency (id, country, unit, value, initDate)
        VALUES ( ?, ?, ?, ?, ? )''', (index + 1, country, unit, value, initDate))

conn.commit()
cur.close()
