import json
import pytz
import MySQLdb
import requests
import datetime
from database import Database
import pandas as pd
from bs4 import BeautifulSoup


def get_db():
    db = Database()                                                             # connect to your google cloud-sql database
    return db

def get_cmc_data():

    url = "https://coinmarketcap.com/all/views/all/"
    print 'url', url
    html = requests.get(url).text
    print 'got html resp'
    soup = BeautifulSoup(html, 'lxml')
    table = soup.find_all('table')[0]
    print 'got html tabkle'
    df = pd.read_html(str(table))
    print 'created df'
    k = (df[0].to_json(orient='records'))
    print 'converted df to json'
    data = json.loads(k)
    print 'retrun json'
    return data

def validate_rec(rec):
    for each in rec:
        value = rec[each]
        if type(value) is unicode :
            rec[each] = value.strip().replace(",", "").replace('%', "")\
            .replace('$', "").replace('*', "").replace('Low Vol', '0')\
            .replace('?', '0')
        else:
            pass
    return rec

def update_db(db, rec):
    rec = validate_rec(rec)
    query_str = """SELECT std_name from currencies where std_name = %s""" % rec['Name']
    data = db.query(query_str)
    #for each in data:
    #	print each
    #	break
    coin = rec['Name']

    date_today = now.strftime("%Y-%m-%d")
    up_data = {}
    up_data['name'] = '"' + coin + '"'
    up_data['date'] = "STR_TO_DATE(" + date_today + ", '%d/%m/%Y')"
    up_data['rank'] = rec['#']
    up_data['ticker'] = '"' + rec['Symbol'] + '"'
    up_data['price'] = rec['Price']
    up_data['7_day_change'] = rec['% 7d']
    up_data['24_hr_volume'] = rec['Volume (24h)']
    up_data['24_hr_change'] = rec['% 24h']
    up_data['market_cap'] = rec['Market Cap']
    up_data['1_hr_change'] = rec['% 1h']
    up_data['circulating_supply']  = rec['Circulating Supply']
    placeholders = ', '.join(['%s'] * len(up_data))
    columns = ', '.join(up_data.keys())
    query_string = "INSERT INTO %s ( %s ) VALUES ( %s )" % ('coin_history', columns, placeholders)
    query_string = query_string % tuple(up_data.values())
    try:
        #db.insert(query_string)
        print "[pass", query_string
    except:
        print 'exception for ', query_string

def process_data(db, data):
    now = datetime.datetime.now()
    date_today = now.strftime("%Y-%m-%d")
    for rec in data:
	#update_db(db, rec)
        print rec
        break	
        #res = db.cryptodata.find({"name" : coin})
        #if res.count() > 0:
            #up_data = {}
            #up_data['date'] = date_today
            #up_data['rank'] = rec['#']
            #up_data['ticker'] = rec['Symbol']
            #up_data['price'] = rec['Price']
            #up_data['% 7d'] = rec['% 7d']
            #up_data['Volume (24h)'] = rec['Volume (24h)']
            #up_data['% 24h'] = rec['% 24h']
            #up_data['market_cap'] = rec['Market Cap']
            #up_data['% 1hr'] = rec['% 1h']
            #up_data['ciculating_supply'] = rec['Circulating Supply']
            #db.cryptodata.update({"name" : coin},{'$push':{'data': up_data}})
        #else:
            #record = {}
            #db.cryptodata
            #record['name'] = coin
            #record['data'] = []
            #details = {}
            #details['date'] = date_today
            #details['rank'] = rec['#']
            #details['ticker'] = rec['Symbol']
            #details['price'] = rec['Price']
            #details['% 7d'] = rec['% 7d']
            #details['Volume (24h)'] = rec['Volume (24h)']
            #details['% 24h'] = rec['% 24h']
            #details['market_cap'] = rec['Market Cap']
            #details['% 1h'] = rec['% 1h']
            #details['circulating_supply'] = rec['Circulating Supply']
            #record['data'].append(details)
            #db.cryptodata.insert(record)


if __name__ == '__main__':
    print 'getting data'
    data = get_cmc_data()
    #gprint 'got data', data
    db = get_db()
    print 'go tdb'
    process_data(db, data)
    print 'finished'
