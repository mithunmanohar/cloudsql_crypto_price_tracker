import json
import pytz
import MySqlDb
import requests
import datetime
from database import Database
import pandas as pd
from bs4 import BeautifulSoup


def connect_to_db():
    db = Database()                                                             # connect to your google cloud-sql database
    return db

def get_cmc_data():
    url = "https://coinmarketcap.com/all/views/all/"
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'lxml')
    table = soup.find_all('table')[0]
    df = pd.read_html(str(table))
    k = (df[0].to_json(orient='records'))
    data = json.loads(k)
    return data

def update_db(db, coin_data):
    query_str = """SELECT std_name from currencies where std_name = %s"""
    		% coin_data['Name']
    data = db.query(query_str)
    for each in data:
    	print each
    if len(data) > 0:
    	# update
    else:
    	# insert
    	insert_query = """INSERT into currencies VALUES('%s','%s','%s','%d',
    			'%f','%f','%s','%s')"""%("dsad","das","dasd",21,21.11,21.11,"1312","dassda")

def process_data(db, data):
    now = datetime.datetime.now()
    date_today = now.strftime("%Y-%m-%d")
    for rec in data:
	update_db(db, rec)
	
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
    data = get_cmc_data()
    db = get_db()
    process_data(db, data)
