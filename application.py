import json
import requests
#import pymongo
#import pandas as pd
from flask import Flask
from flask import request
from bs4 import BeautifulSoup
#from pymongo import MongoClient
from database import Database
app = Flask(__name__)


def get_db():
    db = Database()                                                             # connect to your google cloud-sql database
    return db

@app.route('/')
def status():
    return json.dumps({"status":"Success"})

@app.route('/get_data')
def get_data():
    coin = request.args.get('coin_name')
    date = request.args.get('date')
    auth_key = request.args.get('auth_key')
    db = get_db()
#    if auth_key != "fdsrtw435s6af8dsd9sa":
#        return "{ACCESS DENIED:Authentication Failed}"
#    if coin:
#        coin = coin.split(",")
#
#    else:
#        data = db.cryptodata.find({})
    db = get_db()
    data = db.query("select * from coin_history where name = 'BTC Bitcoin'")
    ret_data = []

    for each in data.fetchall():
        for m in each:
            print each[m]
    return json.dumps(str(data))
#    for hist_data in data:
#        for dts in hist_data['data']:
#            ret_data.append(dts)
#    return json.dumps(ret_data)


#@app.route('/get_coin_list')
#def get_coin_list():
    #db = get_db()
    #data = db.cryptodata.find({})
    #coins = {}
    #coin_data = []
    #for each in data:
        #for cn in each['data']:
            #try:
                #coin_data.append({cn['org_name']: cn['ticker']})
            #except Exception as e:
                #pass

    #coins["data"] = coin_data

    #return json.dumps(coins)

#@app.route('/get_coins')
#def get_coins():
    #url = "https://coinmarketcap.com/all/views/all/"
    #html = requests.get(url).text
    #soup = BeautifulSoup(html, 'lxml')
    #table = soup.find_all('table')[0]
    #df = pd.read_html(str(table))
    #k = (df[0].to_json(orient='records'))
    #data = json.loads(k)
    #coin_list = []
    #for rec in data:
        #coin_list.append({"coin":rec['Name'], "ticker":rec['Symbol']})
    #return json.dumps({"coins":coin_list})


if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)
