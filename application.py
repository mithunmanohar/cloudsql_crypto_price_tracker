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

@app.route('/get_all_dates')
def get_all_dates():
    coin = request.args.get('coin_name')
    date = request.args.get('date')
    auth_key = request.args.get('auth_key')
    db = get_db()
    if auth_key != "fdsrtw435s6af8dsd9sa":
        return "{ACCESS DENIED:Authentication Failed}"
    coin = '"' + coin + '"'
    db = get_db()
    query = "select distinct (date), rank from coin_history where name = %s"%(coin)
    data = db.query(query)
    ret_data = []

    for each in data.fetchall():
        day_data = {str(each['date']) : str(each['rank'])}
        ret_data.append(day_data)
    return json.dumps(ret_data)


@app.route('/get_all_ranks')
def get_all_data():
    auth_key = request.args.get('auth_key')
    coin = request.args.get('coin_name')
    db = get_db()
    if auth_key != "fdsrtw435s6af8dsd9sa":
        return "{ACCESS DENIED:Authentication Failed}"
    coin = '"' + coin + '"'
    db = get_db()
    query = "select distinct (date), rank from coin_history where name = %s order by date"%(coin)
    data = db.query(query)
    ret_data = []

    for each in data.fetchall():
        #day_data = {str(each['date']) : str(each['rank'])}
        ret_data.append(day_data)
    return json.dumps(ret_data)

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)
