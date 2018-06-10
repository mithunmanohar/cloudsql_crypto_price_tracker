import json
from flask import Flask
from flask import request
from bs4 import BeautifulSoup
from database import Database
app = Flask(__name__)


def get_db():
    db = Database()                                                             # connect to your google cloud-sql database
    return db

@app.route('/')
def status():
    return json.dumps({"status":"Success"})

@app.route('/get_all_coins')
def get_all_coins():
    auth_key = request.args.get('auth_key')
    db = get_db()
    if auth_key != "fdsrtw435s6af8dsd9sa":
        return "{ACCESS DENIED:Authentication Failed}"
    db = get_db()
    query = "select distinct(name) , rank from coin_history where date=(select date from coin_history order by date desc limit 1)"
    data = db.query(query)
    ret_data = []

    for each in data.fetchall():
        c_data = {each['name'] : each['rank']}
        ret_data.append(c_data)
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
    query = "select distinct (date),rank, price, market_cap, 24_hr_volume, 24_hr_change, 7_day_change, circulating_supply from coin_history where name = %s order by date"%(coin)

    data = db.query(query)
    ret_data = []

    for each in data.fetchall():
        day_data = {'date' : str(each['date']), 'rank':str(each['rank']),
        'price':str(each['price']), '24_hr_volume': str(each['24_hr_volume']),
        '24_hr_change': str(each['24_hr_change']),'7_day_change': str(each['7_day_change']),
        'circulating_supply': str(each['circulating_supply']),
        'market_cap': str(each['market_cap'])}
        #day_data = str(each['date'])
        ret_data.append(day_data)
    return json.dumps(ret_data)

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)
