__author__ = "samrohn77@gmail.com"

from bs4 import BeautifulSoup
import pandas as pd
#from pymongo import MongoClient
import requests
import json
import datetime
import pytz
import time
from database import Database


dates = ['20130428', '20130505','20130512', '20130519','20130526', '20130602', '20130609','20130616', '20130623','20130630','20130707','20130714','20130721','20130728',
        '20130804','20130811','20130818','20130825','20130901','20130908','20130915','20130922','20130929','20140406', '20140413', '20140420','20140427','20140504','20140511','20140518','20140525','20140601', '20140608', '20140615','20140622', '20140629','20150104','20150111','20150118','20150125','20150201','20150208', '20150215','20150222','20150301','20150308','20150315','20150322', '20150329',
        '20150405', '20150412', '20150419', '20150426','20150503','20150510',
        '20150517', '20150524', '20150531', '20150607', '20150607', '20150614', '20150621','20150628','20150705', '20150712','20150719','20150726','20150802', '20150802', '20150809','20150816', '20150823', '20150830','20150906','20150913','20150920','20150927','20151004','20151011','20151018', '20151025','20151101','20151108','20151115','20151122','20151129','20151206','20151213', '20151227','20160103','20160110','20160117','20160124','20160131','20160207','20160214','20160221','20160228','20160306','20160313','20160320','20160327','20160403','20160417','20160424','20160501','20160508', '20160515','20160522','20160529','20160605','20160612','20160619','20160626','20160710','20160717','20160724','20160731','20160814','20160821','20160828','20160904','20160911','20160918','20160925','20160703','20160807','20161009',
        '20161016','20161023', '20161030','20161106', '20161113','20161120','20161127','20161204','20161218','20161225','20170101','20170108','20170115','20170122','20170129','20170205','20170219','20170226','20170305','20170312','20170319','20170326','20170402','20170409','20170416','20170423','20170430','20170507', '20170514','20170528','20170604','20170611','20170618','20170625','20170702','20170709','20170716','20170723','20170730', '20170806','20170813','20170820','20170827','20170903','20170910','20170917', '20170924','20171001','20171008','20171022','20171029','20171105','20171112','20171126','20171203','20171210','20171217','20171224','20171231', '20180107','20180114','20180121','20180128','20180204','20180211',
        '20180218','20180225', '20180304', '20180311', '20180318', '20180325','20180401', '20180408', '20180415',
        '20180422', '20180429', '20180506']

def get_db():
    db = Database()                                                             # connect to your google cloud-sql database
    return db

def get_cmc_data(url):
    html  = requests.get(url).text
    soup = BeautifulSoup(html, 'lxml') # pass the html to lxml parser
    table = soup.find_all('table')[0]  # find table from html
    df = pd.read_html(str(table))
    k = (df[0].to_json(orient='records')) #
    data = json.loads(k)
    return data

def insert_data(db, data, his_date):
    now = datetime.datetime.now()
    his_date = datetime.datetime.strptime(his_date, "%Y%m%d").strftime("%Y-%m-%d")
    for rec in data:
        #if rec["Symbol"] != "BTC":
            #continue
        coin = rec["Name"]

        #res = db.cryptodata.find({db.cryptodata.name : "/^"+coin+"$/"})
        q_string = """SELECT name from coin_history where name= '%s'"""%coin
        res = db.query(q_string)

        if len(res) > 0:
            up_data = {}
            up_data['date'] = his_date
            up_data['rank'] = rec['#']
            up_data['ticker'] = rec['Symbol']
            up_data['price'] = rec['Price']
            up_data['7_day_change'] = rec['% 7d'].strip("%")
            up_data['24_hr_volume'] = rec['Volume (24h)']
            up_data['24_hr_change'] = rec['% 24h'].strip("%")
            up_data['market_cap'] = rec['Market Cap']
            up_data['1_hr_change'] = rec['% 1h'].strip("%")
            up_data['circulating_supply']  = rec['Circulating Supply']
            query_string = """UPDATE TABLE coin_history SET {} """.format(','.join('{}=%s'.format(k) for k in up_data))
            query_string = query_string % tuple(up_data.values())
            query_string = query_string + """ WHERE name = '%s' """ % coin
            print query_string
            db.update(query_string.strip('Low Vol'))
        else:
            #print 'inserting coin, ', coin
            print rec
            up_data = {}
            up_data['name'] = coin
            up_data['date'] = his_date
            up_data['rank'] = rec['#']
            up_data['ticker'] = rec['Symbol']
            up_data['price'] = rec['Price']
            up_data['7_day_change'] = rec['% 7d'].strip("%")
            up_data['24_hr_volume'] = rec['Volume (24h)'].strip("%")
            up_data['24_hr_change'] = rec['% 24h'].strip("%")
            up_data['market_cap'] = rec['Market Cap']
            up_data['1_hr_change'] = rec['% 1h'].strip("%")
            up_data['circulating_supply']  = rec['Circulating Supply']
            placeholders = ', '.join(['%s'] * len(up_data))
            columns = ', '.join(myDict.keys())
            query_string = "INSERT INTO %s ( %s ) VALUES ( %s )" % (table, columns, placeholders)
            query_string = query + tuple(up_data.values())
            print query_string
            db.insert(query_string.strip('Low Vol'))

if __name__ == '__main__':
    #data = get_cmc_data()
    db = get_db()
    count = 0
    for each in dates:
        count = count + 1
        #time.sleep(0.5)
        url = "https://coinmarketcap.com/historical/" + each
        data = get_cmc_data(url)
        insert_data(db, data, each)
        if count == 4:
            break
    #print db.cmcdata.collection
    #clc = db.cryptodata
    #res =  db.cryptodata.find({})
    #for each in res:
    #    print each
    #insert_data(db, data,)
