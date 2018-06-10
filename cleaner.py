from database import Database

def get_db():
    db = Database()                                                             # connect to your google cloud-sql database
    return db

def insert_missing():
    coin = '"' + "BTC Bitcoin" + '"'
    db = get_db()
    data = db.query("select date from coin_history where name = %s") % coin
    dates = []
    for each in data.fetchall():
        dates.append(each['date'])
    names = []
    data = db.query("select distict(name) from coin_history")
    for each in data.fetchall():
        names.append(each['name'])
    for each in dates:
        for name in names:
            name = '"' + name + '"'
            query = "select * from "

