__author__ = "mithunmanohar79@gmail.com"

from bs4 import BeautifulSoup
import pandas as pd
import requests
import json
import datetime
import pytz
import time
import cred

import MySQLdb


class Database:

    host = settings.host
    user_name = settings.user_name
    password = settings.password
    db = settings.database

    def __init__(self):
        self.connection = MySQLdb.connect(self.host, self.user, self.password, self.db)
        self.cursor = self.connection.cursor()

    def insert(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except:
            self.connection.rollback()

    def update(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except:
            self.connection.rollback()


    def query(self, query):
        cursor = self.connection.cursor( MySQLdb.cursors.DictCursor )
        cursor.execute(query)

        return cursor.fetchall()

    def __del__(self):
        self.connection.close()


if __name__ == "__main__":
    db = Database()