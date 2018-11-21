# -*- coding: UTF-8 -*-
# Author:Christina.Li
import sys

reload(sys)
sys.setdefaultencoding("utf8")

import ConfigParser
from DBUtils.PooledDB import PooledDB,SharedDBConnection
import pymysql
import time

class Params:
    def __init__(self,dbcode):
        cf = ConfigParser.ConfigParser()
        cf.read("default.conf")
        self.config = {
        "host": cf.get(dbcode,'host'),
        "port": int(cf.get(dbcode,'port')),
        "user": cf.get(dbcode,'user'),
        "password": cf.get(dbcode,'password')
        }

        self.server_id = int(cf.get(dbcode,'server_id'))
        self.only_schemas = cf.get(dbcode,"database").split(".")

        self.dest_host = cf.get('TEST','host')
        self.dest_user = cf.get('TEST','user')
        self.dest_password = cf.get('TEST','password')
        self.dest_database = cf.get('TEST','database')

class get_data:
    def __init__(self,host,user,password,database,port=3306):
         try:
            conn_pool = PooledDB(pymysql,host=host,user=user,passwd=password,db=database,port=port)
            self.__conn = conn_pool.connection()
            self.__cursor = self.__conn.cursor()
         except Exception,e:
            print e.args
            sys.stdout.flush()
            time.sleep(1)
            db = get_data(host,user,password,database)
            self.__conn = db.__conn
            self.__cursor = db.__conn.cursor()

    def execute(self,sql,val):
        try:
            self.__cursor.execute(sql,val)
            return 0
        except Exception,e:
            print "db execute error:", e.args
            sys.stdout.flush()
            if e.args[0] == 2013 or e.args[0] == 2055 or e.args[0] == 1205:
                time.sleep(5)
                self.execute(sql)
            else:
                exit(1)

    def commit(self):
        self.__conn.commit()






pool = PooledDB(
    creator=pymysql,
    maxconnections=6,
    mincached=2,
    maxcached=5,
    maxshared=3,
    blocking=True,
    setsession=[],
    ping=0,
    host="127.0.0.1",
    port = 3306,
    user = "root",
    password="123456",
    database="lcc",
    charset='utf8'
)

def func():
    conn = pool.connection()
    cur = conn.cursor()
    cur.execute("select * from rme_song_era")
    result = cur.fetchall()
    print result
    conn.close()
if __name__ == '__main__':
    func()




















































































