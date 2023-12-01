import pymysql
from datetime import datetime

host = "220.123.30.211";
port = 3306;
database = "AETHER_IRIS";
user = "root";
password = "biobrain123";

def createConnection():
    global host, port, database, user, password;

    try:
        connection = pymysql.connect(host=host, user=user, passwd=password, db=database, port=port, use_unicode=True, charset="utf8", autocommit=True);
        cursor = connection.cursor(pymysql.cursors.DictCursor);
        return connection, cursor;
    except Exception as error:
        raise error;

def fetchall(cursor, query):
    try:
        cursor.execute(query);
        return cursor.fetchall();
    except Exception as error:
        raise error;

def sqlTimestampNow():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S");