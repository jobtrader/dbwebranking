# Must change database parameter in mysql section of database.ini to test before test.

from config import config
from multiprocessing.dummy import Pool
from mysql.connector import MySQLConnection
import sys

urls = ([
         'google.com',
         'facebook.com',
         'instragram.com',
         'twitter.com',
         'hotmail.com',
         'gmail.com',
         'joojle.com',
         'line.com',
         'afaps.ac.th',
         'kmutnb.ac.th',
         'crma.ac.th',
         'rtna.ac.th',
         'rtaf.ac.th',
         'rpca.ac.th'
     ])


def insert(url):
    sql = "INSERT INTO test_pool (url) VALUES (%s);"
    conn = MySQLConnection(**params)
    cursor = conn.cursor()
    cursor.execute(sql, (url,))
    cursor.close()
    conn.commit()
    sql = "SELECT MAX(url_id) FROM test_pool;"
    cursor = conn.cursor()
    cursor.execute(sql)
    url_id = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return (url_id, url)


def create():
    sql = "CREATE TABLE IF NOT EXISTS test_pool (url_id INT NOT NULL AUTO_INCREMENT, url VARCHAR(255) NOT NULL, PRIMARY KEY (url_id)) ;"
    conn = MySQLConnection(**params)
    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()
    conn.commit()
    conn.close()


def drop():
    sql = "DROP TABLE test_pool;"
    conn = MySQLConnection(**params)
    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()
    conn.commit()
    conn.close()


def select():
    sql = "SELECt * FROM test_pool;"
    conn = MySQLConnection(**params)
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return set(rows)


p = Pool(2)
log = dict()
result_list = list()
params = config(section='mysql')
chunk_size = sys.argv[1]

if chunk_size <= sys.maxsize and chunk_size >=0:
    create()
    for res in p.imap_unordered(insert, urls, chunk_size):
        result_list.append((res[0], res[1]))
    r = select()
#    print(r)
    print(r == set(result_list))
    drop()
else:
    print("chunk_size must >=0 and <= {}".format(sys.maxsize))
# Notice if chunksize of imap_unordered greater than or equal to nmber of elements of url it will result as True otherwise false. 