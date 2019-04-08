from mysql.connector import MySQLConnection, Error
from mysql.connector import Error
from config import config 
 
def connect():
    """ Connect to MySQL database """
    try:
        params = config(section='mysql')
        conn = MySQLConnection(**params)
        if conn.is_connected():
            print('Connected to MySQL database')
 
    except Error as e:
        print(e)
 
    finally:
        conn.close()


def create_table():
    """ create table in the MySQL database"""
    commands = (
                """
                    CREATE TABLE IF NOT EXISTS url
                    (   
                        url_id INT NOT NULL AUTO_INCREMENT,
                        url VARCHAR(255) NOT NULL,
                        title VARCHAR(255) NOT NULL,
                        PRIMARY KEY (url_id)
                    );
                """,
                """
                    CREATE TABLE IF NOT EXISTS word
                    (
                        wurl_id INT NOT NULL,
                        word VARCHAR(255) NOT NULL,
                        count INT NOT NULL,
                        PRIMARY KEY (wurl_id, word),
                        CONSTRAINT fk_word
                            FOREIGN KEY (wurl_id) REFERENCES url (url_id)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE
                    );
                """,
                """
                    CREATE TABLE IF NOT EXISTS refer_url
                    (
                        rurl_id INT NOT NULL,
                        ref_url VARCHAR(255) NOT NULL,
                        PRIMARY KEY (rurl_id, ref_url),
                        CONSTRAINT fk_refer_link
                            FOREIGN KEY (rurl_id) REFERENCES url (url_id)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE
                    );
                """,
                """
                    CREATE TABLE IF NOT EXISTS request
                    (
                        id INT NOT NULL AUTO_INCREMENT,
                        ip_addr VARCHAR(23) NOT NULL,
                        word VARCHAR(255) NOT NULL,
                        uri VARCHAR(255) NOT NULL,
                        request_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (id)
                    );
                """
                )
    conn = None
    try:
        # read the connection parameters
        params = config(section='mysql')
        # connect to the MySQL server
        conn = MySQLConnection(**params)
        # create table one by one
        cursor = conn.cursor()
        for command in commands:
            cursor.execute(command)
        cursor.close()
        # commit the changes
        conn.commit()
        print('Create table successfully.')
    except Error as e:
        print('Error:', e)
    finally:
        conn.close()


def create_index():
    """ create index in the table"""
    commands = (
                "ALTER TABLE word ADD INDEX idx_word (word, count);",
                "ALTER TABLE url ADD INDEX idx_url (url);"
                 )
    conn = None
    try:
        # read the connection parameters
        params = config(section='mysql')
        # connect to the MySQL server
        conn = MySQLConnection(**params)
        # create table one by one
        cursor = conn.cursor()
        # create tables index
        for command in commands:
            cursor.execute(command)
        cursor.close()
        conn.commit()
        print('Create index successfully.')
    except Error as e:
        print('Error:', e)
    finally:
        conn.close()


def insert_word_list(word_list):
    """ insert multiple words into the word table  """
    sql = "INSERT INTO word (wurl_id, word, count) VALUES (%s, %s, %s);"
    conn = None
    try:
        # read the connection parameters
        params = config(section='mysql')
        # connect to the MySQL server
        conn = MySQLConnection(**params)
        # create table one by one
        cursor = conn.cursor()
        # execute the INSERT statement
        cursor.executemany(sql, word_list)
        cursor.close()
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        print('Insert Words Successfully')
        return True
    except Error as e:
        print('Error:', e)
    finally:
        conn.close()


def insert_url(url, title):
    """ insert a url into the url table  """
    sql = "INSERT INTO url (url, title) VALUES (%s, %s);"
    conn = None
    try:
        # read the connection parameters
        params = config(section='mysql')
        # connect to the MySQL server
        conn = MySQLConnection(**params)
        # create table one by one
        cursor = conn.cursor()
        # execute the INSERT statement
        cursor.execute(sql, (url, title))
        cursor.close()
        # commit the changes to the database
        conn.commit()
        print('Insert url Successfully')
        sql = "SELECT MAX(url_id) FROM url;"
        cursor = conn.cursor()
        # execute the INSERT statement
        cursor.execute(sql)
        url_id = cursor.fetchone()[0]
        cursor.close()
        return url_id
    except Error as e:
        print('Error:', e)
    finally:
        conn.close()


def insert_request(ip, word, uri):
    """ insert request info into the request table  """
    sql = "INSERT INTO request (ip_addr, word, uri) VALUES (%s, %s, %s);"
    conn = None
    try:
        # read the connection parameters
        params = config(section='mysql')
        # connect to the MySQL server
        conn = MySQLConnection(**params)
        # create table one by one
        cursor = conn.cursor()
        # execute the INSERT statement
        cursor.execute(sql, (ip, word, uri))
        
        if cursor.lastrowid:
            print('Insert request id {0} Successfully'.format(cursor.lastrowid))
        else:
            print('last insert request id not found')
        cursor.close()
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        return True
    except Error as e:
        print('Error:', e)
    finally:
        conn.close()
    


def insert_refer_url(refer_urls):
    """ insert multiple tuples of referenced url into the refer_url table  """
    sql = "INSERT INTO refer_url (rurl_id, ref_url) VALUES (%s, %s);"
    conn = None
    try:
        # read the connection parameters
        params = config(section='mysql')
        # connect to the MySQL server
        conn = MySQLConnection(**params)
        # create table one by one
        cursor = conn.cursor()
        # execute the INSERT statement
        cursor.executemany(sql, refer_urls)
        cursor.close()
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        print('Insert referenced urls Successfully') 
        return True
    except Error as e:
        print('Error:', e)
    finally: 
        conn.close()


def get_urls():
    """ get distinct urls from the url table """
    conn = None
    try:
        # read the connection parameters
        params = config(section='mysql')
        # connect to the MySQL server
        conn = MySQLConnection(**params)
        # create table one by one
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT url FROM url;")
        rows = cursor.fetchall()
        cursor.close()
        # If no row return []
        return rows
    except Error as e:
        print('Error:', e)
    finally:
        conn.close()


def get_urls_from_word(word, req_ip, request):
    """ get multiple urls, titles and counts of the specified word from the joined table """
    sql = "SELECT url, title, CAST(SUM(count) AS UNSIGNED) AS count FROM url INNER JOIN word ON url.url_id = word.wurl_id WHERE word LIKE {0} GROUP BY wurl_id ORDER BY count DESC;".format("'%{0}%'".format(word))
    conn = None
    try:
        # read the connection parameters
        params = config(section='mysql')
        # connect to the MySQL server
        conn = MySQLConnection(**params)
        # create table one by one
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
        insert_request(req_ip, word, request)
        # If no row return []
        return rows
    except Error as e:
        print('Error:', e)
    finally:
        conn.close()


def get_word(word, req_ip, request):
    """ query matched words of specified word from word table """
    sql = "SELECT DISTINCT word FROM word WHERE word LIKE {0} ORDER BY word DESC LIMIT 10;".format("'{0}%'".format(word))
    conn = None
    try:
        # read the connection parameters
        params = config(section='mysql')
        # connect to the MySQL server
        conn = MySQLConnection(**params)
        # create table one by one
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
        insert_request(req_ip, word, request)
        # If no row return []
        return rows
    except Error as e:
        print('Error:', e)
    finally:
        conn.close()


def get_rate_limit_minute(ip):
    """ query rate limit of specified ip """
    sql = "SELECT COUNT(ip_addr) FROM request WHERE request_timestamp > CURRENT_TIMESTAMP - INTERVAL '1' MINUTE AND ip_addr = %s;"
    conn = None
    try:
        # read the connection parameters
        params = config(section='mysql')
        # connect to the MySQL server
        conn = MySQLConnection(**params)
        cursor = conn.cursor()
        cursor.execute(sql, (ip))
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Error as e:
        print('Error:', e)
    finally:
        conn.close()


def delete_table(table_name):
    """ Delete table"""
    sql = "DELETE FROM {0};".format(table_name)
    conn = None
    try:
        # read the connection parameters
        params = config(section='mysql')
        # connect to the MySQL server
        conn = MySQLConnection(**params)
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        print('Delete table {0} successfully'.format(table_name))
    except Error as e:
        print('Error:', e)
    finally:
        conn.close()


def drop_table(table_name):
    """ Drop table"""
    sql = "DROP TABLE {0};".format(table_name)
    conn = None
    try:
        # read the connection parameters
        params = config(section='mysql')
        # connect to the MySQL server
        conn = MySQLConnection(**params)
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        print('Drop table {0} successfully'.format(table_name))
    except Error as e:
        print('Error:', e)
    finally:
        conn.close()

if __name__ == '__main__':
    connect()
    drop_table('refer_url')
    drop_table('word')
    drop_table('request')
    drop_table('url')
    create_table()
    create_index()
    
#    urls = ([
#         ('hotmail.com', 'Hotmail'),
#         ('gmail.com', 'GMail')
#     ])
#    for each_row in urls:
#        url_id = insert_url(each_row[0], each_row[1])
    
#    insert_word_list([
#        (2, 'abc', 3),
#        (2, 'def', 4)
#        ])


#    insert_refer_url([
#        (2, 'www.example.com'),
#        (2, 'www.example4.com')
#     ])

#    print(get_urls_from_word('vi', '127.0.0.1', '/searchword'))