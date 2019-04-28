#!/usr/bin/python
import psycopg2
from config import config


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        # create a cursor
        cur = conn.cursor()
        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def create_table():
    """ create table in the PostgreSQL database"""
    commands = ("""
                    CREATE TABLE IF NOT EXISTS url 
                    (
                        url_id SERIAL,
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
                        CONSTRAINT fk_refer_url
                            FOREIGN KEY (rurl_id) REFERENCES url (url_id)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE
                    );
                """,
                """
                    CREATE TABLE IF NOT EXISTS request (
                        id SERIAL,  
                        ip_addr VARCHAR(23) NOT NULL,
                        word VARCHAR NOT NULL,
                        uri VARCHAR(255) NOT NULL,
                        request_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (id)
                    );
                """)
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
        print('Create table successfully.')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_index():
    """ create index in the table"""
    commands = (
                "CREATE INDEX IF NOT EXISTS idx_word ON word (word, count);",
                "CREATE INDEX IF NOT EXISTS idx_url ON url (url);"
                 )

    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create tables index
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
        print('Create index successfully.')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_word_list(word_list):
    """ insert multiple words into the word table  """
    sql = "INSERT INTO word (wurl_id, word, count) VALUES (%s, %s, %s);"
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql, word_list)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
        print('Insert Words Successfully')
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_url(url, title):
    """ insert a link into the url table  """
    sql = "INSERT INTO url (url, title) VALUES (%s, %s) RETURNING url_id;"
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (url, title,))
        # get the generated id back
        gen_id = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
        print('Insert url Successfully')
        return gen_id
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_request(ip, word, uri):
    """ insert request info into the request table  """
    sql = "INSERT INTO request (ip_addr, word, uri) VALUES (%s, %s, %s) RETURNING id;"
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (ip, word, uri,))
        # get the generated id back
        gen_id = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
        print('Insert request id {0} Successfully'.format(gen_id))
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_refer_url(refer_urls):
    """ insert multiple tuple of referenced url into the refer_url table  """
    sql = "INSERT INTO refer_url (rurl_id, ref_url) VALUES (%s, %s);"
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql, refer_urls)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
        print('Insert referenced url Successfully')
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_urls():
    """ get distinct urls from the url table """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT url FROM url;")
        rows = cur.fetchall()
        cur.close()
        # If no row return []
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_urls_from_word(word, req_ip, request):
    """ get multiple urls, titles and counts of the specified word from the joined table """
    sql = """
             SELECT url, title, SUM(count) AS count 
             FROM url
             INNER JOIN word
                ON url.url_id = word.wurl_id
             WHERE word LIKE '%%' || (%s) || '%%'
             GROUP BY url_id
             ORDER BY count DESC;
          """
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (word,))
        rows = cur.fetchall()
        cur.close()
        insert_request(req_ip, word, request)
        # If no row return []
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_word(word, req_ip, request):
    """ query matched words of specified word from word table """
    sql = "SELECT DISTINCT word FROM word WHERE word LIKE  (%s) || '%%' ORDER BY word DESC LIMIT 10;"
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (word,))
        rows = cur.fetchall()
        cur.close()
        insert_request(req_ip, word, request)
        # If no row return []
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_rate_limit_minute(ip):
    """ query rate limit of specified ip """
    sql = "SELECT COUNT(ip_addr) FROM request WHERE request_timestamp > CURRENT_TIMESTAMP - INTERVAL '1' MINUTE AND ip_addr = %s;"
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (ip,))
        count = cur.fetchone()[0]
        cur.close()
        return count
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def delete_table(table_name):
    """ Delete table"""
    sql = "DELETE FROM {0};".format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        print('Delete table {0} successfully'.format(table_name))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def drop_table(table_name):
    """ Drop table"""
    sql = "DROP TABLE {0};".format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        print('Drop table {0} successfully'.format(table_name))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    connect()
    drop_table('refer_url')
    drop_table('word')
    drop_table('request')
    drop_table('url')
    create_table()
    create_index()
    urls = ([
         ('google.com', 'Google'),
         ('facebook.com', 'Facebook'),
         ('instragram.com', 'Instragram'),
         ('twitter.com', 'Twitter'),
         ('hotmail.com', 'Hotmail'),
         ('gmail.com', 'GMail')
     ])
    for each_row in urls:
        insert_url(each_row[0], each_row[1])

    insert_word_list([
        (6, 'abc', 3),
        (6, 'def', 4)
        ])


    insert_refer_url([
        (6, 'www.example.com'),
        (6, 'www.example4.com') 
    ])


    print(get_urls_from_word('b', '127.0.0.1', '/searchword'))
    print(get_word('b', '127.0.0.1', '/listword'))
