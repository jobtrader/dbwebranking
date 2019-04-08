from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource
# import pgdb
from itertools import chain
from config import config
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import msql

app = Flask(__name__)
CORS(app)
api = Api(app)


limiter = Limiter(
    app,
    key_func=get_remote_address
)


# Allowed only permitted ip address for update database
# Disallowed ip address for read database
@app.before_request
def limit_remote_addr():    
    con = config('api.ini', 'api')    
    disallowed_ip_addr = eval(con['disallowed_ip'])
    allowed_ip_addr = eval(con['allowed_ip'])    
    if request.method != 'POST':        
        if request.remote_addr in disallowed_ip_addr:  
            abort(403)  # Forbidden    
    else:        
        if request.remote_addr not in allowed_ip_addr: 
            abort(403)  # Forbidden

parser = reqparse.RequestParser()
parser.add_argument('word')
parser.add_argument('words')
parser.add_argument('url')
parser.add_argument('title')
parser.add_argument('refer_urls')


class WordSearch(Resource):
    con = config('api.ini', 'api')
    rate_limit = con['rate_limit']
    decorators = [limiter.limit(rate_limit)]
    @staticmethod
    # Assert word input
    def assert_word(word):
        if not word:
            abort(404, message="word parameter require string, got {0}".format(type(word)))
        return word

    # API for get all urls corresponding to specified word.
    def get(self):
        args = parser.parse_args()
        word = self.assert_word(args['word'])
        # return {'result': pgdb.get_links_from_word(word, request.remote_addr, 'searchword')}, 200
        return {'result': msql.get_urls_from_word(word, request.remote_addr, 'searchword')}, 200


class WordList(Resource):
    con = config('api.ini', 'api')
    rate_limit = con['rate_limit']
    decorators = [limiter.limit(rate_limit)]
    # API for get all words that match specified word.
    def get(self):
        args = parser.parse_args()
        word = args['word']
        if not word:
            word = ''
       # return {'result': list(chain.from_iterable(pgdb.get_word(word, request.remote_addr, 'listword')))}, 200
        return {'result': list(chain.from_iterable(msql.get_word(word, request.remote_addr, 'listword')))}, 200 


class Crawler(Resource):
    # API for get all urls in db.
    @staticmethod
    def get():
        # return {'result': list(chain.from_iterable(pgdb.get_urls()))}, 200
        return {'result': list(chain.from_iterable(msql.get_urls()))}, 200

    # Assert words input
    @staticmethod
    def assert_words(words):
        if not words:
            abort(404, message="words parameter require dictionary of words, got {0}".format(type(words)))
        words = eval(words)
        if not isinstance(words, dict):
            abort(404, message="words parameter require dictionary of words, got {0}".format(type(words)))
        if len(words) > 1:
            if not isinstance(tuple(words.keys())[-1], str):
                abort(404, message="words parameter require string of key of dictionary word, got {0} "
                      .format(type(tuple(words.keys())[-1])))
            if not isinstance(tuple(words.values())[-1], int):
                abort(404, message="words parameter require integer of word counting, got {0} "
                      .format(type(tuple(words.values())[-1])))
        return words

    # Assert url input
    @staticmethod
    def assert_url(url):
        if not url:
            abort(404, message="url parameter require string, got {0}".format(type(url)))
        return url

    # Assert title input
    @staticmethod
    def assert_title(title):
        if not title:
            abort(404, message="title parameter require string, got {0}".format(type(title)))
        return title
    
    # Assert referenced urls input
    @staticmethod
    def assert_ref_urls(ref_urls):
        if not ref_urls:
            abort(404, message="ref_urls parameter require list, got {0}".format(type(ref_urls)))
        ref_urls = eval(ref_urls)
        if not isinstance(ref_urls, list):
            abort(404, message="ref_urls parameter require list, got {0}".format(type(ref_urls)))
        return ref_urls

    # Form parameters to row data of word table
    @staticmethod
    def form_row_word(words, url_id):
        for i in range(len(words)):
            words[i] = (url_id,) + words[i]
        return words
    
    # Form parameters to row data of refer_link table
    @staticmethod
    def form_row_refer_url(url_id, refer_urls):
        refer_urls = list(dict.fromkeys(refer_urls))
        for i in range(len(refer_urls)):
            refer_urls[i] = (url_id, refer_urls[i],)
        return refer_urls

    # Insert new words set from new url into table word
    def post(self):
        args = parser.parse_args()
        words = self.assert_words(args['words'])
        url = self.assert_url(args['url'])
        title = self.assert_title(args['title'])
        refer_urls = self.assert_ref_urls(args['refer_urls'])
        words = list(words.items())
        # link_result = pgdb.insert_link(link, title)
        url_id = msql.insert_url(url, title)
        if url_id:
            row_words = self.form_row_word(words, url_id)
            # word_result = pgdb.insert_word_list(row_words)
            word_result = msql.insert_word_list(row_words)
            if word_result:
                refer_urls = self.form_row_refer_url(url_id, refer_urls)
                # refer_link_result = pgdb.insert_refer_link(refer_links)
                refer_url_result = msql.insert_refer_url(refer_urls)
                if refer_url_result:
                    return {'result': 'Inserted Successfully'}, 201
        return {'result': 'Bad Request'}, 400


api.add_resource(Crawler, '/words')
api.add_resource(WordSearch, '/searchword')
api.add_resource(WordList, '/listword')


if __name__ == '__main__':
    con = config('api.ini', 'api')
    app_ip = con['app_ip']
    port = con['port']
    rate_limit = con['rate_limit']
    disallowed_ip_addr = eval(con['disallowed_ip'])
    allowed_ip_addr = eval(con['allowed_ip'])
    app.run(debug=True, host=app_ip, port=port)