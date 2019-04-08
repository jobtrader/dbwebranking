import requests
from bs4 import BeautifulSoup
from textblob import TextBlob
from multiprocessing.dummy import Pool
import sys
from config import config
import json


# Get urls those contain in html page
def get_url(html):
    try:
        soup = BeautifulSoup(html, 'html.parser')
        collected_urls = list()
        for eachURL in soup.select('a'):
            if eachURL.has_attr('href') and 'http' in eachURL['href']:
                collected_urls.append(eachURL['href'])
        return collected_urls
    except Exception as e:
        print(e)


# Get value in first title tag
def get_title(html):
    soup = BeautifulSoup(html, 'html.parser')
    title_tags = soup.select('title')
    if len(title_tags):
        return title_tags[0].text
    return False


# Delete html tag from html page
def clean_html_tag(html):
    try:
        soup = BeautifulSoup(html, features="html.parser")
        return soup.get_text()
    except Exception as e:
        print(e)


# Remove javascript code
def clean_js_code(html):
    try:
        soup = BeautifulSoup(html, features="html.parser")
        for script in soup("script"):
            script.extract()
        return soup.prettify()
    except Exception as e:
        print(e)


def clean_css_code(html):
    try:
        soup = BeautifulSoup(html, features="html.parser")
        for script in soup(["class", "style"]):
            script.extract()
        return soup.prettify()
    except Exception as e:
        print(e)


# Count words in html page
def get_words(text):
    ranks = dict()
    try:
        blob = TextBlob(text)
        words = blob.noun_phrases
        for each_word in words:
            if each_word not in ranks:
                ranks[each_word] = 1
            else:
                ranks[each_word] += 1
        return ranks
    except Exception as e:
        print(e)


# Crawl to web page
def crawl(url):
    try:
        res = requests.get(api_url)
        if res.status_code == 200:
            exist_urls = set(res.json()['result'])
            if url not in exist_urls:
                res = requests.get(url)
                if res.status_code == 200:
                    urls = get_url(res.content)
                    title = get_title(res.content)
                    if not title:
                        title = url
                    text = clean_js_code(res.content)
                    text = clean_css_code(text)
                    text = clean_html_tag(text)
                    if text:
                        word_ranks = get_words(text)
                        payload = {'words': word_ranks, 'url': url, 'title': title, 'refer_urls': json.dumps(urls)}
                        response = requests.post(api_url, json=payload)
                        if response.status_code == 404:
                            print('Status: {0}, {1}'.format(response.status_code, response.json()['message']))
                        else:
                            print('Status: {0}, {1}'.format(response.status_code, response.reason))
                    return urls
            else:
                print('Existing URL: {0}'.format(url))
                return None
        else:
            print('Internal server error: {0}, Description: {1}'.format(res.status_code, res.reason))
            return None
    except Exception as e:
        print(e)


if __name__ == "__main__":
    parent_urls = [*sys.argv[1:]]
    if len(parent_urls):
        con = config('crawler.ini', 'crawler')
        num_pool = int(con['pool'])
        api_url = con['api_url']
        p = Pool(num_pool)
        while parent_urls:
            tmp_urls = list()
            for child_urls in p.imap_unordered(crawl, parent_urls, sys.maxsize):
                if child_urls:
                    if len(child_urls):
                        tmp_urls.extend(child_urls)
            parent_urls = tmp_urls
        p.close()
        p.join()
    else:
        print('There is no initial url.')