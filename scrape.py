import requests
from bs4 import BeautifulSoup
import re
import pymongo
from pymongo import UpdateMany

class Scrape:

    def __init__(self, url=None, collection=None):
        self.url = url
        self.collection = collection

        # load the stop words from disk
        with open('stop-words.txt', 'r') as stopWordsFile:
            self.stopWords = set()
            self.stopWords.update(stopWordsFile.readlines())
            # remove '/n' from the end of all words
            self.stopWords = [word[:-1] for word in self.stopWords]

    def setUrl(self, url):
        self.url = url

    def findLinks(self, html):
        links = []
        for l in html.find_all('a'):
            l = l.get('href')
            video = ['video.', '/video/', '/video?']
            if any(v in l for v in video):
                continue
            pattern = re.match(r"http[s]?:\/\/(www\.)?(.*(\.com|\.org))(\/)?\.*", l)
            
            if pattern:
                root = pattern.group(2)
                if self.url in root or root in self.url:
                    links.append(l)
            # TODO: Decide if we want to accept relative links too
            # elif l.startswith("/"):
            #     links.append(self.url+l)
        
        return links

    def makeRequest(self, url):
        r = requests.get(url, timeout=60)
        if not r or r.status_code != 200:
            return None
        return r.content

    def findKeywords(self, html):
        keywords = set()
        # Get all paragraph tags in the html
        for p in html.find_all('p'):
            # get content and make sure it's real
            string = p.string
            if string is None:
                continue
            # lowercase and split contents of paragraph to words list
            string = string.lower()
            words = string.split(' ')
            # if there are less than 10 words, don't bother
            if len(words) > 10:
                keywords.update(words)
        # Remove all stop words from the set
        return keywords.difference(self.stopWords)

    def scrape(self):
        r = self.makeRequest(self.url)
        if not r:
            return [None, None]
        html = BeautifulSoup(r, 'html.parser')
        links = self.findLinks(html)
        keywords = self.findKeywords(html)
 
        return [keywords, links] 

    def submitWords(self, words): 
      inserts = [UpdateMany({"word": word}, {"$push": {"urls": self.url}}, upsert=True) for word in words]
      
      # Submit requests
      self.collection.bulk_write(inserts)

# s = Scrape()
# s.setUrl("http://www.bbc.com/")
# text, links = s.scrape()
# print(text.encode('utf8'), links)
