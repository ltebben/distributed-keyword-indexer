import requests
from bs4 import BeautifulSoup
import re
import pymongo
from pymongo import UpdateMany

class Scraper:

    def __init__(self, url=None, collection=None):
        self.url = url
        self.collection = collection

    def getLinks(self, html):
        links = []
        for l in html.find_all('a'):
            l = l.get('href')
            if l and l.startswith("https://"):
                links.append(l)
        return links

    def makeRequest(self, url):
        r = requests.get(url, timeout=10)
        if not r or r.status_code != 200:
            return None
        return r.content

    def scrape(self, url):
        r = self.makeRequest(url)
        if not r:
            return None
        html = BeautifulSoup(r, 'html.parser')
        links = self.getLinks(html)
        text = html.get_text()

        # Persist the extracted words for the given url
        submitWords(text, url)

        return [text, links]

    def submitWords(self, words, url):
      inserts = []
      for word in words:
        # Add this update request to the batch of requests for this url
        inserts = inserts.append(UpdateMany({"word": word}, {"$push": {"urls": url}}, upsert=True))
      
      # Submit requests
      self.collection.bulk_write(inserts)

# s = Scrape()
# text, links = s.scrape("https://www.nbcnews.com/")
# print(text, links)
