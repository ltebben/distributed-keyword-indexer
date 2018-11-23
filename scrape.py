import requests
from bs4 import BeautifulSoup
import re

class Scrape:
    
    def __init__(self, url=None):
        self.url = url
        self.discoveredLinks = []
    
    def getLinks(self):
        return self.links

    def setUrl(self):
        self.url = url

    def findLinks(self, html):
        for l in html.find_all('a'):
            l = l.get('href')
            if l and l.startswith("https://"):
                self.discoveredLinks.append(l)

    def makeRequest(self, url):
        r = requests.get(url, timeout=10)
        if not r or r.status_code != 200:
            return None
        return r.content

    def scrape(self):
        r = self.makeRequest(self.url)
        if not r:
            return None
        html = BeautifulSoup(r, 'html.parser')
        self.findLinks(html)
        text = html.get_text()
        return [text, self.discoveredLinks]

# s = Scrape()
# text, links = s.scrape("https://www.nbcnews.com/")
# print(text, links)
