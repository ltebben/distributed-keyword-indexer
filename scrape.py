import requests
from bs4 import BeautifulSoup
import re

class Scrape:
    
    def __init__(self, url=None):
        self.url = url
        self.discoveredLinks = []
    
    def getLinks(self):
        return self.discoveredLinks

    def setUrl(self, url):
        self.url = url

    def findLinks(self, html):
        for l in html.find_all('a'):
            l = l.get('href')
            if l and (l.startswith("https://") or l.startswith("http://")):
                self.discoveredLinks.append(l)
            # TODO: Decide if we want to accept relative links too
            # elif l.startswith("/"):
            #     self.discoveredLinks.append(self.url+l)

    def makeRequest(self, url):
        r = requests.get(url, timeout=60)
        if not r or r.status_code != 200:
            return None
        return r.content

    def scrape(self):
        r = self.makeRequest(self.url)
        if not r:
            return [None, None]
        html = BeautifulSoup(r, 'html.parser')
        self.findLinks(html)
        text = html.get_text()
        return [text, self.discoveredLinks]

# s = Scrape()
# s.setUrl("http://www.bbc.com/")
# text, links = s.scrape()
# print(text.encode('utf8'), links)
