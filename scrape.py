import requests
from bs4 import BeautifulSoup
import re

class Scrape:
    def __init__(self, url=None):
        self.url = url

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
        return [text, links]


s = Scrape()
text, links = s.scrape("https://www.nbcnews.com/")
print(text, links)
