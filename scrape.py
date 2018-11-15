import requests
from bs4 import BeautifulSoup
import re

class Scrape:
    def __init__(self, url=None):
        self.url = url

    def getLinks(self, html):
        links = []
        for l in html('a'):
            l = l.extract()#.encode('utf-8')
            m = re.match(r'<a href="(https://[^\s]*)">', str(l))
            if m:
                links.append(m.groups()[0].lower())
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
        html_string = [s.encode("utf-8") for s in html.select('body')]
        links = self.getLinks(html)
        return [html_string, links]


s = Scrape()
text, links = s.scrape("https://www.nbcnews.com/")
print(text, links)