import requests
from bs4 import BeautifulSoup
import re


class Scrape:

    def __init__(self, url=None):
        self.url = url
        self.discoveredLinks = []
        # load the stop words from disk
        with open('stop-words.txt', 'r') as stopWordsFile:
            self.stopWords = set()
            self.stopWords.update(stopWordsFile.readlines())
            # remove '/n' from the end of all words
            self.stopWords = [word[:-1] for word in self.stopWords]

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
        self.findLinks(html)
        keywords = self.findKeywords(html)
        return [keywords, self.discoveredLinks]

# s = Scrape()
# s.setUrl("http://www.bbc.com/")
# text, links = s.scrape()
# print(text.encode('utf8'), links)
