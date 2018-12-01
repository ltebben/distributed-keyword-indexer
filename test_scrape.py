import unittest
from scrape import Scrape

class TestScrape(unittest.TestCase):

    def testScrape(self):
        url = "https://www.bbc.com"
        s = Scrape()
        s.setUrl(url)

        keywords, links = s.scrape()
        
        self.assertTrue(keywords, msg='No Keywords found')
        self.assertTrue(links, msg='No links found')

if __name__ == "__main__":
    unittest.main()