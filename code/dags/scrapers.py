# -*- coding=utf-8 -*-

import asyncio
from concurrent.futures import ThreadPoolExecutor

import requests
import re
from bs4 import BeautifulSoup


def fetch_parallel(scrapers):

    async def inner():

        with ThreadPoolExecutor(max_workers=5) as executor:

            with requests.Session() as session:

                loop = asyncio.get_event_loop()

                tasks = [
                    loop.run_in_executor(
                        executor,
                        scraper.fetch_url,
                        session
                    )
                    for scraper in scrapers
                ]

                return await asyncio.gather(*tasks)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(inner())


class HtmlNotSetException(Exception):

    def __init__(self):
        msg = 'Html not set, make sure to call `fetch_url` before'
        super().__init__(msg)


class SoupNotSetException(Exception):

    def __init__(self):
        msg = 'Soup not set, make sure to call `fetch_url` and `parse` before'
        super().__init__(msg)


class IcpeScraper():
    """
    Scraper used to retrieve the rubriques from an icpe
    detail page like this one
    http://www.installationsclassees.developpement-durable.gouv.fr/
    ficheEtablissement.php?champEtablBase=30&champEtablNumero=12015
    """

    def __init__(self, url):
        self.url = url
        self.html = None
        self.soup = None
        self.adresse = []

    def fetch_url(self, session):
        """ fetch an url and set the html field """
        response = session.get(self.url)
        if response.status_code != 200:
            print('Failure %s' % self.url)
            print(response.status_code)
        self.html = response.text

    def parse(self):
        """ parse the html using BeautifulSoup """
        if not self.html:
            raise HtmlNotSetException()
        self.soup = BeautifulSoup(self.html, 'lxml')

    def find_adresse(self):
        """ find adresse in the html tree """
        if not self.soup:
            raise SoupNotSetException()
        content = self.soup.find("div", {"class": "contenuArticle"})
        html = str(content)
        r = re.compile("<br/>(.+?)<br/>", re.MULTILINE)
        m = r.search(html)
        if m:
            groups = m.groups()
            if groups and len(groups) > 0:
                self.adresse = groups[0]