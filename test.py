from bs4 import BeautifulSoup
import urllib.request as urllib2
import re
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def chrome_selenium():
    url="https://www.imdb.com/title/tt9356952/reviews"
    htmlContent =urllib2.urlopen(url).read()
    soup = BeautifulSoup(htmlContent, 'lxml')
    button = soup.find('button', attrs={"id": "load-more-trigger"})
    if button==None:
        print(None)


chrome_selenium()