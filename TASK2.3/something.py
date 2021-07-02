import requests
from bs4 import BeautifulSoup

url = 'https://cfconventions.org/Data/cf-standard-names/77/src/cf-standard-name-table.xml'

document = requests.get(url)

soup= BeautifulSoup(document.content,"lxml-xml")
print (soup.find("title"))