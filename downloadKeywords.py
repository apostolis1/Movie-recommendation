from urllib.parse import urlparse
from threading import Thread
import http.client, sys
from queue import Queue
import sqlite3
import requests
from datetime import datetime
from bs4 import BeautifulSoup

concurrent = 200
conn = sqlite3.connect('./Data/Data.db')
c = conn.cursor()
allkeywords = []

def writeUrls():
    conn = sqlite3.connect('./Data/Data.db')
    c = conn.cursor()
    c.execute('SELECT * FROM info')
    records = c.fetchall()
    conn.close()
    counter = 0
    f = open("./keywordsURL.txt", "w+")
    for row in records:
        if True:
            imdbID = row[1]
            f.write("https://www.imdb.com/title/{id}/keywords/".format(id = imdbID))
            f.write('\n')
            counter += 1
        else:
            break
    f.close()
    
def doWork():
    while True:
        url = q.get()
        status = getStatus(url)
        doSomethingWithResult(status)
        q.task_done()


def getStatus(ourl):
    try:
        imdbID = ourl[len('https://www.imdb.com/title/'):]
        imdbFinal = imdbID[:(len(imdbID)-len("/keywords/"))]
        res = requests.get(ourl)
        return res.text, imdbFinal
    except:
        return "error", ourl

def doSomethingWithResult(status):
    """ Web Scrapping is used for this function since this data isn't available
    in the dataset
    Returns a list of keywords given a imdb ID
    Returns empty list if keywords can't be found
    """
    try:
        soup = BeautifulSoup(status[0], 'html.parser')
        table_tags = soup.find_all('td', {"class": "soda sodavote"})
        keywords = []
        for i in table_tags:
            keywords.append(i["data-item-keyword"])
        print(len(keywords))
    except:
        return []
    strKeywords = str(','.join(keywords))
    allkeywords.append((status[1]+ "\t" + strKeywords + "\n"))
    print(status[1])
    print(keywords)
    return

def removeSpaces():
    with open('./Keywords.txt', 'r') as file :
        filedata = file.read()
    filedata = filedata.replace('  ', '\t')
    with open('./Data/title.keywords.tsv', 'w+') as file:
        file.write(filedata)
    return
    

writeUrls()
starTime = datetime.now()
q = Queue(concurrent * 2)
for i in range(concurrent):
    t = Thread(target=doWork)
    t.daemon = True
    t.start()
try:
    for url in open("./keywordsURL.txt"):
        q.put(url.strip())
    q.join()
except KeyboardInterrupt:
    sys.exit(1)



print(str(datetime.now() - starTime))
f = open("./Keywords.txt", "w+")
for row in allkeywords:
    f.write(row)
f.close()

removeSpaces()
