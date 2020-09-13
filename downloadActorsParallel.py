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
allresults = []

def writeUrls():
    conn = sqlite3.connect('./Data/Data.db')
    c = conn.cursor()
    c.execute('SELECT * FROM info')
    records = c.fetchall()
    conn.close()
    counter = 0
    f = open("./urllist.txt", "w")
    for row in records:
        if True:
            imdbID = row[1]
            f.write("https://www.imdb.com/title/{id}/fullcredits/".format(id = imdbID))
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
        imdbFinal = imdbID[:(len(imdbID)-len("/fullcredits/"))]
        res = requests.get(ourl)  
        return res.text, imdbFinal
    except:
        return "error", ourl

def doSomethingWithResult(status):
    f = open("./Results.txt", "w")
    try:

        soup = BeautifulSoup(status[0], 'html.parser')
        table_tags = soup.find('table', {"class": "cast_list"})
        actorTags = table_tags.find_all('a', href=True)

        actors = []
        for i in actorTags:
            if 'name' in str(i) and str(i.string).strip() != "None":
                actor = i.string
                if actor.endswith('\n'):
                    actor = actor[:-1]
                actors.append(actor)
    except:
        actors = []
        return
    strActors = str(','.join(actors))
    allresults.append((status[1]+ " " + strActors + "\n"))
    print(status[1])
    print(actors)
    f.close()
    return

def removeSpaces():
    with open('Results.txt', 'r') as file :
        filedata = file.read()
    filedata = filedata.replace('  ', '\t')
    try:
        with open('./Data/title.actors.tsv', 'w') as file:
            file.write(filedata)
    except:
        try:
            with open('./Data/title.actors.tsv', 'x') as file:
                file.write(filedata)
        except:
            return
    

writeUrls()
starTime = datetime.now()
q = Queue(concurrent * 2)
for i in range(concurrent):
    t = Thread(target=doWork)
    t.daemon = True
    t.start()
try:
    for url in open("urllist.txt"):
        q.put(url.strip())
    q.join()
except KeyboardInterrupt:
    sys.exit(1)



print(str(datetime.now() - starTime))
f = open("./Results.txt", "w")
for row in allresults:
    f.write(row)
f.close()

removeSpaces()
