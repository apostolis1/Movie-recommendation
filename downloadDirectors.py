from urllib.parse import urlparse
from threading import Thread
import http.client, sys
from queue import Queue
import sqlite3
import requests, csv
from datetime import datetime
from bs4 import BeautifulSoup


"""

TO BE DELETED IF downloadUniqueDirectors.py WORKS

"""
concurrent = 200
allDirectors = []

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
        dataTuple = q.get()
        source = getSource(dataTuple)
        doSomethingWithResult(source)
        q.task_done()

#params: the data tuple
def getSource(data):
    try:
        imdbID = data[0]
        directors = data[1].split(",")
        directorsURLs = []
        sourceResults = []
        for director in directors:
            directorsURLs.append("https://www.imdb.com/name/{id}/".format(id = director))
        for url in directorsURLs:
            res = requests.get(url)
            sourceResults.append(res.text)
        return sourceResults, imdbID
    except:
        return "error", data

def doSomethingWithResult(sourceTuple):
    """ Web Scrapping is used for this function since this data isn't available
    in the dataset
    Returns a list of keywords given a imdb ID
    Returns empty list if keywords can't be found
    """
    try:
        directors = []
        soups = sourceTuple[0]
        for i in soups:
            soup = BeautifulSoup(i, 'html.parser')
            nameTag = soup.find('span', {"class": "itemprop"})
            name = str(nameTag.string)
            directors.append(name)
    except:
        return []
    strDirectors = str(','.join(directors))
    allDirectors.append((sourceTuple[1]+ "\t" + strDirectors + "\n"))
    print(sourceTuple[1])
    print(directors)
    return

def removeSpaces():
    with open('./Keywords.txt', 'r') as file :
        filedata = file.read()
    filedata = filedata.replace('  ', '\t')
    with open('./Data/title.keywords.tsv', 'w+') as file:
        file.write(filedata)
    return
    

# writeUrls()
starTime = datetime.now()

#Queue of tuples with (imdbID, imdbDirectorsID)
q = Queue(concurrent * 2)
for i in range(concurrent):
    t = Thread(target=doWork)
    t.daemon = True
    t.start()
try:
    tsv_file = open("./Data/title.crew.tsv")
    read_tsv = csv.reader(tsv_file, delimiter="\t")
    for row in read_tsv:
        q.put((row[0], row[1]))
    q.join()
except KeyboardInterrupt:
    sys.exit(1)



print(str(datetime.now() - starTime))
f = open("./Directors.txt", "w+")
for row in allDirectors:
    f.write(row)
f.close()

# removeSpaces()