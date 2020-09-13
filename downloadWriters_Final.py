from urllib.parse import urlparse
from threading import Thread
import http.client, sys
from queue import Queue
import sqlite3
import requests
from datetime import datetime
from bs4 import BeautifulSoup


"""

A script to take a list of urls in the form of https://www.imdb.com/name/{nameID}/
where nameID is a valid IMDB name ID from a text file ("./UniqueDirectorsUrls.txt")
It creates a new file in the same folder, "./FinalDirectors.txt" which is a tsv
containing the nameImdbID (string) in first column and the name of the director (string) in the second column

TO BE COMBINED IN ONE MASTER DOWNLOAD DIRECTORS FILE (as the final step, when proccessing is done and we have the ./UniqueDirectorsUrls.txt" file)
"""
concurrent = 200
allDirectors = []
    
def doWork():
    while True:
        url = q.get()
        status = getStatus(url)
        doSomethingWithResult(status)
        q.task_done()


def getStatus(url):
    try:
        imdbID = url[len('https://www.imdb.com/name/'):]
        imdbFinal = imdbID[:(len(imdbID)-len("/"))]
        res = requests.get(url)
        return res.text, imdbFinal
    except:
        return "error", url

def doSomethingWithResult(status):
    """ Web Scrapping is used for this function since this data isn't available
    in the dataset
    """
    try:
        soup = BeautifulSoup(status[0], 'html.parser')
        nameTag = soup.find('span', {"class": "itemprop"})
        name = str(nameTag.string)

    except:
        print(status[0])
        return ""
    
    allDirectors.append((status[1]+ "\t" + name + "\n"))
    print(status[1])
    print(name)
    return

    

starTime = datetime.now()
q = Queue(concurrent * 2)
for i in range(concurrent):
    t = Thread(target=doWork)
    t.daemon = True
    t.start()
try:
    for url in open("./UniqueWritersUrls_moviesOnly.txt"):
        q.put(url.strip())
    q.join()
except KeyboardInterrupt:
    sys.exit(1)



print(str(datetime.now() - starTime))
#"./FinalDirectors.txt" should now contain a list of unique Director IMDB IDs and the corresponding director name
f = open("./FinalWriters.txt", "w+")
for row in allDirectors:
    f.write(row)
f.close()

f = open("./Data/name.writers_moviesOnly.tsv", "w+")
for row in allDirectors:
    f.write(row)
f.close()
