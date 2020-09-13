""""
    Scrap the web, connect to the DB and update DB
    IMDB Dataset Url = https://datasets.imdbws.com/
    The title.basics.tsv.gz dataset contains the IMDB id, titleType (eg movie, series etc.), genres


"""

import os, requests
import gzip
import shutil
import math
from bs4 import BeautifulSoup
import sqlite3
import csv
from datetime import datetime

def getDataset_title_basics():
    
    #Create Data folder if it doesn't exist
    cwd = os.getcwd()
    if not os.path.isdir("./Data"):
        os.mkdir("Data")

    
    datasetPath = cwd + "/Data/title.basics.tsv.gz"
    titleBasicsUrl = "https://datasets.imdbws.com/title.basics.tsv.gz"
    extractedDatasetPath = cwd + "/Data/title.basics.tsv"

    print("Downloading title.basics.tsv from IMDB", titleBasicsUrl)
    r = requests.get(titleBasicsUrl)
    try:
        with open(datasetPath, 'wb') as f:
            f.write(r.content)    
            f.close()
        print("Successfully downloaded title.basics.tsv from", titleBasicsUrl)
    except:
        print("Failed to download title.basics.tsv from", titleBasicsUrl)

    print("Extracting title.basics.tsv")
    try:
        with gzip.open(datasetPath, 'rb') as f_in:
            with open(extractedDatasetPath, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print("Successfully extracted title.basics.tsv")
    except:
        print("Failed to extract title.basics.tsv")


def getCastFromImdb(imdbID):

    """Web Scrapping is used for this function since this data isn't available
    in the dataset
    Returns a list of actors (with spaces, eg " Weiwei Si") given a imdb ID"""
    try:
        castUrl = "https://www.imdb.com/title/{id}/fullcredits/".format(id = imdbID)
        resp = requests.get(castUrl)

        soup = BeautifulSoup(resp.text, 'lxml')
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
        return []
    return actors

def getCrewFromImdb():
    
    #Create Data folder if it doesn't exist
    cwd = os.getcwd()
    if not os.path.isdir("./Data"):
        os.mkdir("Data")

    
    datasetPath = cwd + "/Data/title.crew.tsv.gz"
    titleBasicsUrl = "https://datasets.imdbws.com/title.crew.tsv.gz"
    extractedDatasetPath = cwd + "/Data/title.crew.tsv"

    print("Downloading title.crew.tsv from IMDB", titleBasicsUrl)
    r = requests.get(titleBasicsUrl)
    try:
        with open(datasetPath, 'wb') as f:
            f.write(r.content)    
            f.close()
        print("Successfully downloaded title.crew.tsv from", titleBasicsUrl)
    except:
        print("Failed to download title.crew.tsv from", titleBasicsUrl)

    print("Extracting title.crew.tsv")
    try:
        with gzip.open(datasetPath, 'rb') as f_in:
            with open(extractedDatasetPath, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print("Successfully extracted title.crew.tsv")
    except:
        print("Failed to extract title.crew.tsv")

def getKeywordsFromImdb(imdbID):

    """ Web Scrapping is used for this function since this data isn't available
        in the dataset
        Returns a list of keywords given a imdb ID
        Returns empty list if keywords can't be found
    """
    try:
        castUrl = "https://www.imdb.com/title/{id}/keywords/".format(id = imdbID)
        resp = requests.get(castUrl)

        soup = BeautifulSoup(resp.text, 'lxml')
        table_tags = soup.find_all('td', {"class": "soda sodavote"})
        keywords = []
        for i in table_tags:
            keywords.append(i["data-item-keyword"])
        print(len(keywords))
    except:
        return []
    return keywords

def getNameFromImdb(imdbID):
    """Web Scrapping is used for this function since this data isn't available
        in the dataset
        Returns a name given a name imdbID (eg nm0410331)
        If there is an exception (eg wrong imdbID) it return nan (Not a Number)
    """
    try:
        castUrl = "https://www.imdb.com/name/{id}/".format(id = imdbID)
        resp = requests.get(castUrl)

        soup = BeautifulSoup(resp.text, 'lxml')
        nameTag = soup.find('span', {"class": "itemprop"})
        name = str(nameTag.string)
        print(name)
    except:
        print("Failed to find a name for this id:", imdbID)
        return math.nan
    return name

def createDB():

    if not os.path.isdir("./Data"):
        os.mkdir("Data")
    conn = sqlite3.connect('./Data/Data.db')
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS info (
    id integer PRIMARY KEY,
	imdbID integer ,
	primaryTitle text ,
	originalTitle text,
	genres text,
    actors text,
    director text,
    writers text,
    keywords text,
    year integer,
    soup text,
    UNIQUE(imdbID)
);""")
    return

def extractBasicTsvData(tsvFile):
    tsv_file = open(tsvFile)
    read_tsv = csv.reader(tsv_file, delimiter="\t")

    """A row now looks like this:
        ['tt0000009', 'movie', 'Miss Jerry', 'Miss Jerry', '0', '1894', '\\N', '45', 'Romance']
        where:
        [ImdbID, type, primaryTitle, originalTitle, isAdult, startYear, endYear, runtime, genres]
        More info can be found there: https://www.imdb.com/interfaces/
    """
    counter = 0
    movies = []
    conn = sqlite3.connect('./Data/Data.db')
    c = conn.cursor()

    for row in read_tsv:
        if row[1] == 'movie':
            movies.append(row)
            # c.execute("INSERT OR IGNORE INTO info(imdbID, primaryTitle, originalTitle, genres) VALUES(:id, :pTitle, :oTitle, :gens)",{
            #     'id': row[0],
            #     'pTitle': row[2],
            #     'oTitle': row[3],
            #     'gens': row[8]   
            # }
            # )
            print("Processing row", counter)
            counter += 1
            try:
                yearValue = int(row[5])
            except:
                yearValue = 0
            c.executemany("INSERT OR IGNORE INTO info(imdbID, primaryTitle, originalTitle, genres, year) VALUES(?,?,?,?,?)", [(row[0], row[2] ,row[3], row[8], yearValue)])
    conn.commit()
    conn.close()
    tsv_file.close()
    print("# of movies:", len(movies))
    return 


def extractActorsToDB():

    conn = sqlite3.connect('./Data/Data.db')
    c = conn.cursor()
    c.execute('SELECT * FROM info')
    records = c.fetchall()
    for row in records:
        imdbID = row[1]
        print(imdbID)
        actors = getCastFromImdb(imdbID)
        strActors = str(','.join(actors))
        print(strActors)
        c.executemany("""UPDATE info
SET actors = ?
WHERE imdbID = ?""", [(strActors, imdbID)])
    conn.commit()
    conn.close()
    return


def main():
    # StartTime = datetime.now()
    # # getDataset_title_basics()
    # #getCastFromImdb("tt1074638")
    # # getCrewFromImdb()
    # # getKeywordsFromImdb("tt1937149")
    # # getNameFromImdb("nm0617588")
    # # createDB()
    # # extractBasicTsvData("./Data/title.basics.tsv")
    # extractActorsToDB()
    # # print(getKeywordsFromImdb("tt0085714"))
    # print(str(datetime.now()-StartTime))
    print(getCastFromImdb("tt1074638"))
    return


if __name__ == "__main__":
    main()