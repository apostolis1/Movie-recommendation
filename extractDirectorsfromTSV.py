import sqlite3, csv


"""
This file handles the proccess of updating the database when both the "./Data/crew.tsv" and "./Data/name.directors.tsv" exist
These files should have already been created
"""

def createDictionary(tsvFile):
    tsv_file = open(tsvFile)
    directors = {}
    read_tsv = csv.reader(tsv_file, delimiter="\t")
    for row in read_tsv:
        imdbID = row[0]
        name = row[1]
        directors[imdbID] = name
    tsv_file.close()
    return directors

def updateDB(directorDict):
    #Updates the directors of each entry of the database. params: dictionary of directors returns: none
    conn = sqlite3.connect('./Data/Data.db')
    c = conn.cursor()
    tsv_file = open("./Data/title.crew.tsv")
    read_tsv = csv.reader(tsv_file, delimiter = "\t")
    moviesDirectors = {}
    for row in read_tsv:
        try:
            imdbID = row[0]
            directorsIDs = row[1].split(",")
            finalList = []
            for id in directorsIDs:
                finalList.append(directorDict[id])
            finalStr = ",".join(finalList)
            moviesDirectors[imdbID] = finalStr
        #exceptions occur when there are no directors and the field is "\N"
        except:
            pass
    tsv_file.close()
    
    c.execute("SELECT * FROM info")
    data = c.fetchall()
    for row in data:
        row_IMDBid = row[1]
        try:
            strDirectors = moviesDirectors[row_IMDBid]
            c.executemany("UPDATE info SET director = ? WHERE imdbID = ?", [(strDirectors, row_IMDBid)])
        except:
        #Exceptions when there were no info on the title.crew.tsv file for this movie (field was "\N")
            print(row_IMDBid)
    conn.commit()
    conn.close()
    return


def main():
    try:
        directors = createDictionary("./Data/name.directors.tsv")
        updateDB(directors)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()