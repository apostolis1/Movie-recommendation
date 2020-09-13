import csv, sqlite3


def getMovies():
    #returns the IMDB IDs of all the movies in the Data.db
    conn = sqlite3.connect('./Data/Data.db')
    c = conn.cursor()
    c.execute("SELECT * FROM info")
    records = c.fetchall()
    conn.close()
    result = []
    for record in records:
        result.append(record[1])
    return result


def getUniqueWriters(tsvFile, moviesID):
    tsv_file = open(tsvFile)
    read_tsv = csv.reader(tsv_file, delimiter="\t")
    uniqueDirectorIDs = set()
    for row in read_tsv:
        if row[0] in moviesID: #Check if it is a movie
            writersIDs = row[2].split(",")
            for dirID in writersIDs:
                if dirID not in uniqueDirectorIDs and dirID != "\\N":
                    uniqueDirectorIDs.add(dirID)
    tsv_file.close()
    return uniqueDirectorIDs

def getWritersUrls(uniqueWriters):
    f = open(uniqueWriters, "r")
    targetFile = open("./UniqueWritersUrls_moviesOnly.txt", "w+")
    for row in f:
        imdbUrl = "https://www.imdb.com/name/{id}/".format(id = row.strip())
        targetFile.write(imdbUrl + "\n")
    f.close()
    targetFile.close()
    return


def main():
    moviesIDs = set(getMovies())
    f = open("./UniqueWriters.txt", "w+")
    for id in getUniqueWriters("./Data/title.crew.tsv", moviesIDs):
        f.write(id + "\n")
    f.close()
    getWritersUrls("./UniqueWriters.txt")

if __name__ == "__main__":
    main()
