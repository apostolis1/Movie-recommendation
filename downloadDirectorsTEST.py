import csv


""""
Reads the Unique Directors IDs from the "./UniqueDirectors.txt" file and creates a new one ("./UniqueDirectorsUrls.txt")
which contains the corresponding urls (strings) one in each row


TO BE COMBINED IN ONE MASTER DOWNLOAD DIRECTORS FILE (after the directors.py proccess is done, so that "./UniqueDirectors.txt" file is created)

"""
def getDirectorsUrls():
    f = open("./UniqueDirectors.txt", "r")
    targetFile = open("./UniqueDirectorsUrls.txt", "w+")
    for row in f:
        imdbUrl = "https://www.imdb.com/name/{id}/".format(id = row.strip())
        targetFile.write(imdbUrl + "\n")
    f.close()
    targetFile.close()
    return

def main():
    getDirectorsUrls()

if __name__ == "__main__":
    main()