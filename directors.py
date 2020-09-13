import csv

"""
Creates a "./UniqueDirectors.txt" file which contains the unique IMDB IDs (string), one in each row
This proccess is important so that we don't check a name ID twice and make extra http requests that take time

TO BE COMBINED IN ONE MASTER DOWNLOAD DIRECTORS FILE
"""
def getUniqueDirectors(tsvFile):
    tsv_file = open(tsvFile)
    read_tsv = csv.reader(tsv_file, delimiter="\t")
    uniqueDirectorIDs = set()
    for row in read_tsv:
        directorIDs = row[1].split(",")
        for dirID in directorIDs:
            if dirID not in uniqueDirectorIDs and dirID != "\\N":
                print(dirID)
                uniqueDirectorIDs.add(dirID)
    tsv_file.close()
    return uniqueDirectorIDs

def main():
    f = open("./UniqueDirectors.txt", "w+")
    for id in getUniqueDirectors("./Data/title.crew.tsv"):
        f.write(id + "\n")
    f.close()

if __name__ == "__main__":
    main()
