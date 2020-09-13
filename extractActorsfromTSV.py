import sys, csv, sqlite3


def extractActors(tsvFile):
    conn = sqlite3.connect('./Data/Data.db')
    c = conn.cursor()

    tsv_file = open(tsvFile)
    read_tsv = csv.reader(tsv_file, delimiter="\t")
    for row in read_tsv:
        ID = row[0]
        actors = row[1]
        c.executemany("UPDATE info SET actors = ? WHERE imdbID = ?", [(actors, ID)])
    conn.commit()
    conn.close()
    return

   
if __name__ == "__main__":
    extractActors("./Data/title.actors.tsv")