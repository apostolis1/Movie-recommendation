import sqlite3

def clean_data(x):
    if isinstance(x, list):
        return [str.lower(i.replace(" ", "")) for i in x]
    elif isinstance(x, str):
        mystr = str.lower(x.replace(" ", ""))
        return mystr.replace(",", " ")
    elif isinstance(x, int):
        return str(x)
    else:
        return ''


def makeSoup(DBLocation):

    conn = sqlite3.connect(DBLocation)
    c = conn.cursor()

    c.execute("SELECT * FROM info", ())
    conn.commit()

    recs = c.fetchall()
    # metadata = pd.DataFrame(recs, columns =['id','imdbID','pTitle','oTitle','genres', 'actors', 'director', 'writers', 'keywords','year','soup'])
    # metadata.drop(metadata.columns[[0, 1, 2, 3, 9, 10]], axis=1, inplace=True)
    # print(metadata.head(5))
    # print()
    for rec in recs:
        imdb_ID = rec[1]
        cleanGenres = clean_data(rec[4])
        cleanActors = clean_data(rec[5])
        # cleanActors = "" #TESTING to see what the memory error is
        cleanDirectors = clean_data(rec[6])
        cleanWriters = clean_data(rec[7])
        cleanKeywords = clean_data(rec[8])
        # cleanKeywords = "" #TESTING to see what the memory error is

        Tempsoup = cleanGenres + ' ' + cleanActors + ' ' + cleanDirectors + ' ' + cleanWriters + ' ' + cleanKeywords
        Tempsoup =Tempsoup.rstrip().lstrip()
        soup = Tempsoup.replace("  ", " ")
        c.executemany("UPDATE info SET soup = ? WHERE imdbID = ?",[(soup, imdb_ID)])
    conn.commit()
    conn.close()

    return

if __name__ == "__main__":
    makeSoup("./Data/Data.db")