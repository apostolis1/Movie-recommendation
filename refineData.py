import sqlite3

"""

This scripts removes the faulty parts of the records
Should be run as the final step to prepare the database for creating the metadata

"""

def refineYear(dbPath):
    conn = sqlite3.connect(dbPath)
    c = conn.cursor()

    c.executemany("UPDATE info SET year = NULL WHERE year = ?", [("0")])
    conn.commit()
    conn.close()
    return

def refineGenres(dbPath):
    conn = sqlite3.connect(dbPath)
    c = conn.cursor()

    c.executemany("UPDATE info SET genres=NULL WHERE genres= ?;", [('\\N',)])
    conn.commit()
    conn.close()
    return

def refineKeywords(dbPath):
    conn = sqlite3.connect(dbPath)
    c = conn.cursor()

    c.executemany("UPDATE info SET keywords=NULL WHERE keywords=?;", [('',)])
    conn.commit()
    conn.close()
    return

def main(database):
    refineYear(database)
    refineGenres(database)
    refineKeywords(database)

if __name__ == "__main__":
    main("./Data/Data.db")
