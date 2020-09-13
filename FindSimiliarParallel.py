import pandas as pd
import sqlite3
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from datetime import datetime
import multiprocessing as mp


def get_recommendations(imdbid, metadata, indices, cosine_sim, offset): #takes as arguments the list of ids, metadata, an array of indices, cosine_sim and the offset to return a valid id
    # Get the index of the movie that matches the title
    idx = indices[imdbid]

    # Get the pairwsie similarity scores of all movies with that movie
    sim_scores = list(enumerate(cosine_sim[idx]))

    # Sort the movies based on the similarity scores
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    mydic = {}
    sim_scores=sim_scores
    for i in sim_scores[1:]:
        mydic[i[0]+offset+1] = i[1]
    return mydic

def getRecommendation(DBLocation, MovieID, startIdx, endIdx):
    conn = sqlite3.connect(DBLocation)
    c = conn.cursor()

    c.execute("SELECT * FROM info WHERE id > ? AND id <= ? OR id = ?", (startIdx, endIdx, MovieID))


    conn.commit()

    recs = c.fetchall()
    conn.close()
    metadata = pd.DataFrame(recs, columns =['id','imdbID','pTitle','oTitle','genres', 'actors', 'director', 'writers', 'keywords','year','soup'])
    
    count = CountVectorizer(stop_words='english')
    count_matrix = count.fit_transform(metadata['soup'])
    cosine_sim2 = cosine_similarity(count_matrix, count_matrix)


    metadata = metadata.reset_index()
    indices = pd.Series(metadata.index, index=metadata['id'])

    return get_recommendations(MovieID, metadata, indices, cosine_sim2, startIdx)


def collect_result(result):
    global Final_results
    Final_results.update(result)


def findSimiliar(imdbid ,numberOfResults=25):
    mp.freeze_support()
    startTime = datetime.now()
    conn = sqlite3.connect("./Data/Data.db")
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM info", ())
    MOVIES_LENGTH = 5000
        
    MOVIES_TO_CHECK = c.fetchone()[0]
    conn.close()
        
    movies_checked = 0
    i = 0
    global pool
    while movies_checked < MOVIES_TO_CHECK:
        pool.apply_async(getRecommendation, args=("./Data/Data.db", imdbid, i*MOVIES_LENGTH, min((i+1)*MOVIES_LENGTH, MOVIES_TO_CHECK)), callback=collect_result)
        movies_checked = min((i+1)*MOVIES_LENGTH, MOVIES_TO_CHECK)
        i += 1
            
    pool.close()
    pool.join()
    FINAL_RESULT = sorted(Final_results.items(), key=lambda x: x[1], reverse=True)
    print("Checked", len(FINAL_RESULT)+1, "movies and these were the top results") #+1 because the movie itself isn't included on the result
        
    conn = sqlite3.connect("./Data/Data.db")
    c = conn.cursor()

    Finalrecords = []
    for i in FINAL_RESULT[:numberOfResults]:
        c.execute("SELECT * FROM info WHERE id = ?", (i[0],))
        conn.commit()
        Finalrecords.append(c.fetchone())
    
    for i in Finalrecords:
        print("Title: {title} ImdbID: {imdbid} Year: {year}".format(title=i[2], imdbid=i[1], year=i[9] ))

    print(str(datetime.now()-startTime))
    # input("Press Enter to continue...")
    return Finalrecords

if __name__ == "__main__":

    mp.freeze_support()
    pool = mp.Pool(mp.cpu_count())

    Final_results = {}


    findSimiliar(418903)