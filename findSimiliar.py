import pandas as pd
import sqlite3
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from datetime import datetime


def get_recommendations(imdbid, metadata, indices, cosine_sim, offset): #takes as arguments the list of ids, metadata, an array of indices, cosine_sim and the offset to return a valid id
    # Get the index of the movie that matches the title
    idx = indices[imdbid]

    # Get the pairwsie similarity scores of all movies with that movie
    sim_scores = list(enumerate(cosine_sim[idx]))

    # Sort the movies based on the similarity scores
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    # print(sim_scores)
    mydic = {}
    sim_scores=sim_scores
    for i in sim_scores[1:]:
        mydic[i[0]+offset+1] = i[1]
    # for _ in range(5):
    #     print("\n")
    # for i in mydic:
    #     print(i, mydic[i])
    # print(len(mydic))
    return mydic
    # # Get the scores of the 10 most similar movies
    # sim_scores = sim_scores[1:11]

    # # Get the movie indices
    # movie_indices = [i[0] for i in sim_scores]

    # # Return the top 10 most similar movies
    # return metadata['pTitle'].iloc[movie_indices]


def getRecommendation(DBLocation, MovieID, startIdx, endIdx):
    conn = sqlite3.connect(DBLocation)
    c = conn.cursor()

    c.execute("SELECT * FROM info WHERE id > ? AND id <= ? OR id = ?", (startIdx, endIdx, MovieID))


    conn.commit()

    recs = c.fetchall()
    conn.close()
    metadata = pd.DataFrame(recs, columns =['id','imdbID','pTitle','oTitle','genres', 'actors', 'director', 'writers', 'keywords','year','soup'])
    # print(metadata.head(5))
    
    count = CountVectorizer(stop_words='english')
    count_matrix = count.fit_transform(metadata['soup'])
    # print(count_matrix.shape)
    # print(count_matrix)
    cosine_sim2 = cosine_similarity(count_matrix, count_matrix)


    metadata = metadata.reset_index()
    indices = pd.Series(metadata.index, index=metadata['id'])
    # print("Indices", indices)
    return get_recommendations(MovieID, metadata, indices, cosine_sim2, startIdx)



# The function takes as argument the id of the database (int) and returns a list of ids of the database with the similiar movies

def findSimiliar(imdbid ,numberOfResults=25):
    startTime = datetime.now()
    conn = sqlite3.connect("./Data/Data.db")
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM info", ())
    MOVIES_LENGTH = 5000
        
    MOVIES_TO_CHECK = c.fetchone()[0]
    conn.close()
        
    FINAL_RESULT = {}
    movies_checked = 0
    i = 0
    while movies_checked < MOVIES_TO_CHECK:
        # FINAL_RESULT.update(getRecommendation("./Data/Data.db", 247111, i*MOVIES_LENGTH, min(movies_checked+MOVIES_LENGTH+i, MOVIES_TO_CHECK)))

        FINAL_RESULT.update(getRecommendation("./Data/Data.db", imdbid, i*MOVIES_LENGTH, min((i+1)*MOVIES_LENGTH, MOVIES_TO_CHECK)))
        i += 1
        # print("Final Result:", FINAL_RESULT)
        movies_checked=len(FINAL_RESULT) + i #+i for the movie itself
        # print(len(FINAL_RESULT))
    FINAL_RESULT = sorted(FINAL_RESULT.items(), key=lambda x: x[1], reverse=True)
    print("Checked", len(FINAL_RESULT)+1, "movies and these were the top results") #+1 because the movie itself isn't included on the result
        
    conn = sqlite3.connect("./Data/Data.db")
    c = conn.cursor()

    Finalrecords = []
    for i in FINAL_RESULT[:numberOfResults]:
        c.execute("SELECT * FROM info WHERE id = ?", (i[0],))
        conn.commit()
        Finalrecords.append(c.fetchone())
        # print(i[0], i[1])
        
    # for i in Finalrecords:
    #     # print(type(i))
    #     print("Title: {title} ImdbID: {imdbid} Year: {year}".format(title=i[2], imdbid=i[1], year=i[9] ))
    #     # print(i)
    print(str(datetime.now()-startTime))
    # input("Press Enter to continue...")
    return Finalrecords



if __name__ == "__main__":
    # startTime = datetime.now()

    # conn = sqlite3.connect("./Data/Data.db")
    # c = conn.cursor()
    # c.execute("SELECT COUNT(*) FROM info", ())
    # MOVIES_LENGTH = 5000
    
    # MOVIES_TO_CHECK = c.fetchone()[0]
    # conn.close()
    
    # FINAL_RESULT = {}
    # movies_checked = 0
    # i = 0
    # while movies_checked < MOVIES_TO_CHECK:
    #     # FINAL_RESULT.update(getRecommendation("./Data/Data.db", 247111, i*MOVIES_LENGTH, min(movies_checked+MOVIES_LENGTH+i, MOVIES_TO_CHECK)))

    #     FINAL_RESULT.update(getRecommendation("./Data/Data.db", 418903, i*MOVIES_LENGTH, min((i+1)*MOVIES_LENGTH, MOVIES_TO_CHECK)))
    #     i += 1
    #     # print("Final Result:", FINAL_RESULT)
    #     movies_checked=len(FINAL_RESULT) + i #+i for the movie itself
    #     # print(len(FINAL_RESULT))
    # FINAL_RESULT = sorted(FINAL_RESULT.items(), key=lambda x: x[1], reverse=True)
    # print("Checked", len(FINAL_RESULT)+1, "movies and these were the top results") #+1 because the movie itself isn't included on the result
    
    # conn = sqlite3.connect("./Data/Data.db")
    # c = conn.cursor()

    # Finalrecords = []
    # for i in FINAL_RESULT[:25]:
    #     c.execute("SELECT * FROM info WHERE id = ?", (i[0],))
    #     conn.commit()
    #     Finalrecords.append(c.fetchone())
    #     # print(i[0], i[1])
    
    # for i in Finalrecords:
    #     # print(type(i))
    #     print("Title: {title} ImdbID: {imdbid} Year: {year}".format(title=i[2], imdbid=i[1], year=i[9] ))
    #     # print(i)
    # print(str(datetime.now()-startTime))
    findSimiliar(418903)