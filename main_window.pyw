# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'Main_window.ui'
#
# Created by: PyQt5 UI code generator 5.13.2
#
# WARNING! All changes made in this file will be lost!


from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import Qt
import sqlite3
import sys
# from FindSimiliarParallel import findSimiliar
import time
##############################################################################################

#ADD EVERYTHING IN HERE, IT WORKS THIS WAY
#You can also import the finSimliar function from the FindSimiliarParallel and try to make it work, I didn't have the time
#It should generally be avoided mixing GUI and Functionality



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



##########################################################################

class TableModel(QtCore.QAbstractTableModel):
    def __init__(self, data):
        super(TableModel, self).__init__()
        self._data = data

    def data(self, index, role):
        if role == Qt.DisplayRole:
            # See below for the nested-list data structure.
            # .row() indexes into the outer list,
            # .column() indexes into the sub-list
            return self._data[index.row()][index.column()]

    def rowCount(self, index):
        # The length of the outer list.
        return len(self._data)

    def columnCount(self, index):
        # The following takes the first sub-list, and returns
        # the length (only works if all rows are an equal length)
        return len(self._data[0])


class PopupView(QtWidgets.QMainWindow):
    def __init__(self, data):
        super().__init__()

        self.table = QtWidgets.QTableView()
        # self.table.setSortingEnabled(True)
        # header = QtWidgets.QHeaderView(self)
        # self.table.setHorizontalHeader(header)
        self.model = TableModel(data)
        self.table.setModel(self.model)

        self.setCentralWidget(self.table)
        self.setMinimumSize(self.table.size())


##############################################################################################





class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(794, 667)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.gridLayoutWidget = QtWidgets.QWidget(self.centralwidget)
        self.gridLayoutWidget.setGeometry(QtCore.QRect(10, 0, 781, 47))
        self.gridLayoutWidget.setObjectName("gridLayoutWidget")
        self.gridLayout = QtWidgets.QGridLayout(self.gridLayoutWidget)
        self.gridLayout.setSizeConstraint(QtWidgets.QLayout.SetMaximumSize)
        self.gridLayout.setContentsMargins(0, 0, 0, 0)
        self.gridLayout.setObjectName("gridLayout")
        self.label_3 = QtWidgets.QLabel(self.gridLayoutWidget)
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(10)
        font.setBold(True)
        font.setWeight(75)
        self.label_3.setFont(font)
        self.label_3.setAlignment(QtCore.Qt.AlignCenter)
        self.label_3.setObjectName("label_3")
        self.gridLayout.addWidget(self.label_3, 0, 2, 1, 1, QtCore.Qt.AlignHCenter)
        self.director_lineEdit = QtWidgets.QLineEdit(self.gridLayoutWidget)
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(10)
        self.director_lineEdit.setFont(font)
        self.director_lineEdit.setAlignment(QtCore.Qt.AlignCenter)
        self.director_lineEdit.setObjectName("director_lineEdit")
        self.gridLayout.addWidget(self.director_lineEdit, 1, 2, 1, 1)
        self.actors_lineEdit = QtWidgets.QLineEdit(self.gridLayoutWidget)
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(10)
        self.actors_lineEdit.setFont(font)
        self.actors_lineEdit.setAlignment(QtCore.Qt.AlignCenter)
        self.actors_lineEdit.setObjectName("actors_lineEdit")
        self.gridLayout.addWidget(self.actors_lineEdit, 1, 3, 1, 1)
        self.title_lineEdit = QtWidgets.QLineEdit(self.gridLayoutWidget)
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(10)
        self.title_lineEdit.setFont(font)
        self.title_lineEdit.setAlignment(QtCore.Qt.AlignCenter)
        self.title_lineEdit.setObjectName("title_lineEdit")
        self.gridLayout.addWidget(self.title_lineEdit, 1, 0, 1, 1)
        self.label_4 = QtWidgets.QLabel(self.gridLayoutWidget)
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(10)
        font.setBold(True)
        font.setWeight(75)
        self.label_4.setFont(font)
        self.label_4.setAlignment(QtCore.Qt.AlignCenter)
        self.label_4.setObjectName("label_4")
        self.gridLayout.addWidget(self.label_4, 0, 3, 1, 1)
        self.label_2 = QtWidgets.QLabel(self.gridLayoutWidget)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.label_2.sizePolicy().hasHeightForWidth())
        self.label_2.setSizePolicy(sizePolicy)
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(10)
        font.setBold(True)
        font.setWeight(75)
        self.label_2.setFont(font)
        self.label_2.setAlignment(QtCore.Qt.AlignCenter)
        self.label_2.setObjectName("label_2")
        self.gridLayout.addWidget(self.label_2, 0, 1, 1, 1)
        self.pushButton = QtWidgets.QPushButton(self.gridLayoutWidget)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Expanding)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.pushButton.sizePolicy().hasHeightForWidth())
        self.pushButton.setSizePolicy(sizePolicy)
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(10)
        self.pushButton.setFont(font)
        self.pushButton.setObjectName("pushButton")
        self.gridLayout.addWidget(self.pushButton, 0, 5, 2, 1)
        self.label = QtWidgets.QLabel(self.gridLayoutWidget)
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(10)
        font.setBold(True)
        font.setWeight(75)
        self.label.setFont(font)
        self.label.setTextFormat(QtCore.Qt.PlainText)
        self.label.setAlignment(QtCore.Qt.AlignCenter)
        self.label.setObjectName("label")
        self.gridLayout.addWidget(self.label, 0, 0, 1, 1)
        self.year_lineEdit = QtWidgets.QLineEdit(self.gridLayoutWidget)
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(10)
        self.year_lineEdit.setFont(font)
        self.year_lineEdit.setAlignment(QtCore.Qt.AlignCenter)
        self.year_lineEdit.setObjectName("year_lineEdit")
        self.gridLayout.addWidget(self.year_lineEdit, 1, 1, 1, 1)
        self.label_5 = QtWidgets.QLabel(self.gridLayoutWidget)
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(10)
        font.setBold(True)
        font.setWeight(75)
        self.label_5.setFont(font)
        self.label_5.setTextFormat(QtCore.Qt.PlainText)
        self.label_5.setAlignment(QtCore.Qt.AlignCenter)
        self.label_5.setObjectName("label_5")
        self.gridLayout.addWidget(self.label_5, 0, 4, 1, 1)
        self.imdbID_lineEdit = QtWidgets.QLineEdit(self.gridLayoutWidget)
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(10)
        self.imdbID_lineEdit.setFont(font)
        self.imdbID_lineEdit.setAlignment(QtCore.Qt.AlignCenter)
        self.imdbID_lineEdit.setObjectName("imdbID_lineEdit")
        self.gridLayout.addWidget(self.imdbID_lineEdit, 1, 4, 1, 1)
        self.databaseDisplayTable = QtWidgets.QTableWidget(self.centralwidget)
        self.databaseDisplayTable.setGeometry(QtCore.QRect(10, 50, 781, 521))
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(10)

        self.databaseDisplayTable.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.databaseDisplayTable.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        self.databaseDisplayTable.setFont(font)
        self.databaseDisplayTable.setObjectName("databaseDisplayTable")
        self.databaseDisplayTable.setColumnCount(6)
        self.databaseDisplayTable.setRowCount(0)

        item = QtWidgets.QTableWidgetItem()
        self.databaseDisplayTable.setHorizontalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.databaseDisplayTable.setHorizontalHeaderItem(1, item)
        item = QtWidgets.QTableWidgetItem()
        self.databaseDisplayTable.setHorizontalHeaderItem(2, item)
        item = QtWidgets.QTableWidgetItem()
        self.databaseDisplayTable.setHorizontalHeaderItem(3, item)
        item = QtWidgets.QTableWidgetItem()
        self.databaseDisplayTable.setHorizontalHeaderItem(4, item)
        item = QtWidgets.QTableWidgetItem()
        self.databaseDisplayTable.setHorizontalHeaderItem(5, item)


        header = self.databaseDisplayTable.horizontalHeader()       
        header.setSectionResizeMode(0, QtWidgets.QHeaderView.ResizeToContents)
#       header.setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeToContents)

        header.setSectionResizeMode(1, QtWidgets.QHeaderView.Stretch)
        header.setSectionResizeMode(2, QtWidgets.QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QtWidgets.QHeaderView.Stretch)
        header.setSectionResizeMode(4, QtWidgets.QHeaderView.Stretch)
        header.setSectionResizeMode(5, QtWidgets.QHeaderView.ResizeToContents)


        self.getRecommendationsBtn = QtWidgets.QPushButton(self.centralwidget)
        self.getRecommendationsBtn.setGeometry(QtCore.QRect(10, 575, 781, 51))
        font = QtGui.QFont()
        font.setFamily("Arial")
        font.setPointSize(12)
        font.setBold(True)
        font.setWeight(75)
        self.getRecommendationsBtn.setFont(font)
        self.getRecommendationsBtn.setObjectName("getRecommendationsBtn")
        MainWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 794, 21))
        self.menubar.setObjectName("menubar")
        self.menuHelp = QtWidgets.QMenu(self.menubar)
        self.menuHelp.setObjectName("menuHelp")
        MainWindow.setMenuBar(self.menubar)
        self.menuHelp.addSeparator()
        self.menubar.addAction(self.menuHelp.menuAction())

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

        self.pushButton.clicked.connect(self.search)
        self.getRecommendationsBtn.clicked.connect(self.getReccomendations)

        self.popup = None


    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "MainWindow"))

        item = self.databaseDisplayTable.horizontalHeaderItem(0)
        item.setText(_translate("MainWindow", "ID"))
        item = self.databaseDisplayTable.horizontalHeaderItem(1)
        item.setText(_translate("MainWindow", "Title"))
        item = self.databaseDisplayTable.horizontalHeaderItem(2)
        item.setText(_translate("MainWindow", "Year"))
        item = self.databaseDisplayTable.horizontalHeaderItem(3)
        item.setText(_translate("MainWindow", "Director"))
        item = self.databaseDisplayTable.horizontalHeaderItem(4)
        item.setText(_translate("MainWindow", "Actors"))
        item = self.databaseDisplayTable.horizontalHeaderItem(5)
        item.setText(_translate("MainWindow", "IMDB ID"))

        self.label_3.setText(_translate("MainWindow", "Director"))
        self.label_4.setText(_translate("MainWindow", "Actors"))
        self.label_2.setText(_translate("MainWindow", "Year"))
        self.pushButton.setText(_translate("MainWindow", "Search"))
        self.label.setText(_translate("MainWindow", "Title"))
        self.label_5.setText(_translate("MainWindow", "IMDB ID"))
        self.getRecommendationsBtn.setText(_translate("MainWindow", "Get Recommendations"))
        self.menuHelp.setTitle(_translate("MainWindow", "Help"))



    def search(self):
        year = "%"+self.year_lineEdit.text().strip().replace(" ", "_")+"%"
        boolYear = self.year_lineEdit.text().strip() == ""

        title = "%"+self.title_lineEdit.text().strip().replace(" ", "_")+"%"

        actors = "%"+self.actors_lineEdit.text().strip().replace(" ", "_")+"%"
        boolActors = self.actors_lineEdit.text().strip() == ""

        director = "%"+self.director_lineEdit.text().strip().replace(" ", "_")+"%"
        boolDirector = self.director_lineEdit.text().strip() == ""


        imdbid = "%"+self.imdbID_lineEdit.text().strip().replace(" ", "_")+"%"
        imdbid = "%"+self.imdbID_lineEdit.text().strip()+"%"

        try:
            conn = sqlite3.connect("./Data/Data.db")
            c = conn.cursor()
            c.execute("""SELECT id, primaryTitle, year, director, actors, imdbID FROM info WHERE
             primaryTitle LIKE ?
              AND (year LIKE ? OR (year ISNULL AND ? ))
              AND (actors LIKE ? OR (actors ISNULL AND ?)) 
              AND (director LIKE ? OR (director ISNULL AND ?)) 
              AND  imdbID LIKE ?""", (title, year, boolYear ,actors, boolActors, director, boolDirector, imdbid))
            conn.commit()
            results = c.fetchall()
            # for i in results:
            #     print(i)
            #print(len(results))


            #Now display the data that exist in results:
            while self.databaseDisplayTable.rowCount() > 0:
                self.databaseDisplayTable.removeRow(0)

            for row_number, row_data in enumerate(results):
                self.databaseDisplayTable.insertRow(row_number)
                for column_number, data in enumerate(row_data):
                    if column_number != len(row_data)-1:
                        self.databaseDisplayTable.setItem(row_number, column_number, QtWidgets.QTableWidgetItem(str(data)))
                    else:
                        imdbURL = "www.imdb.com/title/" + str(data)
                        self.databaseDisplayTable.setItem(row_number, column_number, QtWidgets.QTableWidgetItem(imdbURL))

        except Exception as e:
            print("Error while quering data")
            print(e)
            return
        return


    def getReccomendations(self):
        try:
            item = self.databaseDisplayTable.item(self.databaseDisplayTable.currentRow(), 0)
            imdbid = int(item.text())
            print(item.text())
            results = findSimiliar(imdbid)
            # while len(results) < 25:
            #     pass
            for i in results:
                print("Title: {title} ImdbID: {imdbid} Year: {year}".format(title=i[2], imdbid=i[1], year=i[9] ))
        except Exception as e:
            print(e)
        refinedResults = []
        """
        Pass only the following values to the Popup window:
        Title, year, director, actors, genres, imdbID
        """
        for row in results:
            tempRow = []
            for i in [2,9,6,5,4]:
                tempRow.append(row[i])
            tempRow.append("www.imdb.com/title/"+row[1])
            refinedResults.append(tempRow)
        
        try:
            self.popup.close()
        except:
            pass
        self.popup = PopupView(refinedResults)
        self.popup.setWindowTitle("Recommendations")
        self.popup.show()



if __name__ == "__main__":
    import sys
    import multiprocessing as mp

    mp.freeze_support()
    pool = mp.Pool(mp.cpu_count())

    Final_results = {}

    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())
