# Movie-recommender

The script creates a database of all the movies that exist in imdb.com. Web scraping is used for most of the data, since it isn't available on a dataset, so we create our own TSV files and then export them to the database.
The files will be updated and refactored since they are kind of a mess right now, but the way the script works is the following:
- Create a database with the data available in the basics IMDB dataset
- Use web scraping (in parallel, so for around 500.000 movies it takes about 15 hours) to get additional data
- Create the soup for each movie
- Use numpy to find similiarities between movies (again in parallel, takes about 15 secs to get the recommendtions)

Obviously the creation of the database only needs to be done once, additionally I am planning to upload the database so that you can just download it, put it in the folder of the script and be ready to go. Until then, you have to run the scripts to create it in your own. Also the option to create the database from the TSV files will be added.

# Demo

This is a demo of the application. Notice that every field in the main window can be used in order to search for the movie (eg Name, Year, ID etc) and the fields work together to (eg in the demo we use name + actor)

![] (demo/demo.gif) 

# Libraries and Frameworks

- BeautifulSoup and requests module for web scaping
- Multiprocessing for parallel manipulation of data
- PyQt for the Gui part
- Sqlite3 for the database.
