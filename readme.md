# Rap-Caviar ETL

How to Run Code:

1). Open spot folder in your IDE of choice
2). Type in py rap-caviar.py in your terminal

Overview of the Script:

1). The spotipy API requests data from the "rap_caviar" playlist
2). Data from the "rap_caviar" playlist is processed and written to dictionaries and then transformed into dataframes.
3). A sqlite database is created in sqllite-db/pythonsqlite.db
4). "tracks" and "artists" tables are created in pythonsqlite.db if they don't already exist and dataframe data is inserted into these tables.
5). Data is queried from the pythonsqlite.db
5). The following metrics are printed to the terminal and written to "analytics.txt" in the "analytics"  folder :

    a). Number of songs in the playlist
    b). Top 5 tracks by track popularity
    c). The longest song

7). Correlation between track duration and popularity is plotted using plotly . A new browser should open with the scatter plot I have created. A png image of this scatter plot is also in the "analytics" folder.