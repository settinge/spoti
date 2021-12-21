
import spotipy
import spotipy.util as util
import os
from config import SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET, SPOTIPY_REDIRECT_URL, RAP_CAVIAR, USER
from collections import defaultdict
import sqlite3
from sqlite3 import Error
import pandas as pd
from sqlalchemy import create_engine
from beautifultable import BeautifulTable
from sqlite3 import OperationalError
import plotly.express as px

def extract_rap_data():
    USERNAME = USER
    RAP_CAV = RAP_CAVIAR
    SCOPE = 'playlist-read-private'
    os.environ['SPOTIPY_CLIENT_ID'] = SPOTIPY_CLIENT_ID
    os.environ['SPOTIPY_CLIENT_SECRET'] = SPOTIPY_CLIENT_SECRET
    os.environ['SPOTIPY_REDIRECT_URI'] = SPOTIPY_REDIRECT_URL

    token = util.prompt_for_user_token(USERNAME, SCOPE)
    if token:
        sp = spotipy.Spotify(auth=token)
        rap_caviar_playlist_items = sp.playlist(RAP_CAV)
        return rap_caviar_playlist_items

        
def process_rap_data():
    tracks_dict = defaultdict(list)
    artists_dict = defaultdict(list)

    rap_caviar_playlist_items = extract_rap_data()

    for i in range(0,len(rap_caviar_playlist_items['tracks']['items'])-1):
        tracks_dict['id'].append(rap_caviar_playlist_items['tracks']['items'][i]['track']['id'])
        tracks_dict['name'].append(rap_caviar_playlist_items['tracks']['items'][i]['track']['name'])
        tracks_dict['popularity'].append(rap_caviar_playlist_items['tracks']['items'][i]['track']['popularity'])
        tracks_dict['duration_ms'].append(rap_caviar_playlist_items['tracks']['items'][i]['track']['duration_ms'])
        tracks_dict['artist_name'].append(rap_caviar_playlist_items['tracks']['items'][i]['track']['artists'][0]['name'])
        tracks_dict['artist_id'].append(rap_caviar_playlist_items['tracks']['items'][i]['track']['artists'][0]['id'])
        artists_dict['id'].append(rap_caviar_playlist_items['tracks']['items'][i]['track']['artists'][0]['id'])
        artists_dict['name'].append(rap_caviar_playlist_items['tracks']['items'][i]['track']['artists'][0]['name'])

    tracks_df=pd.DataFrame.from_dict(tracks_dict,orient='index').transpose()
    artists_df =pd.DataFrame.from_dict(artists_dict,orient='index').transpose()
    
    return tracks_df, artists_df

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def load_rap_data():
    filepath = os.path.realpath('../spoti/sqllite-db/pythonsqlite.db')
    create_connection(filepath)
    filepath = os.path.join(filepath,'', '')
    filepath = ('sqlite:///'+filepath)
    tracks_df, artists_df = process_rap_data()
    engine = create_engine(filepath, echo=False)
    filepath = os.path.realpath('../spoti/sqllite-db/pythonsqlite.db')
    # filepath = os.path.join(filepath)
    con = sqlite3.connect(filepath)
    cur = con.cursor() 
    try:
        cur.execute("select * from tracks") 
    except:
        OperationalError("no such table: tracks")
        tracks_df.to_sql('tracks', con=engine)
    try:
        cur.execute("select * from artists")
    except:
        OperationalError("no such table: tracks")
        artists_df.to_sql('artists', con=engine)    

    return filepath, con, cur 

def analytical_queries():
    # load_rap_data()
    output_file = os.path.join("analytics/analytics.txt")
    table = BeautifulTable()
    table.column_headers = ["track", "popularity"]
    popularity = []
    duration = []

    with open(output_file, "w") as datafile:
        filepath, con, cur = load_rap_data()

        cur.execute('SELECT count(name) FROM tracks')
        number_songs = cur.fetchall()[0][0]
        print(f"There are {number_songs} songs in the playlist")
        datafile.write("1).There are " + str(number_songs) +" songs in the playlist")

        cur.execute('SELECT popularity,name from tracks order by popularity desc limit 5')
        top_five_songs = cur.fetchall()
        for i in top_five_songs:
            table.append_row([i[1],i[0]])
        print(table)
        table_string= table.get_string()
        datafile.write("2) " +table_string +"\n \n")

        cur.execute('SELECT name, duration_ms from tracks order by duration_ms desc limit 1')
        longest_song = cur.fetchone()
        print(f"{longest_song[0]} is the longest track with a duration of {longest_song[1]} ms")
        datafile.write("3) " + longest_song[0] +" is the longest track with a duration of " + str(longest_song[1]) +" ms")

        cur.execute('SELECT popularity, duration_ms from tracks')
        pop_duration = cur.fetchall()
        for i in pop_duration:
            duration.append(i[1])
            popularity.append(i[0])

        fig = px.scatter(x=duration, y=popularity)

        fig.update_layout(
        title="Correlation between Track Duration and Popularity",
        xaxis_title=" Track Duration (ms)",
        yaxis_title=" Poplularity",
        font=dict(
            family="Times new roman",
            size=18,
            color="black"
        )
                    )
        fig.show()

analytical_queries()