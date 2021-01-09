import mysql.connector
from mysql.connector import Error #for easier access to functions
import pandas as pd

pw = "***************"
db = "playlist"

#establish connection to SQL server
def create_server_connection(host_name, user_name, user_password):
    connection = None #close existing connections

    #attempt to make a connection
    try:
        connection = mysql.connector.connect(host = host_name, user = user_name, passwd = user_password)
        print("MySQL Database connection was successful")
    #print error if there is a connection issue
    except Error as err:
        print(f"Error: '{err}'")
    #return connection object
    return connection

#establish direct connection to specific database in the server
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None #close existing connections

    #attempt to make a connection
    try:
        connection = mysql.connector.connect(host = host_name, user=user_name, passwd = user_password, database = db_name)
        print("MySQL Database connection was successful")

    #print error if there is a connection issue
    except Error as err:
        print(f"Error: '{err}'")
    #return connection object
    return connection

#function used to create a database
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("database has been created successfuly")
    except Error as err:
        print(f"Error: '{err}'")

#function used to execute queries
def execute_query(connection, query):
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        connection.commit() #ensures command in query is implemented
        #print("Query successful")
    except Error as err:
        print(f"Error: {err}'")

#function used to execute a list of queries at once
def execute_list_query(connection, sql, val):
    cursor = connection.cursor()
    try:
        cursor.executemany(sql, val)
        connection.commit()
        #print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

#function to check if table exists to avoid errors
def checkTableExists(dbconnection, tablename):
    dbcursor = dbconnection.cursor()
    dbcursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcursor.fetchone()[0] == 1:
        dbcursor.close()
        return True

    dbcursor.close()
    return False

#function to create desired tables
def create_tables(connection):
    #creating tables
    if(not checkTableExists(connection, 'playlists')):
        create_playlists_table = """
        CREATE TABLE playlists (
        playlist_id INT PRIMARY KEY,
        name VARCHAR(40) NOT NULL,
        num_songs INT NOT NULL,
        duration VARCHAR(20) NOT NULL
        );
        """
        execute_query(connection, create_playlists_table)

    if(not checkTableExists(connection, 'playlist_songs')):
        create_playist_songs_table = """
        CREATE TABLE playlist_songs (
        playlist_id INT NOT NULL,
        song_id INT NOT NULL
        );
        """
        execute_query(connection, create_playist_songs_table)

    if(not checkTableExists(connection, 'songs')):
        create_songs_table = """
        CREATE TABLE songs (
        song_id INT PRIMARY KEY,
        name VARCHAR(200),
        duration VARCHAR(20) NOT NULL,
        channel_Id INT NOT NULL
        );
        """
        execute_query(connection, create_songs_table)


    alter_playlist_songs = """
        ALTER TABLE playlist_songs
        ADD FOREIGN KEY(playlists)
        REFERENCES playlists(playlist_id)
        ADD FOREIGN KEY(songs)
        REFERENCES songs(song_id)
        ON DELETE SET NULL;
    """


#functions to populat individual tables
def pop_playlists_table(connection, query):
    sql = """
    INSERT INTO playlists (playlist_id, name, num_songs, duration) VALUES (%s, %s, %s, %s)
    """

    execute_list_query(connection, sql, query)

def pop_playlist_songs_table(connection, query):
    sql = """
    INSERT INTO playlist_songs (playlist_id, song_id) VALUES (%s, %s)
    """

    execute_list_query(connection, sql, query)

def pop_songs_table(connection, query):
    sql = """
    INSERT INTO songs (song_id, name, duration, channel) VALUES (%s, %s, %s, %s)
    """

    execute_list_query(connection, sql, query)

def main():
    # Connect to the Database
    connection = create_server_connection("localhost", "root", pw)

    create_database_query = "CREATE DATABASE IF NOT EXISTS playlist;"

    create_database(connection, create_database_query)

    connection = create_db_connection("localhost", "root", pw, db)

    create_tables(connection)


if __name__ == "__main__":
    main()
