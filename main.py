from googleapiclient.discovery import build
import re
from datetime import timedelta
import json
from database import create_server_connection, create_db_connection, pop_playlist_songs_table, pop_playlists_table, pop_songs_table
#import os

#api_key = os.environ.get('YT_API_KEY')


pw = "Bilal2bilal123"
db = "playlist"
api_key = 'AIzaSyCNGIMaSdGXn_ELLXoRjK8y8po5jb-9w-s'
my_id = 'UCbBa_kxWNoW94_oazoZRsLg'
pl_id = 'PL_mprygIH1ZJfpsZzs2QWuCJ3kX4mYpsI'


def main():

    youtube = build('youtube', 'v3', developerKey = api_key)
    pl_info = []
    titles = []

    pl_info_request = youtube.playlists().list(
        part ="contentDetails, snippet",
        id = pl_id,
        fields = "items/snippet(title), items/contentDetails(itemCount)"
    )
    pl_info_response = pl_info_request.execute()

    nextPageToken = None

    hours_pattern = re.compile(r'(\d+)H')
    minutes_pattern = re.compile(r'(\d+)M')
    seconds_pattern = re.compile(r'(\d+)S')

    song_ids = []
    song_names = []
    song_dur = []
    song_dur_mins = []
    pub_dates = []
    channel_ids = []
    total_seconds = 0

    while True:
        i=0
        pl_request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=pl_id,
            maxResults = 50,
            pageToken = nextPageToken
            #fields="pageInfo, items/snippet(publishedAt, title, description)"
        )

        pl_response = pl_request.execute()

        #pub_dates = []
            #pubd = item['snippet']['publishedAt']
            #print(json.dumps(item, ensure_ascii = True))
        vid_ids = []

        for item in pl_response["items"]:
            #print(json.dumps(item, ensure_ascii = True))
            song_ids.append(item["snippet"]["position"]+1)
            vid_ids.append(item['contentDetails']['videoId'])

        ','.join(vid_ids)


        video_request = youtube.videos().list(
            part = "contentDetails, snippet",
            id = ','.join(vid_ids)
        )

        video_response = video_request.execute()

        for item in video_response["items"]:
            duration = item["contentDetails"]["duration"]
            song_names.append(item["snippet"]["title"])
            song_dur.append(duration)
            channel_ids.append(item["snippet"]["channelTitle"])

            hours = hours_pattern.search(duration)
            minutes = minutes_pattern.search(duration)
            seconds = seconds_pattern.search(duration)

            hours = int(hours.group(1)) if hours else 0
            minutes = int(minutes.group(1)) if minutes else 0
            seconds = int(seconds.group(1)) if seconds else 0

            video_seconds = timedelta(
                    hours = hours,
                    minutes = minutes,
                    seconds = seconds
                ).total_seconds()

            total_seconds += video_seconds
            temp_seconds = int(video_seconds)

            minutes, seconds = divmod(temp_seconds, 60)
            hours, minutes = divmod(minutes, 60)
            song_dur_mins.append((f"{hours}:{minutes}:{seconds}"))



        nextPageToken = pl_response.get('nextPageToken')

        if(not nextPageToken):
            break

        #print(','.join(titles))

    total_seconds = int(total_seconds)

    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)

    time = ((f"{hours}:{minutes}:{seconds}"))

    for item in pl_info_response["items"]:
        name = item["snippet"]["title"]
        num_songs = item["contentDetails"]["itemCount"]

    pl_query = [(1, name, num_songs, time)]

    connection = create_server_connection("localhost", "root", pw)
    connection = create_db_connection("localhost", "root", pw, db)

    pop_playlists_table(connection, pl_query)

    for i in song_ids:
        print(len(song_ids), i)
        pls_query = [(1,i)]
        pop_playlist_songs_table(connection, pls_query)

    print(song_dur_mins)
    j = 0
    for song in song_names:
        print(j)
        songs_query = [(j + 1, song, song_dur_mins[j], channel_ids[j] )]
        pop_songs_table(connection, songs_query)
        j += 1



if __name__ == "__main__":
    main()


#use position attribute to number song id in the database 