import datetime
import json
import sys
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

filepath_song ='data/song_data'
filepath_log = 'data/log_data'

platform = sys.platform
if platform == "win32":
    chdir_path = 'D:/Documents/GitHub/Udacity/Data-Engineering-Nanodegree/Data-Modeling-with-Postgres/'
    os.chdir(chdir_path)
    filepath_song = chdir_path + 'data/song_data'
    filepath_log = chdir_path + 'data/log_data'

# a function that will copy a file to a temporary json table
# this function is used for bulk loading of data
def copy_file_to_temp_table(cur, filepath):
    print("====================")
    print("copy_file_to_temp_table")
    print("filepath = ",filepath)
    query = "copy temp_json from '"+filepath+"'"
    print("query = ",query)
    cur.execute(query)

# a function to process song files
def process_song_file(cur, conn, filepath):
    # open song file
    print("====================")
    print("process_song_file")
    print("filepath = ",filepath)
    copy_file_to_temp_table(cur, filepath)
    
# a function to load or populate the time dimension table
# the timestamp 13 (i.e. timestamp with timezone) is being converted to a timestamp with local timezone
def load_time_dimension(cur, ts):
    print("====================")
    print("load_time_dimension")
    
    # get rid of dups
    set_ts = set(ts)
    print(f'Print set_ts --> {set_ts}')    
    
    for x in set_ts:
        timestamp = x
        start_time = datetime.datetime.fromtimestamp(timestamp/1000)  # using the local timezone
        print("start_time = "+str(start_time))
        print(start_time.strftime("%Y-%m-%d %H:%M:%S"))  # 2018-04-07 20:48:08, YMMV
        year = start_time.year
        print("year = "+str(year))
        month = start_time.month
        print("month = "+str(month))
        day = start_time.day
        print("day = "+str(day))
        hour = start_time.hour
        print("hour = "+str(hour))
        week = int(start_time.strftime("%V"))
        print("week = "+str(week))
        weekday = start_time.weekday()
        print("weekday = "+str(weekday))
        weekday_name = start_time.strftime("%A")
        print("weekday_name = "+weekday_name)
        row = []
        row.extend([start_time, hour, day, week, month, year, weekday, weekday_name])
        print(row)
        cur.execute(time_table_insert, list(row))
    
# a function to insert user records
def insert_user_records(cur, page, user_df):
    for i, row in user_df.iterrows():
        print(i)
        print(row)
        print(page[i])
        if page[i] == "NextSong":
            cur.execute(user_table_insert, row)

# a function to process log file
# it will load the following table(s):
# time dimension
# users dimension
# songplays fact            
def process_log_file(cur, conn, filepath):
    # open log file
    print("process_log_file")
    print("filepath = ",filepath)
    
    data = []
    for line in open(filepath, 'r'):
        print("line = "+line)
        data.append(json.loads(line))
    print(data)
    
    # load time dimension
    ts = list(map(lambda x : x['ts'], data))
    print(f'ts --> {ts}')
    load_time_dimension(cur, ts)
    
    # change timestamp with timezone to datetime with local timezone
    for i, s in enumerate(ts):
        ts[i] = datetime.datetime.fromtimestamp(s/1000)

    # load user table
        
    print("====================")
    user_id = list(map(lambda x : x['userId'], data))
    print(f'user_id --> {user_id}')
    first_name = list(map(lambda x : x['firstName'], data))
    print(f'first_name --> {first_name}')
    last_name = list(map(lambda x : x['lastName'], data))
    print(f'last_name --> {last_name}')
    gender = list(map(lambda x : x['gender'], data))
    print(f'gender --> {gender}')
    level = list(map(lambda x : x['level'], data))
    print(f'level --> {level}')
    page = list(map(lambda x : x['page'], data))
    print(f'page --> {page}')
    
    user_df = pd.DataFrame({'user_id': user_id,
                            'first_name': first_name,
                            'last_name': last_name,
                            'gender': gender,
                            'level': level})
    print(user_df)

    # insert user records
    insert_user_records(cur, page, user_df)
        
    # insert songplay records
    artist = list(map(lambda x : x['artist'], data))
    print(f'artist --> {artist}')
    song = list(map(lambda x : x['song'], data))
    print(f'song --> {song}')
    session_id = list(map(lambda x : x['sessionId'], data))
    print(f'session_id --> {session_id}')
    length = list(map(lambda x : x['length'], data))
    print(f'length --> {length}')
    location = list(map(lambda x : x['location'], data))
    print(f'location --> {location}')
    user_agent = list(map(lambda x : x['userAgent'], data))
    print(f'user_agent --> {user_agent}')

    songplay_df = pd.DataFrame({'page': page,
                                'start_time': ts,
                                'artist': artist,
                                'song': song,
                                'length': length,
                                'user_id': user_id,
                                'level': level,
                                'session_id': session_id,
                                'location': location,
                                'user_agent': user_agent})
    # get rid of songs where page is not equal to "NextSong"
    songplay_df = songplay_df[songplay_df.page == "NextSong"]
    print(songplay_df)
    
    songplay_id = 0;
    
    for index, row in songplay_df.iterrows():
        print(row.artist)
        print(row.song)
        print(row.length)
        results = cur.execute(song_select, (row.artist, row.song, row.length))
        #results = cur.execute(song_select, ('Adam Ant', 'Something Girls', 233.40363))
        one_row = cur.fetchone()
        print("====================")
        print(one_row)
        if one_row:
            print("yahoo!")
            artist_id = one_row[0]
            song_id = one_row[2]
            print('artist_id = '+artist_id)
            print('song_id = '+song_id)
            
            songplay_data = []
            songplay_id += 1
            songplay_data = [songplay_id,
                             row.start_time, 
                             row.user_id, 
                             row.level,
                             song_id,
                             artist_id,
                             row.session_id,
                             row.location,
                             row.user_agent]
            print(songplay_data)
            cur.execute(songplay_table_insert, songplay_data)           
            #conn.commit()
            #sys.exit()
        else:    
            print("boo hoo!")
    
# a function to process data
# it will process a file and then execute a passed in functionload the following table(s):
def process_data(cur, conn, filepath, func):
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, conn, datafile)
        print('{}/{} files processed.'.format(i, num_files))

# a function that will process song data
# it will populate the following table(s):
# artists dimension
# songs dimension
# it uses the temporary json table to bulk insert into the above tables        
def do_song(cur, conn, filepath):
    # create temporary json table
    query = ("create temporary table temp_json (values text) on commit drop;")
    cur.execute(query)
    process_data(cur, conn, filepath=filepath, func=process_song_file)
    cur.execute(artist_table_insert)
    cur.execute(song_table_insert)
    conn.commit()

# a function that will process log data
def do_log(cur, conn, filepath):
    process_data(cur, conn, filepath=filepath, func=process_log_file)
    conn.commit()

# the main driver
# it will process song data first and then process log data second    
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    do_song(cur, conn, filepath_song)
    do_log(cur, conn, filepath_log)

    conn.close()

if __name__ == "__main__":
    main()