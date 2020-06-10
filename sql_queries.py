import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE staging_events(
    eventId INT IDENTITY(0,1) PRIMARY KEY,
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession VARCHAR,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INT)
""")

staging_songs_table_create = (""" CREATE TABLE staging_songs(
    songId VARCHAR NOT NULL PRIMARY KEY,
    numSong INT,
    artistId VARCHAR,
    latitude FLOAT, 
    longitude FLOAT,
    location VARCHAR, 
    duration FLOAT,
    artistName VARCHAR,
    title VARCHAR,
    year INT)
""")

songplay_table_create = ("""CREATE TABLE songplays(

    songplayId INT IDENTITY(0,1) PRIMARY KEY,
    starttime TIMESTAMP REFERENCES time(starttime),
    userId INT NOT NULL REFERENCES users(userId),
    level VARCHAR,
    songId VARCHAR NOT NULL REFERENCES song(songId) SORTKEY,
    artistId VARCHAR NOT NULL REFERENCES artist(artistId) DISTKEY,
    sessionId INT,
    location VARCHAR,
    userAgent VARCHAR)
    
""")

user_table_create = ("""CREATE TABLE users(
    userId INT NOT NULL PRIMARY KEY,
    firstName VARCHAR,
    lastName VARCHAR,
    gender VARCHAR,
    level VARCHAR)
""")

song_table_create = ("""CREATE TABLE song(
    songId VARCHAR PRIMARY KEY,
    title VARCHAR,
    artistId VARCHAR NOT NULL,
    year INTEGER,
    duration FLOAT)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist(
        artistId VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT)
""")

time_table_create = ("""CREATE TABLE time(
    starttime TIMESTAMP NOT NULL PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])



# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (starttime, userId, level, songId, artistId, sessionId, location, userAgent)
    SELECT  DISTINCT(e.ts)  AS start_time, 
            e.userId        AS user_id, 
            e.level         AS level, 
            s.songId       AS song_id, 
            s.artistId     AS artist_id, 
            e.sessionId     AS session_id, 
            e.location      AS location, 
            e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artistName)
    AND e.page  ==  'NextSong';
""")

user_table_insert = ("""INSERT INTO users(userId, firstName, lastName, gender, level)
    SELECT DISTINCT userId,
                    firstName,
                    lastName,
                    gender,
                    level
    FROM staging_events
    WHERE page = 'NextSong'
    AND userId IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO song(songId, title, artistId, year, duration)
    SELECT  DISTINCT(songId) AS song_id,
            title,
            artistId,
            year,
            duration
    FROM staging_songs
    WHERE songId IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artist(artistId, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artistId,
        artistName,
        location,
        latitude,
        longitude
        FROM staging_songs
        WHERE artistId IS NOT NULL
""")

time_table_insert = ("""INSERT INTO time (starttime, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(starttime)                AS start_time,
            EXTRACT(hour FROM starttime)       AS hour,
            EXTRACT(day FROM starttime)        AS day,
            EXTRACT(week FROM starttime)       AS week,
            EXTRACT(month FROM starttime)      AS month,
            EXTRACT(year FROM starttime)       AS year,
            EXTRACT(dayofweek FROM starttime)  as weekday
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
