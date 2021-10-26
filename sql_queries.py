import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
    (
        artist varchar,
        auth varchar,
        first_name varchar,
        gender varchar,
        itemInSession integer,
        last_name varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration bigint,
        sessionId integer,
        song varchar,
        status integer,
        ts timestamp,
        userAgent varchar,
        userId integer
    )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs integer,
        artist_id varchar NOT NULL PRIMARY KEY, 
        artist_latitude float, 
        artist_longitude float,
        artist_location varchar, 
        artist_name varchar, 
        song_id varchar sortkey,
        title varchar, 
        duration float, 
        year integer
    )
    
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
    (
        songplay_id INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY,
        start_time timestamp NOT NULL, 
        user_id integer NOT NULL, 
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id integer, 
        location varchar, 
        user_agent varchar 
    )
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
    (
        user_id integer NOT NULL PRIMARY KEY, 
        first_name varchar NOT NULL, 
        last_name varchar NOT NULL, 
        gender varchar(1), 
        level varchar NOT NULL
    )
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
    (
        song_id varchar NOT NULL PRIMARY KEY, 
        title varchar NOT NULL, 
        artist_id varchar NOT NULL, 
        year integer, 
        duration float
    )
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
    (
        artist_id varchar NOT NULL PRIMARY KEY, 
        name varchar NOT NULL, 
        location varchar, 
        latitude float, 
        longitude float
    )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
    (
        start_time timestamp NOT NULL PRIMARY KEY,
        hour integer, 
        day integer,
        week integer, 
        month integer, 
        year integer, 
        weekday varchar
    )
""")

# STAGING TABLES

staging_events_copy = (f"""
    copy staging_events from {LOG_DATA}
    credentials 'aws_iam_role={ARN}'
    format as json {LOG_JSON_PATH}
    timeformat as 'epochmillisecs'
    truncatecolumns blanksasnull emptyasnull
    compupdate off
    REGION 'us-west-2';
""")

staging_songs_copy = (f"""
    copy staging_songs from {SONG_DATA}
    credentials 'aws_iam_role={ARN}'
    json 'auto'
    truncatecolumns blanksasnull emptyasnull
    compupdate off
    region 'us-west-2';
""")

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(   select distinct 
        evs.ts,
        evs.userId,
        evs.level,
        sngs.song_id,
        sngs.artist_id,
        evs.sessionId,
        evs.location,
        evs.userAgent
    from staging_events as evs
    join staging_songs as sngs on evs.artist = sngs.artist_name
    where evs.page = 'NextSong'
);
""")

user_table_insert = ("""
insert into users (

   select distinct
        userId,
        first_name,
        last_name,
        gender,
        level
    from staging_events
    where page = 'NextSong' and userId is not null

)

""")

song_table_insert = ("""
insert into songs (

   select distinct
        song_id,
        title,
        artist_id,
        year,
        duration
    from staging_songs
    where song_id is not null
    
)
""")

artist_table_insert = ("""
insert into artists (

   select distinct
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    from staging_songs
    where artist_id is not null

)
""")

time_table_insert = ("""
insert into time (

   select distinct
        ts,
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(weekday from ts)
    from staging_events
    where page = 'NextSong' and ts is not null

)
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
