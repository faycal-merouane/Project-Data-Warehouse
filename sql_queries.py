import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
  artist VARCHAR NULL,
  auth VARCHAR NULL,
  firstName VARCHAR NULL,
  gender VARCHAR NULL,
  itemInSession INT,
  lastName VARCHAR NULL,
  length FLOAT NULL,
  level VARCHAR NULL,
  location VARCHAR NULL,
  method VARCHAR NULL,
  page VARCHAR NULL,
  registration FLOAT,
  sessionId BIGINT NOT NULL,
  song VARCHAR NULL,
  status int NOT NULL,
  ts TIMESTAMP,
  userAgent VARCHAR NULL, 
  user_id INT 
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
  artist_id VARCHAR(18) NOT NULL,
  artist_latitude FLOAT,
  artist_location VARCHAR,
  artist_longitude FLOAT,
  artist_name VARCHAR,
  duration FLOAT,
  num_songs int,
  song_id VARCHAR(18) NOT NULL,
  title VARCHAR,
  year int 
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
  songplay_id int IDENTITY(0,1),
  start_time BIGINT NOT NULL, 
  user_id int NOT NULL,
  level VARCHAR NULL,
  song_id VARCHAR(18),
  artist_id VARCHAR(18) NOT NULL,
  session_id int,
  location VARCHAR,
  user_agent VARCHAR,
  primary key(songplay_id),
  foreign key(user_id) references users(user_id),
  foreign key(song_id) references songs(song_id),
  foreign key(artist_id) references artists(artist_id),
  foreign key(start_time) references time(start_time)
)
""")

user_table_create = ("""

CREATE TABLE IF NOT EXISTS users(
  user_id int,
  first_name VARCHAR NULL,
  last_name VARCHAR NULL,
  gender VARCHAR NULL,
  level VARCHAR NULL,
  primary key(user_id)
)

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
  song_id  VARCHAR(18) NOT NULL,
  title VARCHAR,
  artist_id VARCHAR(18) NOT NULL,
  year INT NULL,
  duration FLOAT,
  primary key(song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
  artist_id VARCHAR(18),
  artist_name VARCHAR NULL,
  location VARCHAR NULL,
  lattitude VARCHAR NULL,
  longitude VARCHAR NULL,
  primary key(artist_id)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
  start_time BIGINT,
  hour INT NOT NULL,
  day INT NOT NULL,
  week INT NOT NULL,
  month INT NOT NULL,
  year INT NOT NULL,
  weekday INT NOT NULL,
  primary key(start_time)
)
""")

# STAGING TABLES

staging_events_copy = ("""
 copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    FORMAT AS JSON {} timeformat 'epochmillisecs' region 'us-west-2';
""").format(config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
 copy staging_songs from 's3://udacity-dend/song_data/'
    credentials 'aws_iam_role={}'
    JSON 'auto' region 'us-west-2';
""").format(config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
  start_time,
  user_id,
  level,
  song_id,
  artist_id,
  session_id,
  location,
  user_agent
) SELECT DISTINCT date_part(epoch, se.ts), se.user_id, se.level, st.song_id, st.artist_id, se.sessionId, se.location, se.userAgent from staging_events se, staging_songs st where se.page = 'NextSong' AND st.artist_name = se.artist AND se.song = st.title AND se.length = duration
""")

user_table_insert = ("""
INSERT INTO users(
  user_id,
  first_name,
  last_name,
  gender,
  level
 ) SELECT DISTINCT user_id, firstName, lastName, gender, level FROM staging_events  WHERE page='NextSong'

""")

song_table_insert = ("""
INSERT INTO songs(
    song_id,
    title,
    artist_id,
    year,
    duration
)SELECT DISTINCT song_id, title, artist_id, year, duration from staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists(
  artist_id,
  artist_name,
  location,
  lattitude,
  longitude
)SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs


""")

time_table_insert = ("""
INSERT INTO time(
	start_time,
	hour,
	day,
	week,
	month,
	year,
	weekday
)SELECT DISTINCT date_part(epoch, ts), extract(hour from ts), extract(day from ts), extract(week from ts), extract(month from ts), extract(year from ts), extract(weekday from ts) from staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,  user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
