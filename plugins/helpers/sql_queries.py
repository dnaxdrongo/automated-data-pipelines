# helpers/sql_queries.py

class SqlQueries:
    songplay_table_insert = ("""
        INSERT INTO songplays
            (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
            TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'        AS start_time,
            CAST(se.userid AS VARCHAR(50))                             AS user_id,
            se.level                                                   AS level,
            CAST(ss.song_id AS VARCHAR(40))                            AS song_id,
            CAST(ss.artist_id AS VARCHAR(50))                          AS artist_id,
            se.sessionid                                               AS session_id,
            CAST(se.location AS VARCHAR(100))                          AS location,
            CAST(se.useragent AS VARCHAR(255))                         AS user_agent
        FROM staging_events se
        JOIN staging_songs  ss
          ON se.song = ss.title AND se.artist = ss.artist_name
        WHERE se.page = 'NextSong';
    """)

    user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT
            se.userid                       AS user_id,
            se.firstname                    AS first_name,
            se.lastname                     AS last_name,
            se.gender                       AS gender,
            se.level                        AS level
        FROM staging_events se
        WHERE se.page = 'NextSong' AND se.userid IS NOT NULL;
    """)

    song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT
            CAST(ss.song_id AS VARCHAR(50))     AS song_id,
            CAST(ss.title AS VARCHAR(500))      AS title,
            CAST(ss.artist_id AS VARCHAR(50))   AS artist_id,
            ss.year                              AS year,
            ss.duration                          AS duration
        FROM staging_songs ss
        WHERE ss.song_id IS NOT NULL;
    """)

    artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT
            CAST(ss.artist_id AS VARCHAR(50))       AS artist_id,
            CAST(ss.artist_name AS VARCHAR(500))    AS name,
            CAST(ss.artist_location AS VARCHAR(500)) AS location,
            CAST(ss.artist_latitude AS DECIMAL(9))  AS latitude,
            CAST(ss.artist_longitude AS DECIMAL(9)) AS longitude
        FROM staging_songs ss
        WHERE ss.artist_id IS NOT NULL;
    """)

    time_table_insert = ("""
        INSERT INTO "time" (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT
            ts_utc                                   AS start_time,
            EXTRACT(hour    FROM ts_utc)::SMALLINT   AS hour,
            EXTRACT(day     FROM ts_utc)::SMALLINT   AS day,
            EXTRACT(week    FROM ts_utc)::SMALLINT   AS week,
            EXTRACT(month   FROM ts_utc)::SMALLINT   AS month,
            EXTRACT(year    FROM ts_utc)::SMALLINT   AS year,
            EXTRACT(weekday FROM ts_utc)::SMALLINT   AS weekday
        FROM (
            SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS ts_utc
            FROM staging_events
            WHERE page = 'NextSong'
        ) t;
    """)
