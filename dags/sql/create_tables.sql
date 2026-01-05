
-- ===== DROP TABLES =====
DROP TABLE IF EXISTS public.songplays;
DROP TABLE IF EXISTS public.users;
DROP TABLE IF EXISTS public.songs;
DROP TABLE IF EXISTS public.artists;
DROP TABLE IF EXISTS public."time";
DROP TABLE IF EXISTS public.staging_events;
DROP TABLE IF EXISTS public.staging_songs;

-- ===== STAGING =====
CREATE TABLE IF NOT EXISTS public.staging_events (
    event_id    BIGINT IDENTITY(0,1) NOT NULL,
    artist      VARCHAR NULL,
    auth        VARCHAR NULL,
    firstname   VARCHAR NULL,
    gender      VARCHAR NULL,
    iteminsession VARCHAR NULL,
    lastname    VARCHAR NULL,
    length      VARCHAR NULL,
    level       VARCHAR NULL,
    location    VARCHAR NULL,
    method      VARCHAR NULL,
    page        VARCHAR NULL,
    registration VARCHAR NULL,
    sessionid   INTEGER NOT NULL DISTKEY SORTKEY,
    song        VARCHAR NULL,
    status      VARCHAR NULL,
    ts          BIGINT NOT NULL,
    useragent   VARCHAR NULL,
    userid      INTEGER NULL
);

CREATE TABLE IF NOT EXISTS public.staging_songs (
    num_songs       INTEGER NULL,
    artist_id       VARCHAR NOT NULL DISTKEY SORTKEY,
    artist_latitude VARCHAR NULL,
    artist_longitude VARCHAR NULL,
    artist_location VARCHAR(500) NULL,
    artist_name     VARCHAR(500) NULL,
    song_id         VARCHAR NOT NULL,
    title           VARCHAR(500) NULL,
    duration        DECIMAL(9) NULL,
    year            INTEGER NULL
);

-- ===== ANALYTICS =====
CREATE TABLE IF NOT EXISTS public.songs (
    song_id   VARCHAR(50)  NOT NULL SORTKEY,
    title     VARCHAR(500) NOT NULL,
    artist_id VARCHAR(50)  NOT NULL,
    year      INTEGER      NOT NULL,
    duration  DECIMAL(9)   NOT NULL
);

CREATE TABLE IF NOT EXISTS public.artists (
    artist_id  VARCHAR(50)  NOT NULL SORTKEY,
    name       VARCHAR(500) NULL,
    location   VARCHAR(500) NULL,
    latitude   DECIMAL(9)   NULL,
    longitude  DECIMAL(9)   NULL
);

CREATE TABLE IF NOT EXISTS public.users (
    user_id    INTEGER      NOT NULL SORTKEY,
    first_name VARCHAR(50)  NULL,
    last_name  VARCHAR(80)  NULL,
    gender     VARCHAR(10)  NULL,
    level      VARCHAR(10)  NULL
);

CREATE TABLE IF NOT EXISTS public."time" (
    start_time TIMESTAMP NOT NULL SORTKEY,
    hour       SMALLINT  NULL,
    day        SMALLINT  NULL,
    week       SMALLINT  NULL,
    month      SMALLINT  NULL,
    year       SMALLINT  NULL,
    weekday    SMALLINT  NULL
);

CREATE TABLE IF NOT EXISTS public.songplays (
    songplay_id INTEGER IDENTITY(1,1) NOT NULL SORTKEY,
    start_time  TIMESTAMP NOT NULL,
    user_id     VARCHAR(50) NOT NULL DISTKEY,
    level       VARCHAR(10) NULL,
    song_id     VARCHAR(40) NOT NULL,
    artist_id   VARCHAR(50) NOT NULL,
    session_id  INTEGER NOT NULL,
    location    VARCHAR(100) NULL,
    user_agent  VARCHAR(255) NULL
);
