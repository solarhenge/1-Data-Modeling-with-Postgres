# drop tables

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# create tables

songplay_table_create = (
"""
-- Table: public.songplays

-- DROP TABLE public.songplays;

CREATE TABLE public.songplays
(
    songplay_id integer NOT NULL,
    start_time timestamp without time zone NOT NULL,
    user_id character varying COLLATE pg_catalog."default" NOT NULL,
    level character varying COLLATE pg_catalog."default" NOT NULL,
    song_id character varying COLLATE pg_catalog."default" NOT NULL,
    artist_id character varying COLLATE pg_catalog."default" NOT NULL,
    session_id integer NOT NULL,
    location character varying COLLATE pg_catalog."default" NOT NULL,
    user_agent character varying COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id),
    CONSTRAINT songplays_artists_fkey FOREIGN KEY (artist_id)
        REFERENCES public.artists (artist_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT songplays_songs_fkey FOREIGN KEY (song_id)
        REFERENCES public.songs (song_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT songplays_time_fkey FOREIGN KEY (start_time)
        REFERENCES public."time" (start_time) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT songplays_users_fkey FOREIGN KEY (user_id)
        REFERENCES public.users (user_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.songplays
    OWNER to student;

-- Index: fki_songplays_artists_fkey

-- DROP INDEX public.fki_songplays_artists_fkey;

CREATE INDEX fki_songplays_artists_fkey
    ON public.songplays USING btree
    (artist_id COLLATE pg_catalog."default")
    TABLESPACE pg_default;

-- Index: fki_songplays_songs_fkey

-- DROP INDEX public.fki_songplays_songs_fkey;

CREATE INDEX fki_songplays_songs_fkey
    ON public.songplays USING btree
    (song_id COLLATE pg_catalog."default")
    TABLESPACE pg_default;

-- Index: fki_songplays_time_fkey

-- DROP INDEX public.fki_songplays_time_fkey;

CREATE INDEX fki_songplays_time_fkey
    ON public.songplays USING btree
    (start_time)
    TABLESPACE pg_default;

-- Index: fki_songplays_users_fkey

-- DROP INDEX public.fki_songplays_users_fkey;

CREATE INDEX fki_songplays_users_fkey
    ON public.songplays USING btree
    (user_id COLLATE pg_catalog."default")
    TABLESPACE pg_default;
""")
#songplay_table_create = ("create table songplays ( songplay_id integer not null generated always as identity ( increment 1 start 1 minvalue 1 maxvalue 2147483647 cache 1 ), start_time timestamp without time zone not null, user_id character varying not null, level character varying not null, song_id character varying not null, artist_id character varying not null, session_id integer not null, location character varying not null, user_agent character varying not null, primary key (songplay_id) ) with ( oids = false ); alter table songplays owner to student;")

user_table_create = ("create table users ( user_id character varying not null, first_name character varying not null, last_name character varying not null, gender character varying not null, level character varying not null, primary key (user_id) ) with ( oids = false ); alter table users owner to student;")

song_table_create = ("create table songs ( song_id character varying not null, title character varying not null, artist_id character varying not null, year double precision not null, duration double precision not null, primary key (song_id) ) with ( oids = false ); alter table songs owner to student;")

artist_table_create = ("create table artists ( artist_id character varying not null, name character varying not null, location character varying, latitude double precision, longitude double precision, primary key (artist_id) ) with ( oids = false ); alter table artists owner to student;")

time_table_create = ("create table time ( start_time timestamp without time zone not null, hour double precision not null, day double precision not null, week double precision not null, month double precision not null, year double precision not null, weekday double precision not null, weekday_name character varying not null, primary key (start_time) ) with ( oids = false ); alter table time owner to student;")

# insert records

songplay_table_insert = ("insert into songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) values (%s, %s, %s, %s, %s, %s, %s, %s, %s) on conflict on constraint songplays_pkey  do nothing;")

user_table_insert = ("insert into users ( user_id, first_name, last_name, gender, level) values (%s, %s, %s, %s, %s) on conflict on constraint users_pkey  do nothing;")

song_table_insert = ("insert into songs (song_id, title, artist_id, year, duration) select distinct values::json->>'song_id' as song_id ,values::json->>'title' as title  ,values::json->>'artist_id' as artist_id  ,cast(values::json->>'year' as double precision) as year  ,cast(values::json->>'duration' as double precision) as duration  from temp_json on conflict on constraint songs_pkey  do nothing;")

artist_table_insert = ("insert into artists (artist_id, name, location, latitude, longitude) select distinct values::json->>'artist_id' as artist_id ,values::json->>'artist_name' as name ,values::json->>'artist_location' as location ,cast(values::json->>'artist_latitude' as double precision) as latitude ,cast(values::json->>'artist_longitude' as double precision) as longitude from temp_json on conflict on constraint artists_pkey  do nothing;")

time_table_insert = ("insert into time (start_time, hour, day, week, month, year, weekday, weekday_name) values (%s, %s, %s, %s, %s, %s, %s, %s) on conflict on constraint time_pkey  do nothing;")

# find songs

song_select = ("""select a.artist_id, a.name, s.song_id, s.title, s.duration from artists a join songs s on a.artist_id = s.artist_id where 1 = 1 and a.name = %s and s.title = %s and s.duration = %s;""")

# query lists

create_table_queries = [ artist_table_create,  song_table_create,  time_table_create,  user_table_create,  songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]