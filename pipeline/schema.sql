DROP TABLE IF EXISTS album_tag_assignment;
DROP TABLE IF EXISTS track_tag_assignment;
DROP TABLE IF EXISTS artist_merchandise_assignment;
DROP TABLE IF EXISTS artist_album_assignment;
DROP TABLE IF EXISTS artist_track_assignment;
DROP TABLE IF EXISTS sale_merchandise_assignment;
DROP TABLE IF EXISTS sale_album_assignment;
DROP TABLE IF EXISTS sale_track_assignment;
DROP TABLE IF EXISTS merchandise;
DROP TABLE IF EXISTS album;
DROP TABLE IF EXISTS track;
DROP TABLE IF EXISTS sale;
DROP TABLE IF EXISTS artist;
DROP TABLE IF EXISTS tag;
DROP TABLE IF EXISTS country;

CREATE TABLE country (
    country_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    country_name VARCHAR(60) NOT NULL UNIQUE,
    PRIMARY KEY (country_id)
);

CREATE TABLE artist (
    artist_id BIGINT GENERATED ALWAYS AS IDENTITY,
    artist_name VARCHAR(100) NOT NULL UNIQUE,
    PRIMARY KEY (artist_id)
);

CREATE TABLE tag (
    tag_id BIGINT GENERATED ALWAYS AS IDENTITY,
    tag_name VARCHAR(50) NOT NULL UNIQUE,
    PRIMARY KEY (tag_id)
);

CREATE TABLE merchandise (
    merchandise_id BIGINT GENERATED ALWAYS AS IDENTITY,
    merchandise_name VARCHAR(255),
    release_date DATE,
    art_url VARCHAR(255),
    url VARCHAR(255) NOT NULL UNIQUE,
    PRIMARY KEY (merchandise_id)
);

CREATE TABLE album (
    album_id BIGINT GENERATED ALWAYS AS IDENTITY,
    album_name TEXT NOT NULL,
    release_date DATE,
    art_url VARCHAR(255),
    url VARCHAR(255) NOT NULL UNIQUE,
    PRIMARY KEY (album_id)
);

CREATE TABLE track (
    track_id BIGINT GENERATED ALWAYS AS IDENTITY,
    track_name TEXT NOT NULL,
    release_date DATE,
    art_url VARCHAR(255),
    url VARCHAR(255) NOT NULL UNIQUE,
    PRIMARY KEY (track_id)
);

CREATE TABLE sale (
    sale_id BIGINT GENERATED ALWAYS AS IDENTITY,
    utc_date TIMESTAMP NOT NULL,
    country_id SMALLINT NOT NULL,
    PRIMARY KEY (sale_id),
    FOREIGN KEY (country_id) REFERENCES country (country_id)
);

-- Sale assignment tables 

CREATE TABLE sale_merchandise_assignment (
    merchandise_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    merchandise_id BIGINT NOT NULL,
    sale_id BIGINT NOT NULL,
    sold_for DECIMAL NOT NULL,
    PRIMARY KEY (merchandise_assignment_id),
    FOREIGN KEY (merchandise_id) REFERENCES merchandise (merchandise_id),
    FOREIGN key (sale_id) REFERENCES sale (sale_id)
);


CREATE TABLE sale_album_assignment (
    album_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    album_id BIGINT NOT NULL,
    sale_id BIGINT NOT NULL,
    sold_for DECIMAL NOT NULL,
    PRIMARY KEY (album_assignment_id),
    FOREIGN KEY (album_id) REFERENCES album (album_id),
    FOREIGN key (sale_id) REFERENCES sale (sale_id)
);


CREATE TABLE sale_track_assignment (
    track_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    track_id BIGINT NOT NULL,
    sale_id BIGINT NOT NULL,
    sold_for DECIMAL NOT NULL,
    PRIMARY KEY (track_assignment_id),
    FOREIGN KEY (track_id) REFERENCES track (track_id),
    FOREIGN key (sale_id) REFERENCES sale (sale_id)
);


-- Artist Assignment Tables

CREATE TABLE artist_merchandise_assignment (
    artist_merch_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    artist_id BIGINT NOT NULL,
    merchandise_id BIGINT NOT NULL,
    PRIMARY KEY (artist_merch_assignment_id),
    FOREIGN KEY (artist_id) REFERENCES artist (artist_id),
    FOREIGN KEY (merchandise_id) REFERENCES merchandise (merchandise_id),
    UNIQUE (artist_id, merchandise_id)
);


CREATE TABLE artist_album_assignment (
    artist_album_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    artist_id BIGINT NOT NULL,
    album_id BIGINT NOT NULL,
    PRIMARY KEY (artist_album_assignment_id),
    FOREIGN KEY (artist_id) REFERENCES artist (artist_id),
    FOREIGN KEY (album_id) REFERENCES album (album_id),
    UNIQUE (artist_id, album_id)
);


CREATE TABLE artist_track_assignment (
    artist_track_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    artist_id BIGINT NOT NULL,
    track_id BIGINT NOT NULL,
    PRIMARY KEY (artist_track_assignment_id),
    FOREIGN KEY (artist_id) REFERENCES artist (artist_id),
    FOREIGN KEY (track_id) REFERENCES track (track_id),
    UNIQUE (artist_id, track_id)
);


-- Tag Assignment tables

CREATE TABLE track_tag_assignment (
    track_tag_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    tag_id BIGINT NOT NULL,
    track_id BIGINT NOT NULL,
    PRIMARY KEY (track_tag_assignment_id),
    FOREIGN KEY (tag_id) REFERENCES tag (tag_id),
    FOREIGN KEY (track_id) REFERENCES track (track_id),
    UNIQUE (tag_id, track_id)
);

CREATE TABLE album_tag_assignment (
    album_tag_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    tag_id BIGINT NOT NULL,
    album_id BIGINT NOT NULL,
    PRIMARY KEY (album_tag_assignment_id),
    FOREIGN KEY (tag_id) REFERENCES tag (tag_id),
    FOREIGN KEY (album_id) REFERENCES album (album_id),
    UNIQUE (tag_id, album_id)
);

ALTER SEQUENCE country_country_id_seq RESTART WITH 1;
ALTER SEQUENCE artist_artist_id_seq RESTART WITH 1;
ALTER SEQUENCE tag_tag_id_seq RESTART WITH 1;
ALTER SEQUENCE merchandise_merchandise_id_seq RESTART WITH 1;
ALTER SEQUENCE album_album_id_seq RESTART WITH 1;
ALTER SEQUENCE track_track_id_seq RESTART WITH 1;
ALTER SEQUENCE sale_sale_id_seq RESTART WITH 1;
ALTER SEQUENCE sale_merchandise_assignment_merchandise_assignment_id_seq RESTART WITH 1;
ALTER SEQUENCE sale_album_assignment_album_assignment_id_seq RESTART WITH 1;
ALTER SEQUENCE sale_track_assignment_track_assignment_id_seq RESTART WITH 1;
ALTER SEQUENCE artist_merchandise_assignment_artist_merch_assignment_id_seq RESTART WITH 1;
ALTER SEQUENCE artist_album_assignment_artist_album_assignment_id_seq RESTART WITH 1;
ALTER SEQUENCE artist_track_assignment_artist_track_assignment_id_seq RESTART WITH 1;
ALTER SEQUENCE track_tag_assignment_track_tag_assignment_id_seq RESTART WITH 1;
ALTER SEQUENCE album_tag_assignment_album_tag_assignment_id_seq RESTART WITH 1;

