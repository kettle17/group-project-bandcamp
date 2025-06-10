-- This file contains all SQL commands to create the tables and relationships for the tracktion database.

DROP TABLE IF EXISTS tag_assignment;
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

DROP TABLE IF EXISTS tag_assignment;

CREATE TABLE country (
    country_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    country_name VARCHAR(60) NOT NULL,
    PRIMARY KEY (country_id)
);

CREATE TABLE artist (
    artist_id BIGINT GENERATED ALWAYS AS IDENTITY,
    artist_name VARCHAR(100),
    PRIMARY KEY (artist_id)
);

CREATE TABLE tag (
    tag_id BIGINT GENERATED ALWAYS AS IDENTITY,
    tag_name VARCHAR(50),
    PRIMARY KEY (tag_id)
);

CREATE TABLE merchandise (
    merchandise_id BIGINT GENERATED ALWAYS AS IDENTITY,
    art_url VARCHAR(255),
    url VARCHAR(255),
    price_at_point_of_sale DECIMAL,
    PRIMARY KEY (merchandise_id)
);

CREATE TABLE album (
    album_id BIGINT GENERATED ALWAYS AS IDENTITY,
    album_name VARCHAR(50),
    release_date DATE,
    art_url VARCHAR(255),
    url VARCHAR(255),
    price_at_point_of_sale DECIMAL,
    PRIMARY KEY (album_id)
);

CREATE TABLE track (
    track_id BIGINT GENERATED ALWAYS AS IDENTITY,
    track_name VARCHAR(50),
    release_date DATE,
    art_url VARCHAR(255),
    url VARCHAR(255),
    price_at_point_of_sale DECIMAL,
    PRIMARY KEY (track_id)
);

CREATE TABLE sale (
    sale_id BIGINT GENERATED ALWAYS AS IDENTITY,
    utc_date TIMESTAMP,
    amount_paid_usd DECIMAL,
    country_id SMALLINT,
    PRIMARY KEY (sale_id),
    FOREIGN KEY (country_id) REFERENCES country (country_id)
);

-- Sale assignment tables 

CREATE TABLE sale_merchandise_assignment (

    merchandise_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    merchandise_id BIGINT,
    sale_id BIGINT,
    PRIMARY KEY (merchandise_assignment_id),
    FOREIGN KEY (merchandise_id) REFERENCES merchandise (merchandise_id),
    FOREIGN key (sale_id) REFERENCES sale (sale_id)
);


CREATE TABLE sale_album_assignment (

    album_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    album_id BIGINT,
    sale_id BIGINT,
    PRIMARY KEY (album_assignment_id),
    FOREIGN KEY (album_id) REFERENCES album (album_id),
    FOREIGN key (sale_id) REFERENCES sale (sale_id)
);


CREATE TABLE sale_track_assignment (

    track_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    track_id BIGINT,
    sale_id BIGINT,
    PRIMARY KEY (track_assignment_id),
    FOREIGN KEY (track_id) REFERENCES track (track_id),
    FOREIGN key (sale_id) REFERENCES sale (sale_id)
);


-- Artist Assignment Tables

CREATE TABLE artist_merchandise_assignment (

    artist_merchandise_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    artist_id BIGINT,
    merchandise_id BIGINT,
    PRIMARY KEY (artist_merchandise_assignment_id),
    FOREIGN KEY (artist_id) REFERENCES artist (artist_id),
    FOREIGN KEY (merchandise_id) REFERENCES merchandise (merchandise_id)
);


CREATE TABLE artist_album_assignment (

    artist_album_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    artist_id BIGINT,
    album_id BIGINT,
    PRIMARY KEY (artist_album_assignment_id),
    FOREIGN KEY (artist_id) REFERENCES artist (artist_id),
    FOREIGN KEY (album_id) REFERENCES album (album_id)
);


CREATE TABLE artist_track_assignment (

    artist_track_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    artist_id BIGINT,
    track_id BIGINT,
    PRIMARY KEY (artist_track_assignment_id),
    FOREIGN KEY (artist_id) REFERENCES artist (artist_id),
    FOREIGN KEY (track_id) REFERENCES track (track_id)
);


-- Tag Assignment table

CREATE TABLE tag_assignment (
    tag_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    tag_id BIGINT,
    track_id BIGINT,
    PRIMARY KEY (tag_assignment_id),
    FOREIGN KEY (tag_id) REFERENCES tag (tag_id),
    FOREIGN KEY (track_id) REFERENCES track (track_id)
);