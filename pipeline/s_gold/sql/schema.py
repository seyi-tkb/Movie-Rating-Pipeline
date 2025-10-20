# python
staging_schema_sql = """
CREATE SCHEMA IF NOT EXISTS stg;
"""

production_schema_sql = """
CREATE SCHEMA IF NOT EXISTS prod;
"""

staging_movies_sql = """
CREATE TABLE IF NOT EXISTS stg.stg_movies (
    item_id INT PRIMARY KEY,
    movie_title TEXT NOT NULL,
    release_date TIMESTAMP,
    imdb_url TEXT,
    primary_genre TEXT
);
"""


movies_sql = """
CREATE TABLE IF NOT EXISTS prod.movies (
    item_id INT PRIMARY KEY,
    movie_title TEXT NOT NULL,
    release_date TIMESTAMP,
    imdb_url TEXT,
    primary_genre TEXT
);
"""

staging_users_sql = """
CREATE TABLE IF NOT EXISTS stg.stg_users (
    user_id INT PRIMARY KEY,
    gender CHAR(1),
    age INT,
    occupation TEXT,
    zip_code TEXT
);
"""

users_sql = """
CREATE TABLE IF NOT EXISTS prod.users (
    user_id INT NOT NULL,
    gender CHAR(1),
    age INT,
    occupation TEXT,
    zip_code TEXT,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (user_id, valid_from)
);
"""

staging_ratings_sql = """
CREATE TABLE IF NOT EXISTS stg.stg_ratings (
    user_id INT,
    item_id INT,
    rating DOUBLE PRECISION,
    timestamp TIMESTAMP,
    PRIMARY KEY (user_id, item_id, timestamp)
);

-- user_id REFERENCES users(user_id)
-- item_id REFERENCES movies(item_id)
"""


ratings_sql = """
CREATE TABLE IF NOT EXISTS prod.ratings (
    user_id INT,
    item_id INT,
    rating DOUBLE PRECISION,
    timestamp TIMESTAMP,
    PRIMARY KEY (user_id, item_id, timestamp)
)
-- make it partitionanble later
PARTITION BY RANGE (timestamp);
"""

# upserts
upsert_movies = """
-- upsert
INSERT INTO prod.movies (item_id, movie_title, release_date, primary_genre, imdb_url)
SELECT item_id, movie_title, release_date, primary_genre, imdb_url
FROM stg.stg_movies
ON CONFLICT (item_id)
DO UPDATE SET 
    movie_title = EXCLUDED.movie_title,
    release_date = EXCLUDED.release_date,
    primary_genre = EXCLUDED.primary_genre,
    imdb_url = EXCLUDED.imdb_url;

-- clear staging
TRUNCATE table stg.stg_movies;
"""

upsert_users = """

-- inserts new users
INSERT INTO prod.users (user_id, gender, age, occupation, zip_code, valid_from, valid_to, is_current)
SELECT s.user_id, s.gender, s.age::int, s.occupation, s.zip_code, now() AT TIME ZONE 'UTC', NULL, TRUE
FROM stg.stg_users s
LEFT JOIN users u ON u.user_id = s.user_id AND u.is_current = TRUE
WHERE u.user_id IS NULL;

-- closes current records where any attribute changed
WITH change_table AS (
    SELECT u.user_id
    FROM prod.users u
    JOIN stg.stg_users s ON u.user_id = s.user_id
    WHERE u.is_current = TRUE AND (
        COALESCE(u.occupation, '') <> COALESCE(s.occupation, '') OR
        COALESCE(u.zip_code, '') <> COALESCE(s.zip_code, ''))
        -- coalesce so if either is null, it recognizes it properly as a change
)
UPDATE prod.users
SET is_current = FALSE,
    valid_to = now() AT TIME ZONE 'UTC'
WHERE user_id IN (SELECT user_id FROM change_table) AND is_current = TRUE;

-- inserts new version of changed users
WITH change_table AS (
    SELECT u.user_id
    FROM prod.users u
    JOIN stg.stg_users s ON u.user_id = s.user_id
    WHERE u.is_current = TRUE AND (
        COALESCE(u.occupation, '') <> COALESCE(s.occupation, '') OR
        COALESCE(u.zip_code, '') <> COALESCE(s.zip_code, '')
    )
)
INSERT INTO prod.users (user_id, gender, age, occupation, zip_code, valid_from, valid_to, is_current)
SELECT s.user_id, s.gender, s.age::int, s.occupation, s.zip_code, now() AT TIME ZONE 'UTC', NULL, TRUE
FROM stg.stg_users s
JOIN change_table c ON s.user_id = c.user_id;

-- clear staging
TRUNCATE table stg.stg_users;
"""

upsert_ratings = """

-- Upsert
INSERT INTO prod.ratings (user_id, item_id, rating, timestamp)
SELECT user_id, item_id, rating, timestamp
FROM stg.stg_ratings 
ON CONFLICT (user_id, item_id, timestamp)
DO UPDATE SET rating = EXCLUDED.rating;

-- clear staging
TRUNCATE table stg.stg_ratings;
"""

create_ratings_partition = """
SELECT prod.ensure_partitions_for_staging();
"""

# question. to detect changes in SCDs or dimensions generally early on, do you do an upsert before even lifting from the initial database(actual raw before bronze/staging)
# the reason being. if it isn't a full load everytime for the dimensions
# you will only append with  watermarkks based on ids that havent beene loaded before
# and that means those that have been loaded before don't get to gold, even if they are update in the raw intial file or systems


# found my answer, create a "last_updated" column on every dimension that shows when the dimension was updated
# watermarks is then based on that and last load
# this ensure every change moves upstream.


# add last_updated, batch_id to dimensions from P1
