SET search_path to test;

CREATE TABLE IF NOT EXISTS film (
    tconst VARCHAR(50) PRIMARY KEY,
    "titleType" VARCHAR(50),
    "primaryTitle" VARCHAR(500),
    "originalTitle" VARCHAR(500),
    "isAdult" VARCHAR(5),
    "startYear" VARCHAR(10),
    "endYear" VARCHAR(10),
    "runtimeMinutes" VARCHAR(100),
    genres VARCHAR(100)
);
 CREATE TABLE IF NOT EXISTS job (
    tconst VARCHAR(50),
    ordering VARCHAR(50),
    nconst VARCHAR(50),
    category VARCHAR(200),
    job VARCHAR(200),
    characters VARCHAR(200),
    PRIMARY KEY(tconst,ordering)
);
CREATE TABLE IF NOT EXISTS ratings (
    tconst VARCHAR(50) PRIMARY KEY,
    "averageRating" VARCHAR(50),
    "numVotes" VARCHAR(50)
);
CREATE TABLE IF NOT EXISTS people(
    nconst VARCHAR(50),
    "primaryName" VARCHAR(200),
    "birthYear" VARCHAR(50),
    "deathYear" VARCHAR(50),
    "primaryProfession" VARCHAR(200),
    "knownForTitles" VARCHAR(200)
);