Schema = "Herry"

#Recupererable depuis title.basic et ratings
filmTable = f"""
CREATE TABLE IF NOT EXISTS {Schema}.filmbasic (
    filmId VARCHAR(100) PRIMARY KEY,
    title VARCHAR(500),
    mainGenre VARCHAR(500),
    startYear INT,
    length INT,

);
"""
#Recuperable depuis name.basics
peopleTable = f"""
CREATE TABLE {Schema}.people (
    pId VARCHAR(100) PRIMARYKEY,
    name VARCHAR(100),
    yearBirth INT,
    yearDeath INT
 );"""

#Recuperable depuis title.principals
jobTable = f"""
 CREATE TABLE {Schema}.job (
    jId VARCHAR(100) PRIMARYKEY,
    pId VARCHAR(100),
    filmId VARCHAR(100),
    CONSTRAINT Person FOREIGN KEY (pId) REFERENCES people(pId),
    CONSTRAINT Film FOREIGN KEY (filmID) REFERENCES filmbasic(filmId)
 );
 """

#Recuperable ratings
ratingsTable = f"""
 CREATE TABLE {Schema}.ratings (
    rId VARCHAR(100) PRIMARYKEY,
    filmId VARCHAR(100),
    CONSTRAINT Film FOREIGN KEY (filmID) REFERENCES filmbasic(filmId)
 );
"""