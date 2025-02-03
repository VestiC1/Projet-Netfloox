#Recupererable depuis title.basic et ratings
filmTable = """
CREATE TABLE IF NOT EXISTS Herry.filmbasic (
    filmId VARCHAR(100) PRIMARY KEY,
    title VARCHAR(500),
    mainGenre VARCHAR(500),
    startYear INT,
    length INT,
    avg_Rating FLOAT,
    num_Rating INT
);
"""
#Recuperable depuis name.basics
peopleTable = """
CREATE TABLE Herry.people (
    pId VARCHAR(100) PRIMARYKEY,
    name VARCHAR(100),
    yearBirth INT,
    yearDeath INT
 );"""

#Recuperable depuis title.principals
jobTable = """
 CREATE TABLE Herry.job (
 jId VARCHAR(100) PRIMARYKEY,
 pId VARCHAR(100),
 filmId VARCHAR(100),
 );
 """