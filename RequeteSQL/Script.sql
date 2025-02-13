select title_ratings.tconst as titre, title_ratings."averageRating" as note, title_basics.genres as genre, name_basics."primaryName" as acteurs
from dossantos.title_ratings
inner join dossantos.title_basics on dossantos.title_ratings.tconst = dossantos.title_basics.tconst
inner join dossantos.title_principals on dossantos.title_basics.tconst = dossantos.title_principals.tconst
inner join dossantos.name_basics on dossantos.title_principals.nconst = dossantos.name_basics.nconst
