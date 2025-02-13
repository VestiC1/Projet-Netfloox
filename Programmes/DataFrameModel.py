import os
import pandas as pd
import psycopg2

requete = f"""SELECT test.film."primaryTitle" , "startYear" ,"runtimeMinutes" ,
split_part("genres", ',', 1) AS genre_1,
split_part("genres", ',', 2) AS genre_2,
split_part("genres", ',', 3) AS genre_3,
test.ratings."numVotes", test.ratings."averageRating"
FROM test.film 
join test.ratings on test.ratings.tconst = test.film.tconst 
and "startYear" != '\\N'  -- Escaping the backslash
and cast("startYear" as int) > 2000
and "titleType" = 'movie'
and "isAdult" = '0';"""

#Load manuel des variable de l'environement car le module ne fonctionne pas
def load_dotenv(filepath):
    """Manually load environment variables from a .env file."""
    with open(filepath) as f:
        for line in f:
            # Ignore empty lines and comments (lines starting with '#')
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip()


#Réécriture de la fonction de connection
def se_connecter_a_la_base_de_donnees():
        load_dotenv('.env')
        """Connexion à la base de données PostgreSQL."""
        host = os.getenv("HOST")
        port = os.getenv("PORT")
        utilisateur = os.getenv("USER")
        mot_de_passe = os.getenv("PASSWORD")
        nom_base_de_donnees = "postgres"

        try:
            connexion = psycopg2.connect(
                dbname=nom_base_de_donnees,
                user=utilisateur,
                password=mot_de_passe,
                host=host,
                port=port
            )
            print("Connexion réussie à la base de données")
            return connexion
        except psycopg2.Error as e:
            print(f"Erreur lors de la connexion à la base de données: {e}")
            return None




#Recupere un dataframe qui est pret a etre utiliser dans un modele type pycaret
def GetFrame():
    connection = se_connecter_a_la_base_de_donnees()
    frame = None
    if(connection):

        curseur = connection.cursor()
        try:
            curseur.execute(requete)
            frame = pd.DataFrame(curseur.fetchall())
            frame.columns = ['movieName', 'launchYear', 'runTimemins', 'genre_1','genre_2','genre_3',"numVotes","avgNote"]
            frame["avgNote"]=frame["avgNote"].apply(float)
        except:
            pass
        finally:
            curseur.close()
            connection.close()
            print("Connection fermer")
    return frame