import pandas as pd
import os
import psycopg2
import psycopg2.extras
import requests
import sys
import gzip
import math

#Load manuel des variable de l'environement car le module ne semble pas disponible pour la version 3.11 avec pycaret
def load_dotenv(filepath):
    """Manually load environment variables from a .env file."""
    with open(filepath) as f:
        for line in f:
            # Ignore empty lines and comments (lines starting with '#')
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip()



#Une classe qui contient les schema de creation de table ainsi qu'un loader des datas depuis un site web
class DataBaseManager:
    def __init__(self):
        pass
    Schema = "test"

    createFilm = f"""CREATE TABLE IF NOT EXISTS {Schema}.film (
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
"""
    createJob = f"""
CREATE TABLE IF NOT EXISTS {Schema}.job (
    tconst VARCHAR(50),
    ordering VARCHAR(50),
    nconst VARCHAR(50),
    category VARCHAR(200),
    job VARCHAR(200),
    characters VARCHAR(200),
    PRIMARY KEY(tconst,ordering)
);"""
    createRatings = f"""
CREATE TABLE IF NOT EXISTS {Schema}.ratings (
    tconst VARCHAR(50) PRIMARY KEY,
    "averageRating" VARCHAR(50),
    "numVotes" VARCHAR(50)
);"""
    createPeople = f"""
CREATE TABLE IF NOT EXISTS {Schema}.people(
    nconst VARCHAR(50),
    "primaryName" VARCHAR(200),
    "birthYear" VARCHAR(50),
    "deathYear" VARCHAR(50),
    "primaryProfession" VARCHAR(200),
    "knownForTitles" VARCHAR(200)
)
"""

    filmInfo = {"url":"https://datasets.imdbws.com/title.basics.tsv.gz","path":"./title.basics.tsv.gz","createTable":createFilm,"tableName":f"{Schema}.film"}
    jobInfo = {"url":"https://datasets.imdbws.com/title.principals.tsv.gz","path":"./title.principals.tsv.gz","createTable":createJob,"tableName":f"{Schema}.job"}
    ratingsInfo = {"url":"https://datasets.imdbws.com/title.ratings.tsv.gz","path":"./title.ratings.tsv.gz","createTable":createRatings,"tableName":f"{Schema}.ratings"}
    peopleInfo = {"url":"https://datasets.imdbws.com/name.basics.tsv.gz","path":"./name.basics.tsv.gz","createTable":createPeople,"tableName":f"{Schema}.people"}


    listeAll = [filmInfo,jobInfo,ratingsInfo,peopleInfo]

    #Renvoi la connection ou None
    def se_connecter_a_la_base_de_donnees(self):
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

    def telecharger_fichier(self,url, chemin_destination):
        """Télécharge un fichier depuis une URL."""
        try:
            os.makedirs(os.path.dirname(chemin_destination), exist_ok=True)
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(chemin_destination, 'wb') as fichier:
                for chunk in response.iter_content(chunk_size=8192):
                    fichier.write(chunk)
            print(f"Fichier téléchargé avec succès : {chemin_destination}")
        except requests.RequestException as e:
            print(f"Erreur lors du téléchargement du fichier : {e}")

    def inserer_donnees_en_bulk(self,connexion, table, donnees):
        """Insère les données en bulk pour une importation plus rapide."""
        try:
            with connexion.cursor() as curseur:
                colonnes = list(donnees[0].keys())
                colonnes_formattees = ", ".join([f'"{col}"' for col in colonnes])
                requete = f"INSERT INTO {table} ({colonnes_formattees}) VALUES %s"
                valeurs = [[d[col] for col in colonnes] for d in donnees]
                psycopg2.extras.execute_values(curseur, requete, valeurs, page_size=10000)
                connexion.commit()
                print(f"Insertion bulk terminée avec succès pour {len(valeurs)} lignes.")
                return True
        except psycopg2.Error as e:
            print(f"Erreur lors de l'insertion des données: {e}")
            connexion.rollback()
            return False

    def traiter_et_inserer_fichier_par_lots(self,filePath, connexion, table, taille_lot=1000000,maxError=5):
        """Lit et insère un fichier TSV en streaming pour éviter les problèmes de mémoire."""
        errorNumber =  0
        try:
            with gzip.open(filePath, 'rt', encoding='utf-8') as f:
                line_count = sum(1 for _ in f)
                chunkCount= math.ceil(line_count/taille_lot)
                print(f"Nombres de lignes : {line_count}")
                reader = pd.read_csv(filePath, sep='\t', chunksize=taille_lot, on_bad_lines='skip')

                for i, chunk in enumerate(reader):
                    donnees = chunk.to_dict(orient='records')
                    if(self.inserer_donnees_en_bulk(connexion, table, donnees)):
                        print(f"Chunk {i + 1}/{chunkCount} traité avec succès.")
                    else:
                        errorNumber+=1
                        if(errorNumber>maxError):
                            raise Exception("Trop d'erreur dans l'insertion des chunks")
        except Exception as e:
            print(f"Erreur lors du traitement du fichier : {e}")


    #Creer la table en parametres
    def CreateTable(self,connexion,createRequest,tableName):
        print(f"{createRequest} request , {connexion} connexion")
        try:
            with connexion.cursor() as cursor:
                print("curseurCreer")
                cursor.execute(createRequest)
                print(f"Table Creer : {tableName}")
                connexion.commit()
        except Exception as e:
            print(f"Erreur lors de la creation de table {e}")
            connexion.rollback()
            
    #Telecharge les fichiers et les inserere
    def TeleInsertData(self,url,filePath,connexion,tableName):
        self.telecharger_fichier(url, filePath)
        self.traiter_et_inserer_fichier_par_lots(filePath, connexion, tableName, taille_lot=200000)
        print(f"Table Remplis : {tableName}")

    #Prend en paramaetres la liste des table que l'on souhaite manipuler et se que l'on veux faire
    def IterateTable(self,listeInfo,create=True,doImport=True):
        print(f"Creation de {len(listeInfo)} Tables")
        for info in listeInfo:
            try:
                if(create and doImport):
                    self.CreateTable(connexion,info["createTable"],info["tableName"])
                    self.TeleInsertData(info["url"],info["path"],connexion,info["tableName"])
                elif(create):
                    self.CreateTable(connexion,info["createTable"],info["tableName"])
                elif(doImport):
                    self.TeleInsertData(info["url"],info["path"],connexion,info["tableName"])
                else:
                    print("Erreur de parametres demande d'iteration sans creer ni inserer")
            except Exception as e:
                connexion.rollback()
                print(f"Erreur dans le processus {e}")

if __name__ == "__main__":
    load_dotenv('.env')
    DBManag = DataBaseManager()
    connexion = DBManag.se_connecter_a_la_base_de_donnees()
    arguments = sys.argv
    if connexion:
        #Gere les parametres avec un cas par default et si on veux juste creer ou importer

        match len(arguments):
            case 1:
                listeInfo = DBManag.listeAll
                create = True
                doImport = True
            case 2:
                listeInfo = DBManag.listeAll
                if(arguments[1]=="Create"):
                    create=True
                    doImport=False
                if(arguments[1]=="Import"):
                    create=True
                    doImport=True
                if(arguments[1]=="Job"):
                    create=True
                    doImport=True
                    listeInfo=[DBManag.jobInfo]
                if(arguments[1]=="Ratings"):
                    create=True
                    doImport=True
                    listeInfo=[DBManag.ratingsInfo]
                if(arguments[1]=="People"):
                    create = True
                    doImport = True
                    listeInfo=[DBManag.peopleInfo]
        DBManag.IterateTable(listeInfo,create,doImport)      
    connexion.close()
    print("Deconnexion de la base")

    """
    CAN DO :
    -Savoir combien de chunks il doit traiter pour donner une estimation du temps necessaire
    -Decoupage en plusieurs fichier
    -Gerer la taille des colonnes et leur insertion
    """