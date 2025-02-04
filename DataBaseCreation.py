import pandas as pd
from dotenv import load_dotenv
import os
import psycopg2
import psycopg2.extras
import requests


Schema = "Herry"

createFilm = f"""
CREATE TABLE IF NOT EXISTS {Schema}.film (
    tconst VARCHAR(50) PRIMARY KEY,
    titleType VARCHAR(50),
    primaryTitle VARCHAR(500),
    originalTitle VARCHAR(500),
    isAdult VARCHAR(5),
    startYear VARCHAR(10),
    endYear VARCHAR(10),
    runtimeMinutes VARCHAR(100),
    genres VARCHAR(100)
);
"""
createJob = f"""
CREATE TABLE IF NOT EXISTS {Schema}.job (
    tconst VARCHAR(50),
    ordering VARCHAR(50),
    nconst VARCHAR(50),
    category VARCHAR(50),
    job VARCHAR(50),
    characters VARCHAR(50),
    PRIMARY KEY(tconst,ordering)
);"""
createRatings = f"""
CREATE TABLE IF NOT EXISTS {Schema}.ratings (
    tconst VARCHAR(50) PRIMARY KEY,
    averageRating VARCHAR(50),
    numVotes VARCHAR(50),
);"""

filmInfo = {"url":"https://datasets.imdbws.com/title.basics.tsv.gz","path":"./title.basics.tsv.gz","createSQL":createFilm,"tableName":f"{Schema}.film"}
jobInfo = {"url":"https://datasets.imdbws.com/title.principals.tsv.gz","path":"./title.principals.tsv.gz","createSQL":createJob,"tableName":f"{Schema}.job"}
ratingsInfo = {"url":"https://datasets.imdbws.com/title.ratings.tsv.gz","path":"./title.ratings.tsv.gz","createSQL":createRatings,"tableName":f"{Schema}.ratings"}

listeAll = [filmInfo,jobInfo,ratingsInfo]

def se_connecter_a_la_base_de_donnees():
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

def telecharger_fichier(url, chemin_destination):
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

def inserer_donnees_en_bulk(connexion, table, donnees):
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
    except psycopg2.Error as e:
        print(f"Erreur lors de l'insertion des données: {e}")
        connexion.rollback()

def traiter_et_inserer_fichier_par_lots(url_fichier, connexion, table, taille_lot=1000000):
    """Lit et insère un fichier TSV en streaming pour éviter les problèmes de mémoire."""
    try:
        reader = pd.read_csv(f, sep='\t', chunksize=taille_lot, on_bad_lines='skip')
        for i, chunk in enumerate(reader):
            donnees = chunk.to_dict(orient='records')
            inserer_donnees_en_bulk(connexion, table, donnees)
            print(f"Chunk {i + 1} traité avec succès.")
    except Exception as e:
        print(f"Erreur lors du traitement du fichier : {e}")


def CreateAndImport(url,filePath,createRequest,connexion,tableName): 
        try:
            with connexion.cursor() as cursor:
                cursor.execute(createRequest)
        except:
            print("Erreur lors de la creation de table")
            connexion.rollback()
        telecharger_fichier(url, filePath)
        traiter_et_inserer_fichier_par_lots(filePath, connexion, tableName, taille_lot=200000)

if __name__ == "__main__":
    load_dotenv()
    connexion = se_connecter_a_la_base_de_donnees()

    if connexion:
        for info in listeAll:
            try:
                CreateAndImport(info["url"],info["filePath"],info["createRequest"],connexion,info["tableName"])
            except:
                connexion.rollback()
        connexion.close()
