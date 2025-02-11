import os
import gzip
import pandas as pd
import psycopg2
import psycopg2.extras
import requests

# Chargement des variables d'environnement

def se_connecter_a_la_base_de_donnees():
    """Connexion à la base de données PostgreSQL."""
    nom_base_de_donnees = "***"
    utilisateur = "***"
    mot_de_passe = "***"
    host = "***"
    port = "5432"

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

# Téléchargement des fichiers IMDb
def telecharger_fichier(url, chemin_destination):
    """Télécharge un fichier IMDb depuis une URL."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(chemin_destination, 'wb') as fichier:
            for chunk in response.iter_content(chunk_size=8192):
                fichier.write(chunk)
        print(f"📥 Fichier téléchargé avec succès : {chemin_destination}")
    except requests.RequestException as e:
        print(f"❌ Erreur lors du téléchargement : {e}")

# Insertion en masse des données
def inserer_donnees_en_bulk(connexion, table, donnees):
    """Insère les données en batch pour optimiser la performance."""
    if not donnees:
        print(f"⚠️ Aucun enregistrement à insérer pour {table}.")
        return

    try:
        with connexion.cursor() as curseur:
            colonnes = list(donnees[0].keys())
            colonnes_formattees = ", ".join([f'"{col}"' for col in colonnes])
            requete = f'INSERT INTO imdb.{table} ({colonnes_formattees}) VALUES %s ON CONFLICT DO NOTHING'
            valeurs = [[d[col] for col in colonnes] for d in donnees]
            psycopg2.extras.execute_values(curseur, requete, valeurs, page_size=5000)
            connexion.commit()
            print(f"✅ {len(valeurs)} lignes insérées dans {table}.")
    except psycopg2.Error as e:
        print(f"❌ Erreur d'insertion dans {table} : {e}")
        connexion.rollback()

# Traitement des fichiers IMDb et insertion dans la base
def traiter_et_inserer_fichier(chemin_fichier, connexion, table, mapping, taille_lot=500000):
    """Lit un fichier IMDb .tsv.gz et insère les données correspondantes dans PostgreSQL."""
    try:
        with gzip.open(chemin_fichier, 'rt', encoding='utf-8') as f:
            reader = pd.read_csv(f, sep='\t', chunksize=taille_lot, on_bad_lines='skip', dtype=str)
            for i, chunk in enumerate(reader):
                chunk.rename(columns=mapping, inplace=True)
                
                # ✅ Remplacement des valeurs '\N' par None (NULL en SQL)
                chunk.replace(r'\N', None, inplace=True)

                # ✅ Correction de l'ID du film (ex: 'tt123456' → 123456)
                if "id_film" in mapping.values():
                    chunk["id_film"] = chunk["id_film"].str.replace("tt", "", regex=True)
                    chunk["id_film"] = pd.to_numeric(chunk["id_film"], errors="coerce")  # Convertir en entier

                # ✅ Correction de la colonne durée (convertir en int, NaN si invalide)
                if "durée" in mapping.values():
                    chunk["durée"] = pd.to_numeric(chunk["durée"], errors='coerce')

                # ✅ Correction du titre (tronquer à 512 caractères max)
                if "titre" in mapping.values():
                    chunk["titre"] = chunk["titre"].str.slice(0, 512)

                # ✅ Correction des genres (ne prendre que le premier genre)
                if "genre" in mapping.values():
                    chunk["genre"] = chunk["genre"].str.split(',').str[0]

                # 🔍 Supprimer les lignes sans ID film (évite les erreurs SQL)
                donnees = chunk[list(mapping.values())].dropna(subset=["id_film"]).to_dict(orient='records')

                # 🚀 Insertion dans PostgreSQL
                inserer_donnees_en_bulk(connexion, table, donnees)
                print(f"📦 Chunk {i + 1} traité et inséré dans {table}.")
                
    except Exception as e:
        print(f"❌ Erreur lors du traitement du fichier {chemin_fichier} : {e}")


if __name__ == "__main__":
    connexion = se_connecter_a_la_base_de_donnees()

    if connexion:
        # URLs IMDb
        fichiers_imdb = {
            "film": {
                "url": "https://datasets.imdbws.com/title.basics.tsv.gz",
                "chemin": "./title.basics.tsv.gz",
                "table": "film",
                "mapping": {
                    "tconst": "id_film",
                    "primaryTitle": "titre",
                    "startYear": "année_sortie",
                    "runtimeMinutes": "durée",
                    "genres": "genre"
                }
            },
            "acteur": {
                "url": "https://datasets.imdbws.com/name.basics.tsv.gz",
                "chemin": "./name.basics.tsv.gz",
                "table": "acteur",
                "mapping": {
                    "nconst": "id_acteur",
                    "primaryName": "nom",
                    "birthYear": "date_naissance"
                }
            },
            "realisateur": {
                "url": "https://datasets.imdbws.com/title.principals.tsv.gz",
                "chemin": "./title.principals.tsv.gz",
                "table": "realiser",
                "mapping": {
                    "tconst": "id_film",
                    "nconst": "id_realisateur"
                }
            },
            "avis": {
                "url": "https://datasets.imdbws.com/title.ratings.tsv.gz",
                "chemin": "./title.ratings.tsv.gz",
                "table": "avis",
                "mapping": {
                    "tconst": "id_film",
                    "averageRating": "note",
                    "numVotes": "nombre_votes"
                }
            }
        }

        # Téléchargement et insertion
        for cle, fichier in fichiers_imdb.items():
            telecharger_fichier(fichier["url"], fichier["chemin"])
            traiter_et_inserer_fichier(fichier["chemin"], connexion, fichier["table"], fichier["mapping"])

        connexion.close()
        print("🔗 Connexion à la base de données fermée.")
