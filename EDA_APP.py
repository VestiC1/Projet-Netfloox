import psycopg2
import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import seaborn as sns
import matplotlib.pyplot as plt
import streamlit as st

# Connexion à la base de données PostgreSQL sur Azure
def se_connecter_a_la_base():
    conn = psycopg2.connect(
        dbname="***",
        user="***",
        password="***",
        host="***"
        port="**",
        )

# Fonction pour obtenir les données des films
def obtenir_donnees_film(conn):
    query = """
    SELECT id_film, titre, note_moyenne, nombre_votes, genre
    FROM Film
    """
    df = pd.read_sql(query, conn)
    return df

# Algorithme de filtrage collaboratif basé sur k-NN
def filtrage_collaboratif(conn):
    df = obtenir_donnees_film(conn)
    film_ratings = df.pivot_table(index='id_film', columns='genre', values='note_moyenne', aggfunc=np.mean).fillna(0)
    
    knn = NearestNeighbors(n_neighbors=5, metric='cosine')
    knn.fit(film_ratings)
    
    return knn, film_ratings

# Fonction pour recommander un film basé sur le k-NN
def recommander_film(film_id, knn, film_ratings):
    distances, indices = knn.kneighbors([film_ratings.iloc[film_id]], n_neighbors=6)
    film_recommande = film_ratings.iloc[indices[0][1:]]  # Exclure le film demandé
    return film_recommande

# Algorithme de prédiction de la popularité (Régression Linéaire)
def obtenir_donnees_popularite(conn):
    query = """
    SELECT note_moyenne, nombre_votes, genre
    FROM Film
    """
    df = pd.read_sql(query, conn)
    return df

def predict_popularity(conn):
    df = obtenir_donnees_popularite(conn)
    
    X = df[['note_moyenne', 'nombre_votes']]  # Variables indépendantes
    y = df['note_moyenne']  # Variable dépendante (popularité)
    
    model = LinearRegression()
    model.fit(X, y)
    
    y_pred = model.predict(X)
    
    mse = mean_squared_error(y, y_pred)
    print(f"Mean Squared Error: {mse}")
    
    return model

# Streamlit pour visualiser les analyses
def visualiser_analyses():
    conn = se_connecter_a_la_base()
    df = obtenir_donnees_film(conn)

    # Affichage des 5 premiers films
    st.write("Top 5 Films:")
    st.dataframe(df.head())
    
    # Visualisation graphique de la répartition des films par genre
    st.write("Répartition des films par genre")
    plt.figure(figsize=(10,6))
    sns.countplot(x='genre', data=df)
    st.pyplot()

    # Visualisation graphique de la répartition des notes moyennes des films
    st.write("Répartition des notes moyennes des films")
    plt.figure(figsize=(10,6))
    sns.histplot(df['note_moyenne'], kde=True)
    st.pyplot()

    # Application de l'algorithme de recommandation
    film_id = st.number_input("ID du Film à recommander:", min_value=1, max_value=len(df), step=1)
    
    knn, film_ratings = filtrage_collaboratif(conn)
    if st.button("Recommander des films similaires"):
        recommandes = recommander_film(film_id-1, knn, film_ratings)
        st.write("Films recommandés:")
        st.dataframe(recommandes[['titre', 'note_moyenne']])

    # Prédiction de la popularité
    if st.button("Prédire la popularité"):
        model = predict_popularity(conn)
        st.write(f"Modèle de prédiction de la popularité créé avec succès.")

# Lancer l'application Streamlit
if __name__ == '__main__':
    visualiser_analyses()
