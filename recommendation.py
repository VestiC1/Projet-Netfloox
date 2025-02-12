import joblib
import streamlit as st
import pandas as pd

data = pd.read_csv("dataPrep.csv",index_col=["index"])
# Fonction pour charger le model
def load_model():
    model = joblib.load('recomodel.joblib')
    return model 

# Fonction qui vérifie si le model est deja dans la session state
if "model" not in st.session_state:
    st.session_state.model = load_model()
    "Model Charger"

#Systeme de selection d'un film dans data TODO
selection = st.selectbox(label="Veuillez selectionner un film",options=data["movieName"])
donne = data[data["movieName"]==selection]
donne = donne.drop(columns=["movieName","Unnamed: 0","genres"])


if st.button("Predit des recommendations"):
    distance, indices = st.session_state.model.kneighbors(donne, n_neighbors=6)
    film_recommande = data.iloc[indices[0][1:]]  # Exclure le film demandé
    film_recommande["movieName"]