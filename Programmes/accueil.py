import streamlit as st

st.title("Présentation")

st.image("ImageAccueil.png")

text=f"""
Bienvenue sur l'application streamlit netfloox du groupe 2.\n
Cette application se découper en 3 partie :"""

text2 ="""
    recommendation : on peut selectionner un film parmis la liste disponible et notre IA vous recommendera 5 autre film simillaire\n
    prediction : on peut rentrer des informations d'un film imaginaire et notre modele IA predit la note de ce film\n
    requete : on peut faire des requetes SQL pour interoger notre base de données
"""

st.write(text)
st.write(text2)