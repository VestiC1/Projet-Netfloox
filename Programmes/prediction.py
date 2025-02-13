import pandas as pd
import streamlit as st
from pycaret.regression import RegressionExperiment
s = RegressionExperiment()

# Fonction pour charger le model
def load_model():
    model = s.load_model('my_best_pipeline')
    return model 


# Fonction qui vérifie si le model est deja dans la session state
if "modelpred" not in st.session_state:
    st.session_state.modelpred = load_model()
    "Model Charger"


#A modifier
# Movie Name input field
movieName = st.text_input("Nom du film", "l'homme bicentenaire")
# Set up the layout with 6 columns for each input field
col2, col3, col4, col5, col6, col7 = st.columns(6)



# Launch Year input field
launchYear = col2.number_input("Sortie en", min_value=2000, max_value=2100, value=2025)

# Runtime in minutes input field
runTimemins = col3.number_input("Runtime (min)", min_value=1, value=98)

# Genre 1 input field
genre_1 = col4.text_input("Genre 1", "")

# Genre 2 input field
genre_2 = col5.text_input("Genre 2", "")

# Genre 3 input field
genre_3 = col6.text_input("Genre 3", "")

# Number of Votes input field
numVotes = col7.number_input("Nb votes", min_value=0, value=5549)


#Parti mise en dataFrame
data = [movieName, launchYear, runTimemins, genre_1, genre_2, genre_3, numVotes]
# Nom des colonnes
columns = ['movieName', 'launchYear', 'runTimemins', 'genre_1','genre_2','genre_3',"numVotes"]
# Creer le dataFrame
df = pd.DataFrame([data], columns=columns)


if(st.button("Predit la popularité")):
    prediction = s.predict_model(st.session_state.modelpred,df)
    note = prediction["prediction_label"][0]
    st.write(f"La note prédite du film est de {note:.2f} + ou - 1.39")

