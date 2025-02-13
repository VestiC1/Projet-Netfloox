import streamlit as st

pg = st.navigation([st.Page("accueil.py"),st.Page("recommendation.py"),st.Page("prediction.py"),st.Page("request.py")])
pg.run()