import streamlit as st

pg = st.navigation([st.Page("page_1.py"),st.Page("recommendation.py"),st.Page("prediction.py"),st.Page("request.py")])
pg.run()