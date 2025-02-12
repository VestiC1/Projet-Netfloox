import streamlit as st
import psycopg2
import pandas as pd
import DataBaseCreation as dbm
sqlText = st.text_area("Votre requete SQL")

bdM = dbm.DataBaseManager()

# Use the connection from session_state
connection = st.session_state.connection
if st.button("Requete"):
    "Requete :"
    connection= bdM.se_connecter_a_la_base_de_donnees()
    # Use the connection for querying or any other database operations
    if connection:
        try:
            # Example query
            cursor = st.session_state.curseur
            cursor.execute(sqlText)
            frame = pd.DataFrame(cursor.fetchall())
            st.dataframe(frame)
        except psycopg2.Error as e:
            st.error(f"Error querying the database: {e}")
        finally:
            connection.close()
            print("Connection fermer")


# Add a button to close the connection manually
#if st.button("Close Connection"):
 #   close_connection()
