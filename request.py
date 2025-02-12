import streamlit as st
import psycopg2
import pandas as pd
import DataBaseCreation as dbm
sqlText = st.text_area("Votre requete SQL")

bdM = dbm.DataBaseManager()
"""
# Function to create a database connection
def create_connection():
    connection = bdM.se_connecter_a_la_base_de_donnees()
    return connection , connection.cursor()

# Function to close the connection
def close_connection():
    if "connection" in st.session_state and st.session_state.connection:
        try:
            st.session_state.connection.close()
            st.session_state.connection = None
            st.write("Database connection fermer.")
        except Exception as e:
            st.error(f"Error closing the connection: {e}")
    if "curseur" in st.session_state and st.session_state.curseur:
        try:
            st.session_state.curseur.close()
            st.session_state.curseur = None
            st.write("Database curseur fermer.")
        except Exception as e:
            st.error(f"Error closing the connection: {e}")

# Check if connection already exists in session_state
if "connection" not in st.session_state:
    st.session_state.connection,st.session_state.curseur = create_connection()
    "Connection et curseur Creer"
"""
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
