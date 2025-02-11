# Projet-Netfloox-G1

## BDD 
Script SQL création des tables : `CreationSQL.sql` 
Structure complète :
![DrawSQL.png](data/DrawSQL.png)
Structure dans schéma 'test'
![StructureBDD.png](data/StructureBDD.png)

### remplissage des données
Script DataBaseCreation.py : remplissage (filmInfo,jobInfo,ratingsInfo,peopleInfo)

## Application

### Installation

Installation env:
```bash
 python3 -m venv venv
 source venv/bin/activate
 pip install -r requirements.txt
 #pip freeze
```

Environnement :  
Un fichier .env doit être fourni.  
Voir .env.template (à remplir)

### Appli Streamlit

app.py, pages_*.py : appli streamlit (port 8501)
```bash
 streamlit run app.py
```