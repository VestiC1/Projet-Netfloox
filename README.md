# Projet-Netfloox-G1

## BDD 
Script SQL création des tables : `CreationSQL.sql` 
Structure complète :
![DrawSQL.png](data/DrawSQL.png)
Structure dans schéma 'test'
![StructureBDD.png](data/StructureBDD.png)

## Application

### Installation

Installation venv python :
```bash
 python3 -m venv venv
 source venv/bin/activate
 pip install -r requirements.txt
 #pip freeze
```

Environnement :  
Un fichier .env doit être fourni.  
Voir .env.template (à remplir)

### remplissage des données

**DataBaseCreation.py** [Create|Import|Job|Ratings|People]: remplissage (filmInfo,jobInfo,ratingsInfo,peopleInfo)
**import_SAMPLE.py** : fait la même chose (TBD : utiliser le .env)

**request.py** : debug avec streamlit

### Appli Streamlit

app.py, pages_*.py : appli streamlit (port 8501)
```bash
 streamlit run app.py
```

### Modélisation

Utilisation de pycaret -> utilisation de python **3.11**.

1) Modélisation : création modèle (**my_best_pipeline**) par notebook 
2) Utilisation dans **prediction.py**

**EDA_APP.py** : Modélisation 