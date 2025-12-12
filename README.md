# Structure

Dieses Repository enthält alle nötigen SQL-Anweisungen sowie Python-Skripts, um unsere Datenbank in PostgreSQL zu erstellen, mit Beispiel-Daten zu füllen und anschliessend mit nützlichen Queries zu testen. 

```bash
├── README.md
├── database_creation_entities.ipynb # Beinhaltet CREATE TABLE Anweisungen, um Entitäts-Tabellen zu erstellen in der Datenbank
├── database_creation_relations.ipynb # Enthält CREATE TABLE Anweisungen, um Relations-Tabellen zu erstellen in der Datenbank
├── faker-data.py # Enthält Python-Skript, um Dummy-Daten in die Datenbank einzufügen
└── functionality_queries.ipynb # Beinhaltet Queries, um die Funktionalität und Use Cases der Datenbank zu testen
```

# Running Insructions
Um die Datenbank erstellen zu können, muss in den Jupyter-Notebooks sowie im Python-Script jeweils die Konfguration von PostgreSQL angepasst werden.

### Jupyter Notebooks
```bash
%sql postgresql://USER_NAME:USER_PASSWORD@DB_HOST:DB_PORT/DATABASE_NAME
```
Hier müssen die entsprechenden Werte eingefüllt werden, um die Verbindung mit PostgreSQL zu ermöglichen. Ein Beispiel ist in unseren Jupyter-Notebooks zu finden.
In unserem Fall haben wir `airportmanagementsystem` als Namen für unsere Datenbank genutzt.

### Python-Skript
```bash
DB_NAME = "airportmanagementsystem" # Namen der Datenbank
DB_USER = "thierrysuhner" # Benutzername des Users
DB_PASSWORD = "1234"      # Passwort des Users
DB_HOST = "localhost"     # Hostname der Datenbank
DB_PORT = "5432"          # Port der Datenbank
NUM_ROWS = 200            # Anzahl der Zeilen mit Beispiel-Daten
```

Diese Parameter müssen ähnlich wie im Jupyter-Notebook ganz zuoberst im Python-Skript angepasst werden, damit das Skript einwandfrei laufen kann.
Zusätzlich kann hier mittels der `NUM_ROWS` Variable bestimmt werden, wie viele Dummy-Daten pro Relation eingefügt werden sollen von Faker.


# Team
* Thierry Suhner
* Karla Ruggaber
* Lukas Kapferer