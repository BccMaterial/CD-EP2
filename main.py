from neo4j import GraphDatabase
import pandas as pd
import env

#Leitura dos csv:
movies_df = pd.read_csv("/csv/movies_amostra.csv")
ratings_df = pd.read_csv("/csv/ratings_amostra.csv")

#Conexão com o Neo4j:
driver = GraphDatabase.driver(env.db_uri, auth=(env.db_user, env.db_password))

#Modelagem do grafo:
#Vértice "Usuário" com o atributo "userId"
#Vértice "Filme" com os atributos "movieid", "title" e "genres"
#Relação de "Usuário" para "Filme" chamada "Avaliação" com os atributos "rating" e "timestamp"

#def insert_ratings(tx, userId, movieId, rating, timestamp):


#def insert_movies(tx, movieId, rating, timestamp):
