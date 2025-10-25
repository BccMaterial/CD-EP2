from neo4j import GraphDatabase
import pandas as pd
import env

#Conexão com o Neo4j:
driver = GraphDatabase.driver(uri, auth=(user, password))

#Leitura dos csv:
movies_df = pd.read_csv("./csv/movies_amostra.csv")
ratings_df = pd.read_csv("./csv/ratings_amostra.csv")

#Modelagem do grafo:
#Vértice "Usuário" com o atributo "userId"
#Vértice "Filme" com os atributos "movieid", "title" e "genres"
#Relação de "Usuário" para "Filme" chamada "Avaliação" com os atributos "rating" e "timestamp"

def insert_usuario(tx, userId): #Completar e verificar
  query = """
  MERGE(u:Usuario {userId: $userId})
  """
  tx.run(query, userId=userId)

def insert_movie(tx, movieId, title, genres): #Completar e verificar
  query = """
  MERGE(f:Filme {movieId: $movieId} ) 
  SET f.title = $title, f.genres = $genres
  """
  tx.run(query, movieId=movieId, title=title, genres=genres)

def criar_relacao_avaliacao(tx, userId, movieId, rating, timestamp): #Completar e verificar
  query = """
    MATCH = (u:Usuario {userId: $userId})
    MATCH = (f:Filme {movieId: $movieId})
    Merge = (u) -[a:AVALIA]->(f)
    SET a.rating = $rating, a.timestamp = $timestamp
    """
  tx.run(query, userId=userId, movieId=movieId, rating=rating, timestamp=timestamp)

#Inserções:
with driver.session() as session: #driver.session() é o objeto utilizado para realizar queries no banco de dados
  for _, row in movies_df.iterrows(): #row é uma linha do csv convertida em uma Series do pandas
    session.execute_write(insert_movie, row["movieId"], row["title"], row["genres"])

#Inserção de usuário e relações:
  for _, row in ratings_df.itemarrows():
    session.execute_write(insert_usuario, row["userId"]) #inserção do usuário

    session.execute_write(criar_relacao_avaliacao, row["userId"], row["movieId"], row["rating"], row["timestamp"])

driver.close()
print("Inserção concluída.")