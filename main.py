from neo4j import GraphDatabase
import pandas as pd

#Configuração Neo4j:
uri = "bolt://localhost:7687"
user = "neo4j"
password = "senac_celso"

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


def criar_relacao_avaliacao(tx, userId, movieId, rating, timestamp):
  query = """
  MATCH (u:Usuario {userId: $userId})
  MATCH (f:Filme {movieId: $movieId})
  WITH u, f
  MERGE (u)-[a:AVALIA]->(f)
  SET a.rating = $rating, a.timestamp = $timestamp
  RETURN u.userId AS u_id, f.movieId AS f_id
  """
  result = tx.run(query, userId=userId, movieId=movieId, rating=rating, timestamp=timestamp)

#Inserções:
with driver.session() as session:
  # Inserindo filmes
  for _, row in movies_df.iterrows():
    movie_id = int(row["movieId"])
    title = row["title"]
    genres = row["genres"]

    session.execute_write(insert_movie, movie_id, title, genres)
#Inserção de usuário e relações:
  for _, row in ratings_df.iterrows():
    session.execute_write(insert_usuario, row["userId"]) #inserção do usuário
    session.execute_write(criar_relacao_avaliacao, row["userId"], row["movieId"], row["rating"], row["timestamp"])

driver.close()
print("Inserção concluída.")