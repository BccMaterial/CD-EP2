import pandas as pd
from neo4j import GraphDatabase

#Caminho para CSVs locais
pasta_neo4j = "./csv/"  #Ajuste para onde os CSVs estão no seu computador

#Ler CSVs com parsing robusto
movies_df = pd.read_csv(
    pasta_neo4j + "movies_amostra.csv",
    sep=",",
    quotechar='"',
    engine="python",
    skipinitialspace=True,
    encoding="utf-8",
    on_bad_lines='skip'  #ignora linhas mal formatadas
)

ratings_df = pd.read_csv(
    pasta_neo4j + "ratings_amostra.csv",
    sep=",",
    quotechar='"',
    engine="python",
    skipinitialspace=True,
    encoding="utf-8",
    on_bad_lines='skip'
)

#Limpeza e conversão de tipos
#Filtrar filmes com movieId ou title nulos
movies_df = movies_df.dropna(subset=['movieId', 'title'])
movies_df['movieId'] = movies_df['movieId'].astype(int)
movies_df['title'] = movies_df['title'].astype(str)
movies_df['genres'] = movies_df['genres'].fillna('').astype(str)

#Filtrar ratings inválidos
ratings_df = ratings_df.dropna(subset=['userId', 'movieId', 'rating'])
ratings_df['userId'] = ratings_df['userId'].astype(int)
ratings_df['movieId'] = ratings_df['movieId'].astype(int)
ratings_df['rating'] = ratings_df['rating'].astype(float)
ratings_df['timestamp'] = ratings_df['timestamp'].astype(int)

#Conferir dados
print("Filmes:")
print(movies_df.head())
print("\nRatings:")
print(ratings_df.head())

#Conexão Neo4j local
uri = "bolt://localhost:7687"  # seu Neo4j local
user = "neo4j"                 # usuário do Neo4j local
password = "senac_celso"       # senha do Neo4j local
driver = GraphDatabase.driver(uri, auth=(user, password))

#Inserção usando UNWIND
with driver.session() as session:

    #Inserir filmes
    filmes_lista = movies_df.to_dict('records')
    session.run("""
    UNWIND $filmes AS f
    MERGE (m:Filme {movieId: toInteger(f.movieId)})
    SET m.title = f.title, m.genres = f.genres
    """, filmes=filmes_lista)

    #Inserir usuários
    usuarios_lista = ratings_df['userId'].drop_duplicates().tolist()
    usuarios_dict = [{'userId': int(u)} for u in usuarios_lista]
    session.run("""
    UNWIND $usuarios AS u
    MERGE (usr:Usuario {userId: toInteger(u.userId)})
    """, usuarios=usuarios_dict)

    #Inserir relações Avaliacao
    avaliacoes_lista = ratings_df.to_dict('records')
    session.run("""
    UNWIND $avaliacoes AS a
    MATCH (u:Usuario {userId: toInteger(a.userId)})
    MATCH (f:Filme {movieId: toInteger(a.movieId)})
    MERGE (u)-[r:AVALIA]->(f)
    SET r.rating = toFloat(a.rating), r.timestamp = toInteger(a.timestamp)
    """, avaliacoes=avaliacoes_lista)

driver.close()
print("Inserção concluída!")