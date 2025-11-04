import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_score, recall_score
from neo4j import GraphDatabase

ratings_df = pd.read_csv("./csv/ratings_amostra.csv")

#Divisão dos dados em treino e teste
train_df, test_df = train_test_split(ratings_df, test_size=0.2, random_state=42)

#Criaçã da matriz usuário x filme
ratings_matrix = train_df.pivot_table(index='userId', columns='movieId', values='rating')
ratings_matrix = ratings_matrix.fillna(0)

#Correlação de Perason entre filmes
correlation_matrix = ratings_matrix.corr(method='pearson')

#Utiliza KNN regreção
def prever_nota(user_id, movie_id, ratings_matrix, correlation_matrix, k=5):
    if movie_id not in correlation_matrix.columns:
        return np.nan

    user_ratings = ratings_matrix.loc[user_id]
    user_rated_movies = user_ratings[user_ratings > 0].index

    if len(user_rated_movies) == 0:
        return np.nan

    similaridades = correlation_matrix[movie_id].dropna()
    similaridades = similaridades[user_rated_movies]
    if similaridades.empty:
        return np.nan

    similaridades = similaridades.sort_values(ascending=False)[:k]

    numerador = (similaridades * user_ratings[similaridades.index]).sum()
    denominador = similaridades.abs().sum()

    if denominador == 0:
        return np.nan
    return numerador / denominador

#Previsão nos dados de teste
previsoes = []
reais = []

for _, row in test_df.iterrows():
    user_id = row['userId']
    movie_id = row['movieId']
    real = row['rating']

    if user_id not in ratings_matrix.index:
        continue

    pred = prever_nota(user_id, movie_id, ratings_matrix, correlation_matrix)
    if not np.isnan(pred):
        previsoes.append(pred)
        reais.append(real)

previsoes = np.array(previsoes)
reais = np.array(reais)

#Avalia a partir de Precisão e Recall
#Notas >= 3.5 são convertidas em "gostou" ou "não gostou"
threshold = 3.5
pred_bin = (previsoes >= threshold).astype(int)
real_bin = (reais >= threshold).astype(int)

precisao = precision_score(real_bin, pred_bin)
revocacao = recall_score(real_bin, pred_bin)

print("Precisão:", round(precisao, 3))
print("Recall:", round(revocacao, 3))


def recomendar_filmes(user_id, ratings_matrix, correlation_matrix, n=5, k_vizinhos=5):
  user_ratings = ratings_matrix.loc[user_id]
  filmes_nao_vistos = user_ratings[user_ratings == 0].index

  predicoes = {}
  for movie_id in filmes_nao_vistos:
    nota_pred = prever_nota(user_id, movie_id, ratings_matrix, correlation_matrix, k=k_vizinhos)
    if not np.isnan(nota_pred):
      predicoes[movie_id] = nota_pred

  top_filmes = sorted(predicoes.items(), key=lambda x: x[1], reverse=True)[:n]
  return top_filmes

print("\n Top-5 filmes recomendados para o usuário 1:")
recomendados = recomendar_filmes(1, ratings_matrix, correlation_matrix, n=5, k_vizinhos=5)
for movie_id, score in recomendados:
  print(f"Filme {movie_id} com nota prevista: {round(score, 2)}")


def exportar_correlacoes_neo4j(correlation_matrix, uri, user, password, limite=100):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        count = 0
        for i in correlation_matrix.columns:
            for j in correlation_matrix.columns:
                if i != j and not pd.isna(correlation_matrix.loc[i, j]):
                    corr = float(correlation_matrix.loc[i, j])
                    session.run("""
                    MATCH (f1:Filme {movieId: $i})
                    MATCH (f2:Filme {movieId: $j})
                    MERGE (f1)-[r:CORRELACIONADO_COM]->(f2)
                    SET r.c = $corr
                    """, i=int(i), j=int(j), corr=corr)
                    count += 1
                    if count >= limite:
                        break
            if count >= limite:
                break
    driver.close()
    print(f"{count} correlações exportadas ao Neo4j.")
