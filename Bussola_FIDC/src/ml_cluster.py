import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from src.database import get_connection

def segmentar_clientes():
    # Criando uma conexão com o BD e lendo todos os dados da View V_BF_TREINO_ML
    try:
        conn = get_connection()
        if conn:
            # Usando a View criada para o ML
            print("🔌 Conectou")
            query = "SELECT * FROM V_BF_TREINO_ML"
            df_treino = pd.read_sql(query, conn)
            df_treino.columns = [c.upper() for c in df_treino.columns]
            df_treino = df_treino.fillna(0)

    except Exception as e:
        print(f"❌ Erro: {e}")
        df_treino = pd.DataFrame()

    finally:
        conn.close()

    if not df_treino.empty:
        print(f"✅ Dados carregados: {len(df_treino)} registros.")

        # Selecionando as features para agregação
        cols_cluster = [
            'VL_NOMINAL',             # Tamanho da dívida
            'NR_PRAZO_DIAS',          # Perfil temporal
            'VL_SCORE_MATERIALIDADE', # Tamanho da empresa
            'VL_SCORE_QUANTIDADE',    # Histórico
            'TAX_SELIC',              # Contexto Econômico
            'VL_SENTIMENTO_SETORIAL'  # Notícias
        ]

        X = df_treino[cols_cluster]

        # Padronizando para o K-MEANS
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # K-MEANS com 4 clusters
        k = 4
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        clusters = kmeans.fit_predict(X_scaled)

        df_treino['NR_CLUSTER'] = clusters
        perfil_medio = df_treino.groupby('NR_CLUSTER')[['VL_NOMINAL', 'TARGET', 'VL_SCORE_MATERIALIDADE']].mean()

        nomes_clusters = {}

        for cluster_id, row in perfil_medio.iterrows():
          nome = f"Grupo {cluster_id}: "

          if row["TARGET"] > 0.7:
            nome += "Alto Risco "
          elif row["TARGET"] > 0.3:
            nome += "Médio Risco "
          else:
            nome += "Baixo Risco "

          if row['VL_NOMINAL'] > df_treino['VL_NOMINAL'].mean() * 1.5:
            nome += "(Ticket Alto/VIP)"
          elif row['VL_NOMINAL'] < df_treino['VL_NOMINAL'].mean() * 0.5:
            nome += "(Ticket Baixo)"
          elif row['VL_SCORE_MATERIALIDADE'] > df_treino['VL_SCORE_MATERIALIDADE'].mean():
            nome += "(Empresa Sólida)"
          else:
            nome += "(Perfil Comum)"

          nomes_clusters[cluster_id] = nome
          print(f"   -> Cluster {cluster_id} batizado de: '{nome}'")

        df_treino['DS_PERFIL'] = df_treino['NR_CLUSTER'].map(nomes_clusters)

        # Interpretando os novos grupos
        df_save = df_treino[['ID_BOLETO', 'NR_CLUSTER', 'DS_PERFIL']].copy()
        dados_insert = df_save.values.tolist()

    else:
        print("Sem dados para clusterizar.")


    # Alimentando a tabela de clusters
    try:
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            print("🔌Conectou")

            sql_insert = """
                INSERT INTO T_BF_CLUSTER (id_boleto, nr_cluster, ds_perfil)
                VALUES (:1, :2, :3)
            """
            cursor.executemany(sql_insert, dados_insert)
            conn.commit()
            print(f"✅ SUCESSO! {cursor.rowcount} previsões salvas na tabela T_BF_PREDICOES.")

    except Exception as e:
        print(f"❌ Erro ao salvar previsões: {e}")

    finally:
        conn.close()