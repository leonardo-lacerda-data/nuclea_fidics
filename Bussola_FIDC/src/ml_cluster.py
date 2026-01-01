import joblib
import numpy as np
import os
import pandas as pd
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler
from src.db_connection import get_connection

# Configuração de Diretórios
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_DIR = os.path.join(BASE_DIR, 'models')
os.makedirs(MODEL_DIR, exist_ok=True)

ARQUIVO_MODELO = os.path.join(MODEL_DIR, 'modelo_cluster.pkl')
ARQUIVO_SCALER = os.path.join(MODEL_DIR, 'scaler_cluster.pkl')

def nomear_cluster(df_clientes, resumo):
    apelidos = {}
    media_atraso_geral = df_clientes['VL_MEDIO_DIAS_ATRASO'].mean()
    media_ticket_geral = df_clientes['VL_TICKET_MEDIO'].mean()
    for cid, row in resumo.iterrows():
        if row['VL_MEDIO_DIAS_ATRASO'] > media_atraso_geral + 5:  # 5 dias acima da média
            apelidos[cid] = 'Perfil Devedor/Atrasado'
        elif row['VL_TICKET_MEDIO'] > media_ticket_geral * 1.5:
            apelidos[cid] = 'Cliente VIP (Ticket Alto)'
        elif row['NR_FREQUENCIA_COMPRA'] > df_clientes['NR_FREQUENCIA_COMPRA'].quantile(0.7):
            apelidos[cid] = 'Cliente Recorrente/Fiel'
        else:
            apelidos[cid] = 'Cliente Padrão/Esporádico'

    return(apelidos)

def alimentar_tabela(df_final, conn):
    print("   💾 Salvando tabela T_BF_CLUSTER...")
    cursor = conn.cursor()

    # Limpa tabela antiga
    cursor.execute("TRUNCATE TABLE T_BF_CLUSTER")

    sql_insert = """
                 INSERT INTO T_BF_CLUSTER (id_boleto, nr_cluster, ds_perfil)
                 VALUES (:1, :2, :3) \
                 """

    dados_insert = []
    for index, row in df_final.iterrows():
        perfil_txt = row['DS_PERFIL']
        if row['FLAG_ANOMALIA'] == 1:
            perfil_txt += " [ALERTA ANOMALIA]"

        # Usamos 0 como dummy para nr_cluster pois o texto já diz tudo
        dados_insert.append((row['ID_BOLETO'], row['CLUSTER_ID'], perfil_txt))

    cursor.executemany(sql_insert, dados_insert)
    conn.commit()
    print(f"   ✅ Sucesso! {len(dados_insert)} boletos classificados.")

def segmentar_clientes(force_retrain = False):
    print("\n🧩 INICIANDO MOTOR DE SEGMENTAÇÃO COMPORTAMENTAL...")

    conn = get_connection()
    if not conn: return

    # 1. Carregar Dados Brutos (Agrupados por Cliente/Pagador)
    # Em vez de olhar boleto a boleto, olhamos o HISTÓRICO do cliente.
    print("   👥 Analisando perfil de comportamento dos pagadores...")

    query = "SELECT * FROM V_BF_TREINO_ML_CLUSTER"
    try:
        df_clientes = pd.read_sql(query, conn)
        df_clientes = df_clientes.fillna(0)

        if df_clientes.empty:
            print("   ⚠️ Sem dados suficientes para clusterizar.")
            return

        # Pré-processamento (Escalar é obrigatório para K-Means e DBSCAN)
        # Usamos Frequência, Ticket Médio e Atraso para definir quem é o cliente
        cols_features = ['NR_FREQUENCIA_COMPRA', 'VL_TICKET_MEDIO', 'VL_MEDIO_DIAS_ATRASO']
        X = df_clientes[cols_features].fillna(0)

        # K-MEANS
        kmeans = None
        scaler = None

        if os.path.exists(ARQUIVO_MODELO) and os.path.exists(ARQUIVO_SCALER) and force_retrain == False:
            print("   📂 Modelo Cluster encontrado. Carregando...")
            kmeans = joblib.load(ARQUIVO_MODELO)
            scaler = joblib.load(ARQUIVO_SCALER)
            X_scaled = scaler.transform(X)

        elif force_retrain == True:
            print("   🎨 Definindo Perfis (K-Means)...")
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            # 4 Clusters: VIP, Comum, Risco, Pequeno
            kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
            kmeans.fit(X_scaled)

            joblib.dump(kmeans, ARQUIVO_MODELO)
            joblib.dump(scaler, ARQUIVO_SCALER)


        # Lógica de Negócio para dar nomes aos Clusters
        df_clientes['CLUSTER_ID'] = kmeans.predict(X_scaled)
        resumo = df_clientes.groupby('CLUSTER_ID')[cols_features].mean()

        nomeacao = {}
        nomeacao = nomear_cluster(df_clientes, resumo)

        df_clientes['DS_PERFIL'] = df_clientes['CLUSTER_ID'].map(nomeacao)

        # Mostra resultado no console
        print(f"      -> Perfis identificados:\n{df_clientes['DS_PERFIL'].value_counts()}")

        # DBSCAN (DETECÇÃO DE ANOMALIAS)
        print("   🕵️‍♂️ Caçando Anomalias (DBSCAN)...")
        # eps=1.0: Distância para considerar vizinho
        # min_samples=3: Mínimo de vizinhos para ser um grupo
        dbscan = DBSCAN(eps=1.0, min_samples=3)
        clusters_db = dbscan.fit_predict(X_scaled)

        # DBSCAN marca outliers com -1
        df_clientes['FLAG_ANOMALIA'] = np.where(clusters_db == -1, 1, 0)

        qtd_anomalias = df_clientes['FLAG_ANOMALIA'].sum()
        print(f"      🚨 {qtd_anomalias} clientes com comportamento atípico detectados.")

        # 3. Expandir de volta para os Boletos
        # O Power BI mostra boletos, então precisamos ligar o Perfil do Cliente ao Boleto dele
        print("   🔄 Mapeando perfis de volta para os boletos...")

        # Pegamos todos os IDs de boletos e seus pagadores
        query_boletos = "SELECT ID_BOLETO, ID_PAGADOR FROM T_BF_BOLETO"
        df_boletos = pd.read_sql(query_boletos, conn)

        # Join (Merge) Pandas: Junta Boleto com o Perfil do Cliente
        df_final = df_boletos.merge(
            df_clientes[['ID_PAGADOR', 'CLUSTER_ID', 'DS_PERFIL', 'FLAG_ANOMALIA']],
            on='ID_PAGADOR',
            how='inner'
        )

        # Salvar no Banco
        alimentar_tabela(df_final, conn)


    except Exception as e:
        print(f"❌ Erro no Clustering: {e}")
        conn.rollback()
    finally:
        conn.close()