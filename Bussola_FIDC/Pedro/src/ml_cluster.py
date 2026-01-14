import joblib
import numpy as np
import os
import pandas as pd
from src.utils_paths import resource_path
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler
from src.db_connection import get_connection

MODEL_DIR = resource_path("models")
os.makedirs(MODEL_DIR, exist_ok=True)

ARQUIVO_MODELO = os.path.join(MODEL_DIR, 'modelo_cluster.pkl')
ARQUIVO_SCALER = os.path.join(MODEL_DIR, 'scaler_cluster.pkl')


def nomear_cluster(df_clientes, resumo):
    apelidos = {}
    limite_atraso = df_clientes['VL_MEDIO_DIAS_ATRASO'].quantile(0.80)
    limite_vip = df_clientes['VL_TICKET_MEDIO'].quantile(0.90)
    for cid, row in resumo.iterrows():
        if row['VL_MEDIO_DIAS_ATRASO'] > limite_atraso:
            apelidos[cid] = 'Perfil Devedor/Atrasado'
        elif row['VL_TICKET_MEDIO'] > limite_vip:
            apelidos[cid] = 'Cliente VIP (Ticket Alto)'
        elif row['NR_FREQUENCIA_COMPRA'] > df_clientes['NR_FREQUENCIA_COMPRA'].quantile(0.7):
            apelidos[cid] = 'Cliente Recorrente/Fiel'
        else:
            apelidos[cid] = 'Cliente Padrão/Esporádico'
    return apelidos


def alimentar_tabela(df_final, conn):
    print("   💾 Salvando tabela T_BF_CLUSTER...")
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE T_BF_CLUSTER")

    sql = "INSERT INTO T_BF_CLUSTER (id_boleto, nr_cluster, ds_perfil) VALUES (:1, :2, :3)"

    dados_insert = []
    for _, row in df_final.iterrows():
        perfil_txt = row['DS_PERFIL']
        if row['FLAG_ANOMALIA'] == 1: perfil_txt += " [ALERTA ANOMALIA]"
        dados_insert.append((row['ID_BOLETO'], row['CLUSTER_ID'], perfil_txt))

    cursor.executemany(sql, dados_insert)
    conn.commit()
    print(f"   ✅ Sucesso! {len(dados_insert)} registros.")


def segmentar_clientes(force_retrain=False):
    print("\n🧩 INICIANDO MOTOR DE SEGMENTAÇÃO COMPORTAMENTAL...")
    conn = get_connection()
    if not conn: return

    try:
        df_clientes = pd.read_sql("SELECT * FROM V_BF_TREINO_ML_CLUSTER", conn)
        df_clientes = df_clientes.fillna(0)

        if df_clientes.empty:
            print("   ⚠️ Sem dados para clusterizar.")
            return

        cols_features = ['NR_FREQUENCIA_COMPRA', 'VL_TICKET_MEDIO', 'VL_MEDIO_DIAS_ATRASO']
        X = df_clientes[cols_features]

        kmeans = None
        scaler = None

        if os.path.exists(ARQUIVO_MODELO) and os.path.exists(ARQUIVO_SCALER) and not force_retrain:
            print("   📂 Modelo Cluster carregado.")
            kmeans = joblib.load(ARQUIVO_MODELO)
            scaler = joblib.load(ARQUIVO_SCALER)
            X_scaled = scaler.transform(X)
        else:
            print("   🎨 Definindo Perfis (Treino Novo)...")
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
            kmeans.fit(X_scaled)
            joblib.dump(kmeans, ARQUIVO_MODELO)
            joblib.dump(scaler, ARQUIVO_SCALER)

        df_clientes['CLUSTER_ID'] = kmeans.predict(X_scaled)
        resumo = df_clientes.groupby('CLUSTER_ID')[cols_features].mean()
        nomeacao = nomear_cluster(df_clientes, resumo)
        df_clientes['DS_PERFIL'] = df_clientes['CLUSTER_ID'].map(nomeacao)

        print("   🕵️‍♂️ Caçando Anomalias (DBSCAN)...")
        dbscan = DBSCAN(eps=1.0, min_samples=3).fit(X_scaled)
        df_clientes['FLAG_ANOMALIA'] = np.where(dbscan.labels_ == -1, 1, 0)

        print("   🔄 Mapeando volta para boletos...")
        df_boletos = pd.read_sql("SELECT ID_BOLETO, ID_PAGADOR FROM T_BF_BOLETO", conn)
        df_final = df_boletos.merge(df_clientes[['ID_PAGADOR', 'CLUSTER_ID', 'DS_PERFIL', 'FLAG_ANOMALIA']],
                                    on='ID_PAGADOR', how='inner')

        alimentar_tabela(df_final, conn)

    except Exception as e:
        print(f"❌ Erro Clustering: {e}")
    finally:
        conn.close()