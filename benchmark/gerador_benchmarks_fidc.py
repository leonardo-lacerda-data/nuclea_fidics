import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import warnings

# Silencia o aviso dramático do Pandas exigindo SQLAlchemy para o OracleDB
warnings.filterwarnings(
    action='ignore',
    category=UserWarning,
    message='.*SQLAlchemy.*'
)

# Importa a sua conexão real com o banco
from src.db_connection import get_connection

plt.style.use('dark_background')


def gerar_benchmark_risco():
    print("🧠 [Benchmark] Conectando ao Oracle para buscar V_BF_TREINO_ML_RISCO...")
    conn = get_connection()
    if not conn:
        print("❌ Erro de conexão com o banco.")
        return

    try:
        df_treino = pd.read_sql("SELECT * FROM V_BF_TREINO_ML_RISCO", conn)
        df_treino = df_treino.fillna(0)
    finally:
        conn.close()

    if df_treino.empty:
        print("⚠️ View de risco vazia.")
        return

    features = ['VL_NOMINAL', 'NR_PRAZO_DIAS', 'VL_SCORE_MATERIALIDADE', 'VL_SCORE_QUANTIDADE',
                'TAX_SELIC', 'TAX_DOLAR', 'TAX_DESEMPREGO', 'INDICE_PIB', 'VAR_VAREJO', 'VAR_INDUSTRIA',
                'VAR_SERVICOS', 'VAR_AGRO', 'VAR_PECUARIA', 'VL_SENTIMENTO_SETORIAL', 'VL_SETOR_MACRO']

    cols_existentes = [c for c in features if c in df_treino.columns]
    X = df_treino[cols_existentes]
    y = df_treino['TARGET']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # O mesmo modelo do seu ml_risk.py
    modelo = RandomForestClassifier(n_estimators=200, max_depth=10, class_weight='balanced', random_state=42, n_jobs=-1)
    modelo.fit(X_train_scaled, y_train)
    y_pred = modelo.predict(X_test_scaled)

    # 1. Classification Report
    nomes_classes = ['0 (Em Dia)', '1 (Inadimplente)']
    print("\n" + "-" * 50)
    print("📊 CLASSIFICATION REPORT OFICIAL - RANDOM FOREST")
    print("-" * 50)
    print(classification_report(y_test, y_pred, target_names=nomes_classes))

    # 2. Matriz de Confusão
    plt.figure(figsize=(8, 6))
    cm = confusion_matrix(y_test, y_pred)
    sns.heatmap(cm, annot=True, fmt='d', cmap='Greens', xticklabels=nomes_classes, yticklabels=nomes_classes)
    plt.title('Matriz de Confusão - Predição Binária (Inadimplência)', color='white', pad=20)
    plt.ylabel('Risco Verdadeiro (Real)', color='white')
    plt.xlabel('Risco Previsto (Modelo)', color='white')
    plt.tight_layout()
    plt.savefig('benchmark_rf_oficial.png', dpi=300)
    print("✅ Matriz de Confusão salva como 'benchmark_rf_oficial.png'")


def gerar_benchmark_cluster():
    print("\n🧬 [Benchmark] Conectando ao Oracle para buscar V_BF_TREINO_ML_CLUSTER...")
    conn = get_connection()
    if not conn:
        print("❌ Erro de conexão com o banco.")
        return

    try:
        df_clientes = pd.read_sql("SELECT * FROM V_BF_TREINO_ML_CLUSTER", conn)
        df_clientes = df_clientes.fillna(0)
    finally:
        conn.close()

    cols_features = ['NR_FREQUENCIA_COMPRA', 'VL_TICKET_MEDIO', 'VL_MEDIO_DIAS_ATRASO']
    X = df_clientes[cols_features]

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    labels = kmeans.fit_predict(X_scaled)

    # Redução para 2D com PCA
    pca = PCA(n_components=2)
    componentes = pca.fit_transform(X_scaled)
    df_pca = pd.DataFrame(data=componentes, columns=['C1', 'C2'])

    # Mapeamento dinâmico igual ao seu código para nomear corretamente os clusters
    resumo = df_clientes.copy()
    resumo['CLUSTER_ID'] = labels
    medias = resumo.groupby('CLUSTER_ID')[cols_features].mean()

    limite_vip = df_clientes['VL_TICKET_MEDIO'].quantile(0.9375)
    limite_atraso_critico = df_clientes['VL_MEDIO_DIAS_ATRASO'].quantile(0.95)
    limite_fiel = df_clientes['NR_FREQUENCIA_COMPRA'].quantile(0.80)

    apelidos = {}
    for cid, row in medias.iterrows():
        if row['VL_TICKET_MEDIO'] > limite_vip:
            apelidos[cid] = 'VIP (Ticket Alto)'
        elif row['VL_MEDIO_DIAS_ATRASO'] > limite_atraso_critico:
            apelidos[cid] = 'Devedor/Atrasado'
        elif row['NR_FREQUENCIA_COMPRA'] > limite_fiel:
            apelidos[cid] = 'Recorrente/Fiel'
        else:
            apelidos[cid] = 'Padrão/Esporádico'

    df_pca['Perfil'] = [apelidos[l] for l in labels]

    plt.figure(figsize=(10, 6))
    cores = {'VIP (Ticket Alto)': '#deff9a', 'Recorrente/Fiel': '#a3cf62', 'Padrão/Esporádico': '#888888',
             'Devedor/Atrasado': '#ff4d4d'}

    sns.scatterplot(x='C1', y='C2', hue='Perfil', palette=cores, data=df_pca, s=100, alpha=0.8)
    plt.title('Dispersão K-Means Oficial (PCA em 2D)', color='white')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig('benchmark_kmeans_oficial.png', dpi=300)
    print("✅ Gráfico de Dispersão salvo como 'benchmark_kmeans_oficial.png'")


if __name__ == "__main__":
    gerar_benchmark_risco()
    gerar_benchmark_cluster()
