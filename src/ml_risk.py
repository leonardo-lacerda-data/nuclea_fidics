import joblib
import os
import pandas as pd
from src.utils_paths import resource_path
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from src.db_connection import get_connection

# Configuração de Caminhos
MODEL_DIR = resource_path("models")
os.makedirs(MODEL_DIR, exist_ok=True)

ARQUIVO_MODELO = os.path.join(MODEL_DIR, 'modelo_risco.pkl')
ARQUIVO_SCALER = os.path.join(MODEL_DIR, 'scaler_risco.pkl')


def classificar_risco(prob):
    if prob > 0.7:
        return 'ALTO'
    elif prob > 0.3:
        return 'MEDIO'
    else:
        return 'BAIXO'


def definir_setor(cnae):
    try:
        p = int(str(cnae)[:2])
        if 1 <= p <= 3: return 'AGRO'
        if 5 <= p <= 33 or 41 <= p <= 43: return 'INDUSTRIA'
        if 45 <= p <= 47: return 'VAREJO'
        if p >= 49: return 'SERVICOS'
        return 'OUTROS'
    except:
        return 'OUTROS'


def gerar_justificativa(row):
    motivos = []
    if row['VL_SCORE_QUANTIDADE'] < 100: motivos.append("Histórico de pagamentos insuficiente")
    if row['VL_SENTIMENTO_SETORIAL'] < -0.15: motivos.append(f"Notícias negativas no setor ({row['DS_SETOR']})")
    if row['DS_SETOR'] == 'AGRO' and row.get('VAR_AGRO_PRODUCAO', 0) < 0: motivos.append("Quebra de safra")
    if row['TAX_SELIC'] > 12.0: motivos.append("Pressão de Juros Altos")

    if not motivos and row['PROBABILIDADE'] > 0.5:
        motivos.append("Análise estatística geral de risco")
    elif not motivos:
        motivos.append("Bons indicadores gerais")

    return ". ".join(motivos)[:250] + "."


def calcular_risco_credito(force_retrain=False):
    print("🧠 [ML Risco] Iniciando Cálculo...")
    conn = get_connection()
    if not conn: return

    try:
        df_treino = pd.read_sql("SELECT * FROM V_BF_TREINO_ML_RISCO", conn)
        print(f"✅ Dados carregados! Total de linhas: {len(df_treino)}")
    except Exception as e:
        print(f"❌ Erro ao ler dados: {e}")
        conn.close()
        return
    finally:
        conn.close()  # Fecha aqui para liberar a conexão enquanto treina a IA

    if df_treino.empty:
        print("⚠️ Tabela de treino vazia. Execute o ETL primeiro.")
        return

    df_treino = df_treino.fillna(0)
    df_treino['DS_SETOR'] = df_treino['CD_CNAE'].apply(definir_setor)

    features = ['VL_NOMINAL', 'NR_PRAZO_DIAS', 'VL_SCORE_MATERIALIDADE', 'VL_SCORE_QUANTIDADE',
                'TAX_SELIC', 'TAX_DOLAR', 'TAX_DESEMPREGO', 'INDICE_PIB', 'VAR_VAREJO', 'VAR_INDUSTRIA',
                'VAR_SERVICOS', 'VAR_AGRO', 'VAR_PECUARIA', 'VL_SENTIMENTO_SETORIAL', 'VL_SETOR_MACRO']

    # Garante que só usa colunas que existem
    cols_existentes = [c for c in features if c in df_treino.columns]
    X = df_treino[cols_existentes]

    modelo = None
    scaler = None

    # Lógica de Carregar ou Treinar
    if os.path.exists(ARQUIVO_MODELO) and os.path.exists(ARQUIVO_SCALER) and not force_retrain:
        print("   📂 Modelo salvo encontrado. Carregando...")
        modelo = joblib.load(ARQUIVO_MODELO)
        scaler = joblib.load(ARQUIVO_SCALER)
        X_scaled = scaler.transform(X)
    else:
        print("   ⚙️ Treinando novo modelo de Risco...")
        if 'TARGET' not in df_treino.columns:
            print("   ❌ Erro: Coluna TARGET ausente.")
            return

        y = df_treino['TARGET']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        modelo = RandomForestClassifier(n_estimators=200, max_depth=10, class_weight='balanced', random_state=42,
                                        n_jobs=-1)
        modelo.fit(X_train_scaled, y_train)

        y_pred = modelo.predict(X_test_scaled)
        print(f"   📊 Acurácia do Modelo: {accuracy_score(y_test, y_pred):.2%}")

        # Treino final completo
        scaler_final = StandardScaler()
        X_scaled = scaler_final.fit_transform(X)
        modelo.fit(X_scaled, y)

        joblib.dump(modelo, ARQUIVO_MODELO)
        joblib.dump(scaler_final, ARQUIVO_SCALER)
        print("   💾 Modelo salvo.")

    # Predições
    probs = modelo.predict_proba(X_scaled)[:, 1]
    df_treino['PROBABILIDADE'] = probs.round(4)
    df_treino['FAIXA_RISCO'] = df_treino['PROBABILIDADE'].apply(classificar_risco)
    df_treino["DS_PRINCIPAL"] = df_treino.apply(gerar_justificativa, axis=1)

    df_final = df_treino[['ID_BOLETO', 'PROBABILIDADE', 'FAIXA_RISCO', 'DS_PRINCIPAL']].drop_duplicates(
        subset=['ID_BOLETO'])
    dados_insert = df_final.values.tolist()

    # Salvar no Oracle
    conn = get_connection()
    if not conn: return
    try:
        cursor = conn.cursor()
        print("🔌 Conectou ao Banco de Dados para Salvar...")
        cursor.execute("TRUNCATE TABLE T_BF_PREDICOES")
        cursor.executemany(
            "INSERT INTO T_BF_PREDICOES (id_boleto, vl_probabilidade_inadimplencia, st_faixa_risco, ds_principal) VALUES (:1, :2, :3, :4)",
            dados_insert)
        conn.commit()
        print(f"✅ SUCESSO! {cursor.rowcount} previsões salvas.")
    except Exception as e:
        print(f"❌ Erro ao salvar: {e}")
    finally:
        conn.close()