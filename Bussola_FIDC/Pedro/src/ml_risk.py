import joblib
import os
import pandas as pd
from src.db_connection import get_connection
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# Configuração de Caminhos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_DIR = os.path.join(BASE_DIR, 'models')
os.makedirs(MODEL_DIR, exist_ok=True)

ARQUIVO_MODELO = os.path.join(MODEL_DIR, 'modelo_risco.pkl')
ARQUIVO_SCALER = os.path.join(MODEL_DIR, 'scaler_risco.pkl')

# Faixa de risco (Experimental)
def classificar_risco(prob):
    if prob > 0.7: return 'ALTO'
    elif prob > 0.3: return 'MEDIO'
    else: return 'BAIXO'

# Criando uma coluna com o setor
def definir_setor(cnae):
    """Apenas para texto legível (o modelo usa o CD_SETOR_MACRO numérico)"""
    try:
        p = int(str(cnae)[:2])
        if 1 <= p <= 3: return 'AGRO'
        if 5 <= p <= 33 or 41 <= p <= 43: return 'INDUSTRIA'
        if 45 <= p <= 47: return 'VAREJO'
        if p >= 49: return 'SERVICOS'
        return 'OUTROS'
    except:
        return 'OUTROS'

# Criando a justificativa
def gerar_justificativa(row):
    """
    Cria uma frase explicativa para o Gerente de Crédito ler no Power BI.
    Usa a lógica do Feature Importance para dizer O PORQUÊ.
    """
    motivos = []

    # 1. Risco Comportamental (O mais forte)
    if row['VL_SCORE_QUANTIDADE'] < 100:
        motivos.append("Histórico de pagamentos insuficiente")

    # 2. Risco Setorial (Notícias - A nossa estrela ⭐)
    if row['VL_SENTIMENTO_SETORIAL'] < -0.15:
        motivos.append(f"Notícias negativas no setor ({row['DS_SETOR']})")

    # 3. Risco Agro (Safra)
    if row['DS_SETOR'] == 'AGRO' and row.get('VAR_AGRO_PRODUCAO', 0) < 0:
        motivos.append("Quebra de safra/produção agrícola")

    # 4. Risco Macro (Dólar/Juros)
    if row['TAX_SELIC'] > 12.0:
        motivos.append("Pressão de Juros Altos")

    # Se não tiver motivo específico, mas o risco for alto:
    if not motivos and row['PROBABILIDADE'] > 0.5:
        motivos.append("Análise estatística geral de risco")
    elif not motivos:
        motivos.append("Bons indicadores gerais")

    # Junta tudo numa frase
    texto_final = ". ".join(motivos) + "."
    return texto_final[:250]

def calcular_risco_credito(force_retrain=False):
    print("🧠 [ML Risco] Iniciando Cálculo...")
    conn = None
    try:
        conn = get_connection()
        if not conn:
            raise RuntimeError("❌ Sem conexão com o banco.")
        if conn:
            # Pega os dados da View de Treino que criamos
            query = "SELECT * FROM V_BF_TREINO_ML_RISCO"
            df_treino = pd.read_sql(query, conn)
        print(f"✅ Dados carregados! Total de linhas: {len(df_treino)}")
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return
    finally:
        if conn:
            conn.close()

    # Preenche nulos restantes com 0 (evitar que tenhamos valores nulos)
    df_treino = df_treino.fillna(0)

    df_treino['DS_SETOR'] = df_treino['CD_CNAE'].apply(definir_setor)

    # Separando as Features (X) do Target (y)
    features = ['VL_NOMINAL', 'NR_PRAZO_DIAS', 'VL_SCORE_MATERIALIDADE', 'VL_SCORE_QUANTIDADE',
                'TAX_SELIC', 'TAX_DOLAR', 'TAX_DESEMPREGO', 'INDICE_PIB', 'VAR_VAREJO', 'VAR_INDUSTRIA', 'VAR_SERVICOS',
                'VAR_AGRO', 'VAR_PECUARIA', 'VL_SENTIMENTO_SETORIAL', 'VL_SETOR_MACRO']
    cols_existentes = [c for c in features if c in df_treino.columns]
    X = df_treino[cols_existentes]

    # Persistência
    modelo = None
    scaler = None

    if os.path.exists(ARQUIVO_MODELO) and os.path.exists(ARQUIVO_SCALER) and not force_retrain:
        print("   📂 Modelo salvo encontrado. Carregando...")
        modelo = joblib.load(ARQUIVO_MODELO)
        scaler = joblib.load(ARQUIVO_SCALER)
        X_scaled = scaler.transform(X)

    elif force_retrain:
        print("   ⚙️ Treinando novo modelo de Risco...")
        # Pra conferir se existe a coluna de Target
        if 'TARGET' not in df_treino.columns:
            print("   ❌ Erro: Coluna TARGET não encontrada para treino.")
            return

        y = df_treino['TARGET']

        # Usando 30% como teste
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

        # Criando e treinando o modelo
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        modelo = RandomForestClassifier(
            n_estimators=200,
            max_depth=10,
            class_weight='balanced',
            random_state=42,
            n_jobs=-1
        )
        modelo.fit(X_train_scaled, y_train)

        # Avaliando o modelo (Acurácia)
        y_pred = modelo.predict(X_test_scaled)
        acuracia = accuracy_score(y_test, y_pred)
        print(f"Acurácia do Modelo: {acuracia:.2%}")

        if hasattr(modelo, 'feature_importances_'):
            importances = modelo.feature_importances_
            feature_names = X.columns

            # Cria um DataFrame para visualizar melhor
            df_imp = pd.DataFrame({'Feature': feature_names, 'Importancia': importances})
            df_imp = df_imp.sort_values(by='Importancia', ascending=False)

            print("\n   🏆 O que a IA mais valorizou (Feature Importance):")
            print(df_imp)

        # Treinando 100% dos dados para salvar (usando joblib)
        scaler_final = StandardScaler()
        X_scaled = scaler_final.fit_transform(X)
        modelo.fit(X_scaled, y)

        joblib.dump(modelo, ARQUIVO_MODELO)
        joblib.dump(scaler_final, ARQUIVO_SCALER)
        print("   💾 Modelo Final e Scaler salvos em 'models/'.")

    # Calcula a probabilidade de atraso (0 a 1) para todos
    probs = modelo.predict_proba(X_scaled)[:, 1] # Pega a chance de ser 1 (Inadimplente)
    df_treino['PROBABILIDADE'] = probs
    df_treino['PROBABILIDADE'] = round(df_treino['PROBABILIDADE'], 4)

    # Gerando a coluna da faixa de risco apresentada por cada boleto
    df_treino['FAIXA_RISCO'] = df_treino['PROBABILIDADE'].apply(classificar_risco)

    # Gerando uma coluna de justificativa
    df_treino["DS_PRINCIPAL"] = df_treino.apply(gerar_justificativa, axis = 1)

    # Fazendo um DataFrame final para alimentação do SQL
    df_final = df_treino[['ID_BOLETO', 'PROBABILIDADE', 'FAIXA_RISCO', 'DS_PRINCIPAL']].copy()
    df_final = df_final.drop_duplicates(subset=['ID_BOLETO'])

    # Converte para lista (Para o OracleDB)
    dados_insert = df_final.values.tolist()

    # Alimentando a tabela de predições
    try:
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            print("🔌Conectou ao Banco de Dados")
            cursor.execute("TRUNCATE TABLE T_BF_PREDICOES")

            sql_insert = """
                INSERT INTO T_BF_PREDICOES (id_boleto, vl_probabilidade_inadimplencia, st_faixa_risco, ds_principal)
                VALUES (:1, :2, :3, :4)
            """
            cursor.executemany(sql_insert, dados_insert)
            conn.commit()
            print(f"✅ SUCESSO! {cursor.rowcount} previsões salvas na tabela T_BF_PREDICOES.")

    except Exception as e:
        print(f"❌ Erro ao salvar previsões: {e}")

    finally:
        conn.close()