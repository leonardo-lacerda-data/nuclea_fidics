import pandas as pd
from src.database import get_connection
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# Criando uma coluna com o setor
def definir_setor(cnae):
  try:
    prefixo = int(str(cnae)[:2])
    if 5 <= prefixo <= 33:
      return 'INDUSTRIA'
    elif 41 <= prefixo <= 43:
      return 'INDUSTRIA (CONSTR)'
    elif 45 <= prefixo <= 47:
      return 'COMERCIO'
    elif prefixo >= 49:
      return 'SERVICOS'
    else:
      return 'OUTROS'
  except:
    return 'DESCONHECIDO'

# Faixa de risco (Experimental)
def classificar_risco(prob):
    if prob > 0.7: return 'ALTO'
    if prob > 0.3: return 'MEDIO'
    return 'BAIXO'

# Criando a justificativa
def gerar_justificativa(record):
  texto = f"Setor: {record['DS_SETOR']}. "
  if record['PROBABILIDADE'] > 0.7:
    texto += "Alerta: Probabilidade alta de default. "
    if record['TAX_DESEMPREGO'] > 8:
      texto += "Desemprego alto pode impactar consumo. "
    if record['TAX_DOLAR'] > 5.20:
      texto += "Dólar pressionado. "
  else:
    texto = "Empresa com bons indicadores. "
  return texto[:100]

def calcular_risco_credito():
    print("Conectando ao Oracle")
    try:
        conn = get_connection()
        if conn:
            # Pega os dados da View de Treino que criamos
            query = "SELECT * FROM V_BF_TREINO_ML"
            df_treino = pd.read_sql(query, conn)
        print(f"✅ Dados carregados! Total de linhas: {len(df_treino)}")

    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
    finally:
        conn.close()

    # Preenche nulos restantes com 0 (evitar que tenhamos valores nulos)
    df_treino = df_treino.fillna(0)

    df_treino['DS_SETOR'] = df_treino['CD_CNAE'].apply(definir_setor)

    # Separando as Features (X) do Target (y)
    features = ['VL_NOMINAL', 'NR_PRAZO_DIAS', 'VL_SCORE_MATERIALIDADE', 'VL_SCORE_QUANTIDADE',
                'TAX_SELIC', 'TAX_DOLAR', 'TAX_DESEMPREGO', 'INDICE_PIB', 'VAR_VAREJO', 'VAR_INDUSTRIA', 'VAR_SERVICOS',
                'VL_SENTIMENTO_SETORIAL']
    X = df_treino[features]
    y = df_treino['TARGET']

    # TREINAMENTO
    print("Treinando modelo de Regressão Logística")

    # Usando 30% como teste
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # Criando e treinando o modelo
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.fit_transform(X_test)

    modelo = LogisticRegression()
    modelo.fit(X_train_scaled, y_train)

    # Avaliando o modelo (Acurácia)
    y_pred = modelo.predict(X_test_scaled)
    acuracia = accuracy_score(y_test, y_pred)
    print(f"Acurácia do Modelo: {acuracia:.2%}")

    coeficientes = pd.DataFrame({
        'Feature': features,
        'Peso': modelo.coef_[0]
    }).sort_values(by='Peso', ascending=False)

    print("\n⚖️ Variáveis que mais aumentam o Risco (Peso Positivo) ou Diminuem (Negativo):")
    print(coeficientes.head(15))

    # Calcula a probabilidade de atraso (0 a 1) para todos
    X_full_scaled = scaler.transform(X)
    probs = modelo.predict_proba(X_full_scaled)[:, 1] # Pega a chance de ser 1 (Inadimplente)
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