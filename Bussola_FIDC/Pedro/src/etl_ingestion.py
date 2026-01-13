import numpy as np
import os
import pandas as pd
from datetime import datetime
from src.db_connection import get_connection
from src.elt_random_dates import variar_datas_apenas

# Define onde estão os CSVs (na raiz do projeto, pasta data)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
dfs_finais = []

# Para ajudar no tratamento dos dados
meses_pt = {
    'janeiro': '01', 'fevereiro': '02', 'março': '03', 'abril': '04', 'maio': '05', 'junho': '06',
    'julho': '07', 'agosto': '08', 'setembro': '09', 'outubro': '10', 'novembro': '11', 'dezembro': '12',
    'jan': '01', 'fev': '02', 'mar': '03', 'abr': '04', 'mai': '05', 'jun': '06',
    'jul': '07', 'ago': '08', 'set': '09', 'out': '10', 'nov': '11', 'dez': '12'
}

estados_uf = {
        'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM', 'Brasil': 'BR', 'Bahia': 'BA',
        'Ceará': 'CE', 'Distrito Federal': 'DF', 'Espírito Santo': 'ES', 'Goiás': 'GO',
        'Maranhão': 'MA', 'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS', 'Minas Gerais': 'MG',
        'Pará': 'PA', 'Paraíba': 'PB', 'Paraná': 'PR', 'Pernambuco': 'PE', 'Piauí': 'PI',
        'Rio de Janeiro': 'RJ', 'Rio Grande do Norte': 'RN', 'Rio Grande do Sul': 'RS',
        'Rondônia': 'RO', 'Roraima': 'RR', 'Santa Catarina': 'SC', 'São Paulo': 'SP',
        'Sergipe': 'SE', 'Tocantins': 'TO'
}


def carregar_empresas():
    print("📄 Processando Empresas...")
    arquivo = os.path.join(DATA_DIR, 'empresas.csv')

    if not os.path.exists(arquivo):
        print(f"      ⚠️ Arquivo não encontrado: {arquivo}")
        return {}

    # Lê o CSV
    df_empresas = pd.read_csv(arquivo)

    # --- CORREÇÃO DO CNAE ---
    df_empresas["cd_cnae_prin"] = df_empresas["cd_cnae_prin"].fillna(0)
    df_empresas["cd_cnae_prin"] = df_empresas["cd_cnae_prin"].astype(float).astype(int).astype(str)
    df_empresas["cd_cnae_prin"] = df_empresas["cd_cnae_prin"].str.zfill(7)

    # Restante das limpezas...
    df_empresas["uf"] = df_empresas["uf"].fillna("ND")
    df_empresas["uf"] = df_empresas["uf"].str.strip().str.upper()

    df_empresas["score_quantidade_v2"] = df_empresas["score_quantidade_v2"].fillna(0.0)
    df_empresas["score_materialidade_v2"] = df_empresas["score_materialidade_v2"].fillna(0.0)

    df_empresas = df_empresas.where(pd.notnull(df_empresas), None)

    # Separando colunas
    colunas_empresas_sql = ['id_cnpj', 'cd_cnae_prin', 'uf', 'score_materialidade_v2', 'score_quantidade_v2']
    dados_empresas = df_empresas[colunas_empresas_sql].values.tolist()

    conn = get_connection()
    if not conn: return {}
    cursor = conn.cursor()

    try:
        # Ordem importa: Primeiro boletos (filhos), depois empresas (pais)
        cursor.execute("DELETE FROM T_BF_BOLETO")
        cursor.execute("DELETE FROM T_BF_EMPRESA")
    except Exception as e:
        print(f"      ℹ️ Limpeza inicial: {e}")

    sql_insert_empresas = """
                          INSERT INTO T_BF_EMPRESA (id_empresa, cd_cnae, sg_uf, vl_score_materialidade, \
                                                    vl_score_quantidade)
                          VALUES (:1, :2, :3, :4, :5) \
                          """

    try:
        cursor.executemany(sql_insert_empresas, dados_empresas)
        conn.commit()
        print(f"      ✅ {len(dados_empresas)} empresas inseridas.")

    except Exception as e:
        print(f"      ❌ Erro ao inserir empresas: {e}")
        # Se der erro na empresa, não adianta seguir para boletos sem FK
        conn.rollback()
        conn.close()
        return {}

    conn.close()

def carregar_boletos():
    print("📄 Processando Boletos...")
    # Função para variar os dados para podermos testar o ML
    variar_datas_apenas(seed=42)
    arquivo = os.path.join(DATA_DIR, 'boletos_datas_variadas.csv')

    if not os.path.exists(arquivo):
        print(f"      ⚠️ Arquivo não encontrado: {arquivo}")
        return

    df_boletos = pd.read_csv(arquivo)

    # Converter para data
    colunas_data = ["dt_emissao", "dt_vencimento", "dt_pagamento"]
    for col in colunas_data:
        df_boletos[col] = pd.to_datetime(df_boletos[col], format='%Y-%m-%d', errors="coerce")

    df_boletos["vlr_baixa"] = df_boletos["vlr_baixa"].fillna(0.0)
    df_boletos["tipo_baixa"] = df_boletos["tipo_baixa"].fillna("Não pago")

    if df_boletos["dt_vencimento"].isna().any():
        df_boletos = df_boletos.dropna(subset=["dt_vencimento"])
    df_boletos["dias_atraso"] = (df_boletos["dt_pagamento"] - df_boletos["dt_vencimento"]).dt.days
    # pagamentos antes do vencimento -> 0
    df_boletos["dias_atraso"] = df_boletos["dias_atraso"].fillna(0)
    df_boletos["dias_atraso"] = df_boletos["dias_atraso"].apply(lambda x: 0 if x < 0 else x)
    # se não pagou, atraso = hoje - vencimento
    mask_np = df_boletos["dt_pagamento"].isna()
    df_boletos.loc[mask_np, "dias_atraso"] = (pd.Timestamp.now() - df_boletos.loc[mask_np, "dt_vencimento"]).dt.days
    # agora sim: inteiro seguro
    df_boletos["dias_atraso"] = df_boletos["dias_atraso"].fillna(0).astype(int)

    df_boletos["alvo_inadimplencia"] = np.where(df_boletos["dias_atraso"] > 0, 1, 0)

    conn = get_connection()
    if not conn: return
    cursor = conn.cursor()

    sql_insert_boletos = """
        INSERT INTO T_BF_BOLETO
        (id_boleto, id_pagador, id_beneficiario, vl_nominal, vl_baixa,
        dt_emissao, dt_vencimento, dt_pagamento, tp_baixa, nr_dias_atraso, vl_inadimplencia)
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)
    """

    df_boletos = df_boletos.where(pd.notnull(df_boletos), None)

    # Seleciona colunas (Atenção às datas que devem estar em string YYYY-MM-DD no CSV)
    colunas_boletos_sql = [
        'id_boleto', 'id_pagador', 'id_beneficiario', 'vlr_nominal', 'vlr_baixa',
        'dt_emissao', 'dt_vencimento', 'dt_pagamento', 'tipo_baixa', 'dias_atraso', 'alvo_inadimplencia'
    ]
    dados_insert_boletos = df_boletos[colunas_boletos_sql].values.tolist()

    try:
        cursor.executemany(sql_insert_boletos, dados_insert_boletos)
        conn.commit()
        print(f"      ✅ {len(dados_insert_boletos)} boletos inseridos e vinculados.")
    except Exception as e:
        print(f"      ❌ Erro ao inserir boletos: {e}")
    finally:
        conn.close()

def carregar_dados():
    carregar_empresas()
    carregar_boletos()