import numpy as np
import os
import pandas as pd
from datetime import datetime
from src.utils_paths import resource_path
from src.db_connection import get_connection
from src.elt_random_dates import variar_datas_apenas

DATA_DIR = resource_path("data")

def carregar_empresas():
    print("📄 Processando Empresas...")
    arquivo = os.path.join(DATA_DIR, 'empresas.csv')

    if not os.path.exists(arquivo):
        print(f"      ⚠️ Arquivo não encontrado: {arquivo}")
        return

    df_empresas = pd.read_csv(arquivo)

    # Tratamento CNAE e UF
    df_empresas["cd_cnae_prin"] = df_empresas["cd_cnae_prin"].fillna(0).astype(float).astype(int).astype(str).str.zfill(
        7)
    df_empresas["uf"] = df_empresas["uf"].fillna("ND").str.strip().str.upper()
    df_empresas["score_quantidade_v2"] = df_empresas["score_quantidade_v2"].fillna(0.0)
    df_empresas["score_materialidade_v2"] = df_empresas["score_materialidade_v2"].fillna(0.0)

    df_empresas = df_empresas.where(pd.notnull(df_empresas), None)
    dados_empresas = df_empresas[
        ['id_cnpj', 'cd_cnae_prin', 'uf', 'score_materialidade_v2', 'score_quantidade_v2']].values.tolist()

    conn = get_connection()
    if not conn: return
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM T_BF_BOLETO")  # Limpa filhos primeiro
        cursor.execute("DELETE FROM T_BF_EMPRESA")

        sql = "INSERT INTO T_BF_EMPRESA (id_empresa, cd_cnae, sg_uf, vl_score_materialidade, vl_score_quantidade) VALUES (:1, :2, :3, :4, :5)"
        cursor.executemany(sql, dados_empresas)
        conn.commit()
        print(f"      ✅ {len(dados_empresas)} empresas inseridas.")
    except Exception as e:
        conn.rollback()
        print(f"      ❌ Erro ao inserir empresas: {e}")
    finally:
        conn.close()


def carregar_boletos():
    print("📄 Processando Boletos...")
    # Em produção, comente a linha abaixo para não alterar o histórico
    variar_datas_apenas(seed=42)

    arquivo = os.path.join(DATA_DIR, 'boletos_datas_variadas.csv')
    if not os.path.exists(arquivo): return

    df_boletos = pd.read_csv(arquivo)

    # Datas
    for col in ["dt_emissao", "dt_vencimento", "dt_pagamento"]:
        df_boletos[col] = pd.to_datetime(df_boletos[col], format='%Y-%m-%d', errors="coerce")

    df_boletos = df_boletos.dropna(subset=["dt_vencimento"])

    # Cálculo Atraso
    hoje = pd.Timestamp.now()
    df_boletos["dias_atraso"] = (df_boletos["dt_pagamento"] - df_boletos["dt_vencimento"]).dt.days
    # Se dt_pagamento for NaT, calcula com HOJE
    mask_nat = df_boletos["dt_pagamento"].isna()
    df_boletos.loc[mask_nat, "dias_atraso"] = (hoje - df_boletos.loc[mask_nat, "dt_vencimento"]).dt.days

    # Remove atrasos negativos (pagou adiantado = 0 atraso)
    df_boletos["dias_atraso"] = df_boletos["dias_atraso"].fillna(0).apply(lambda x: 0 if x < 0 else x).astype(int)

    df_boletos["alvo_inadimplencia"] = np.where(df_boletos["dias_atraso"] > 0, 1, 0)
    df_boletos["vlr_baixa"] = df_boletos["vlr_baixa"].fillna(0.0)
    df_boletos["tipo_baixa"] = df_boletos["tipo_baixa"].fillna("Não pago")

    df_boletos = df_boletos.where(pd.notnull(df_boletos), None)

    colunas = ['id_boleto', 'id_pagador', 'id_beneficiario', 'vlr_nominal', 'vlr_baixa', 'dt_emissao', 'dt_vencimento',
               'dt_pagamento', 'tipo_baixa', 'dias_atraso', 'alvo_inadimplencia']
    dados = df_boletos[colunas].values.tolist()

    conn = get_connection()
    if not conn: return
    try:
        cursor = conn.cursor()
        sql = "INSERT INTO T_BF_BOLETO (id_boleto, id_pagador, id_beneficiario, vl_nominal, vl_baixa, dt_emissao, dt_vencimento, dt_pagamento, tp_baixa, nr_dias_atraso, vl_inadimplencia) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)"
        cursor.executemany(sql, dados)
        conn.commit()
        print(f"      ✅ {len(dados)} boletos inseridos.")
    except Exception as e:
        print(f"      ❌ Erro boletos: {e}")
    finally:
        conn.close()


def carregar_dados():
    carregar_empresas()
    carregar_boletos()