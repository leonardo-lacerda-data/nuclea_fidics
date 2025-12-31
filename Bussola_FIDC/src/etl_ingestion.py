import numpy as np
import pandas as pd
import os
from datetime import datetime
from src.database import get_connection

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
    print("🏢 Processando Empresas...")
    # Renomeei o arquivo base_auxiliar_fiap.csv para empresas.csv antes de utilizar essa função
    arquivo = os.path.join(DATA_DIR, 'empresas.csv')

    if not os.path.exists(arquivo):
        print(f"      ⚠️ Arquivo não encontrado: {arquivo}")
        return {}

    # Lê o CSV
    df_empresas = pd.read_csv(arquivo)

    # Tratando os dados
    df_empresas["cd_cnae_prin"] = df_empresas["cd_cnae_prin"].fillna(0000000)
    df_empresas["cd_cnae_prin"] = df_empresas["cd_cnae_prin"].astype(int).astype(str)

    df_empresas["uf"] = df_empresas["uf"].fillna("ND")
    df_empresas["uf"] = df_empresas["uf"].str.strip().str.upper()

    df_empresas["score_quantidade_v2"] = df_empresas["score_quantidade_v2"].fillna(0.0)
    df_empresas["score_materialidade_v2"] = df_empresas["score_materialidade_v2"].fillna(0.0)

    df_empresas = df_empresas.where(pd.notnull(df_empresas), None)

    # Separando quais colunas do CSV que iremos inserir no nosso Banco de Dados
    colunas_empresas_sql = ['id_cnpj', 'cd_cnae_prin', 'uf', 'score_materialidade_v2','score_quantidade_v2']
    dados_empresas = df_empresas[colunas_empresas_sql].values.tolist()

    conn = get_connection()
    if not conn: return {}
    cursor = conn.cursor()

    # Limpa tabela para evitar duplicidade (Cuidado em produção!)
    try:
        # Apagamos boletos antes pois eles dependem das empresas (FK)
        cursor.execute("DELETE FROM T_BF_BOLETO")
        cursor.execute("DELETE FROM T_BF_EMPRESA")

    except Exception as e:
        print(f"      ℹ️ Tabela limpa ou vazia: {e}")

    # SQL de Insert
    sql_insert_empresas = """
        INSERT INTO T_BF_EMPRESA (id_empresa, cd_cnae, sg_uf, vl_score_materialidade, vl_score_quantidade)
        VALUES (:1, :2, :3, :4, :5)
    """

    try:
        cursor.executemany(sql_insert_empresas, dados_empresas)
        conn.commit()
        print(f"      ✅ {len(dados_empresas)} empresas inseridas.")

    except Exception as e:
        print(f"      ❌ Erro ao inserir empresas: {e}")
        conn.close()
        return {}

def carregar_boletos():
    print("📄 Processando Boletos...")
    arquivo = os.path.join(DATA_DIR, 'boletos.csv')

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


    df_boletos["dias_atraso"] = (df_boletos["dt_pagamento"] - df_boletos["dt_vencimento"]).dt.days
    df_boletos["dias_atraso"] = df_boletos["dias_atraso"].apply(lambda x: 0 if x < 0 else x)
    df_boletos.loc[df_boletos["dt_pagamento"].isnull(), "dias_atraso"] = ( datetime.now() - df_boletos["dt_vencimento"]).dt.days
    df_boletos["dias_atraso"] = df_boletos["dias_atraso"].astype(int)

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

def tratar_data_ibge(texto_data):
    # Ex: "janeiro 2024" -> 01/01/2024
    # Ex: "nov-dez-jan 2024" -> 01/01/2024 (Pega o último mês do trimestre)
    try:
        partes = texto_data.split(' ')
        ano = partes[-1]
        mes_texto = partes[0].lower()

        # Se for trimestre móvel (ex: "nov-dez-jan"), pega o último
        if '-' in mes_texto:
            mes_texto = mes_texto.split('-')[-1]

        mes_num = meses_pt.get(mes_texto, '01')
        return pd.to_datetime(f"{ano}-{mes_num}-01")
    except:
        return None

def carregar_ibge():
    arquivos_ibge = [
        {'arquivo': os.path.join(DATA_DIR, 'PMC.csv'), 'indicador': 'PMC', 'skip': 3},
        {'arquivo': os.path.join(DATA_DIR, 'PMS.csv'), 'indicador': 'PMS', 'skip': 3},
        {'arquivo': os.path.join(DATA_DIR, 'PIM.csv'), 'indicador': 'PIM', 'skip': 3},
        {'arquivo': os.path.join(DATA_DIR, 'IPCA.csv'), 'indicador': 'IPCA', 'skip': 3},
        {'arquivo': os.path.join(DATA_DIR, 'Desemprego.csv'), 'indicador': 'DESEMPREGO', 'skip': 3}
    ]

    for item in arquivos_ibge:
        print(f"📄 Processando {item['indicador']}...")

        try:
            df_ibge = pd.read_csv(item['arquivo'], skiprows=item['skip'], encoding='utf-8', sep=',')
            col_uf = df_ibge.columns[0]

            # Melt (Transforma colunas de datas em linhas)
            df_melt = df_ibge.melt(id_vars=[col_uf], var_name='data_texto', value_name='vl_indicador')

            # Tratamento
            df_melt['nm_indicador'] = item['indicador']
            df_melt['dt_referencia'] = df_melt['data_texto'].apply(tratar_data_ibge)

            # Padroniza UF (Brasil -> BR)
            df_melt['sg_uf'] = df_melt[col_uf]
            df_melt['sg_uf'] = df_melt['sg_uf'].map(estados_uf)

            # Remove linhas com datas inválidas ou valores nulos/texto
            df_melt = df_melt.dropna(subset=['dt_referencia'])
            df_melt['vl_indicador'] = pd.to_numeric(df_melt['vl_indicador'], errors='coerce')
            df_melt = df_melt.dropna(subset=['vl_indicador'])

            dfs_finais.append(df_melt[['sg_uf', 'dt_referencia', 'nm_indicador', 'vl_indicador']])

        except Exception as e:
            print(f"❌ Erro ao ler {item['arquivo']}: {e}")

def carregar_selic():
    print(f"📄 Processando SELIC...")
    arquivo = os.path.join(DATA_DIR, 'SELIC.csv')

    if not os.path.exists(arquivo):
        print(f"      ⚠️ Arquivo não encontrado: {arquivo}")
        return

    df_selic = pd.read_csv(arquivo, sep=';', decimal=',')
    df_selic.rename(columns={df_selic.columns[0]: 'dt_referencia', df_selic.columns[1]: 'vl_indicador'}, inplace=True)
    df_selic.drop(df_selic.index[-1], inplace=True)
    df_selic['dt_referencia'] = pd.to_datetime(df_selic['dt_referencia'], format='%d/%m/%Y', errors='coerce')
    df_selic['vl_indicador'] = df_selic['vl_indicador'].str.replace(',', '.')
    df_selic['vl_indicador'] = df_selic['vl_indicador'].astype('float')
    df_selic_mensal = df_selic.set_index('dt_referencia').resample('MS')['vl_indicador'].mean().reset_index()
    df_selic_mensal['nm_indicador'] = 'SELIC'
    df_selic_mensal['sg_uf'] = 'BR'
    dfs_finais.append(df_selic_mensal[['sg_uf', 'dt_referencia', 'nm_indicador', 'vl_indicador']])

def carregar_dolar():
    print(f"📄 Processando PTAX...")
    arquivo = os.path.join(DATA_DIR, 'PTAX.csv')

    if not os.path.exists(arquivo):
        print(f"      ⚠️ Arquivo não encontrado: {arquivo}")
        return

    df_ptax = pd.read_csv(arquivo, sep=';')
    df_ptax.columns = ['dt_referencia', 'vl_indicador']
    df_ptax['nm_indicador'] = 'DOLAR'
    df_ptax['sg_uf'] = 'BR'
    df_ptax.drop(df_ptax.index[-1], inplace=True)
    df_ptax['dt_referencia'] = pd.to_datetime(df_ptax['dt_referencia'], format='%m/%Y')
    dfs_finais.append(df_ptax[['sg_uf', 'dt_referencia', 'nm_indicador', 'vl_indicador']])

def carregar_ibc_br():
    print(f"📄 Processando IBC_Br...")
    arquivo = os.path.join(DATA_DIR, 'IBC_Br.csv')

    if not os.path.exists(arquivo):
        print(f"      ⚠️ Arquivo não encontrado: {arquivo}")
        return

    df_ibc = pd.read_csv(arquivo, sep=';')
    df_ibc.columns = ['dt_referencia', 'vl_indicador']
    df_ibc['nm_indicador'] = 'PIB'  # Ou 'IBCR', conforme sua view
    df_ibc['sg_uf'] = 'BR'
    df_ibc.drop(df_ibc.index[-1], inplace=True)
    df_ibc['dt_referencia'] = pd.to_datetime(df_ibc['dt_referencia'], format='%m/%Y')
    dfs_finais.append(df_ibc[['sg_uf', 'dt_referencia', 'nm_indicador', 'vl_indicador']])

def carregar_macro():
    carregar_ibge()
    carregar_selic()
    carregar_dolar()
    carregar_ibc_br()

    conn = get_connection()
    if not conn: return
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE T_BF_MACRO_ECONOMIA")

    sql_insert_macro = """
        INSERT INTO T_BF_MACRO_ECONOMIA (sg_uf, dt_referencia, nm_indicador, vl_indicador)
        VALUES (:1, :2, :3, :4)
    """

    total_inserido = 0

    try:
        for i, df in enumerate(dfs_finais):
            dados = df.values.tolist()
            cursor.executemany(sql_insert_macro, dados)
            total_inserido += len(dados)
            print(f"   -> Lote {i + 1}: {len(dados)} linhas inseridas.")

        conn.commit()
        print(f"      ✅ {total_inserido} indicadores carregados.")
    except Exception as e:
        print(f"      ❌ Erro ao inserir dados macro: {e}")
    finally:
        conn.close()

def carregar_dados():
    carregar_empresas()
    carregar_boletos()

    carregar_macro()
