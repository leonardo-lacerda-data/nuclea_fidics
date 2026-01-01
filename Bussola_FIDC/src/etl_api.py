import requests
import pandas as pd
from datetime import datetime
from src.db_connection import get_connection

# Filtrando pelos dados dos últimos dois anos
data_inicial = (pd.to_datetime(datetime.now()) - pd.DateOffset(years=2)).strftime('%d/%m/%Y')
data_final = datetime.now().strftime('%d/%m/%Y')

# Criando lista vazia para armazenar os dados antes de alimentar as tabelas SQL
dfs_finais = []

# Parâmetros úteis para padronizarmos os nossos dados
meses_pt = {
    'janeiro': '01', 'fevereiro': '02', 'março': '03', 'abril': '04', 'maio': '05', 'junho': '06',
    'julho': '07', 'agosto': '08', 'setembro': '09', 'outubro': '10', 'novembro': '11', 'dezembro': '12',
    'jan': '01', 'fev': '02', 'mar': '03', 'abr': '04', 'mai': '05', 'jun': '06',
    'jul': '07', 'ago': '08', 'set': '09', 'out': '10', 'nov': '11', 'dez': '12'
}

mapa_uf = {
        'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM', 'Brasil': 'BR', 'Bahia': 'BA',
        'Ceará': 'CE', 'Distrito Federal': 'DF', 'Espírito Santo': 'ES', 'Goiás': 'GO',
        'Maranhão': 'MA', 'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS', 'Minas Gerais': 'MG',
        'Pará': 'PA', 'Paraíba': 'PB', 'Paraná': 'PR', 'Pernambuco': 'PE', 'Piauí': 'PI',
        'Rio de Janeiro': 'RJ', 'Rio Grande do Norte': 'RN', 'Rio Grande do Sul': 'RS',
        'Rondônia': 'RO', 'Roraima': 'RR', 'Santa Catarina': 'SC', 'São Paulo': 'SP',
        'Sergipe': 'SE', 'Tocantins': 'TO'
}


def get_selic():
    """
    Busca a Taxa Selic Diária (Série 11) diretamente do Banco Central.
    Correção: Adicionado filtro de dataInicial para respeitar o limite de 10 anos da API.
    """
    print("   🌐 [API] Consultando Selic no Banco Central...")

    # URL Oficial com filtro de Data (Obrigatório para séries diárias)
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"

    try:
        # Fazendo a requisição
        response = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()

        # Transferindo dados para o DataFrame
        dados = response.json()
        df_selic = pd.DataFrame(dados)

        # Limpeza
        df_selic['data'] = pd.to_datetime(df_selic['data'], format='%d/%m/%Y', errors='coerce')
        df_selic['valor'] = pd.to_numeric(df_selic['valor'])
        df_selic_mensal = df_selic.set_index('data').resample('MS')['valor'].mean().reset_index()

        # Padronização
        df_selic_final = pd.DataFrame()
        df_selic_final['DT_REFERENCIA'] = df_selic_mensal['data']
        df_selic_final['VL_INDICADOR'] = df_selic_mensal['valor']
        df_selic_final['NM_INDICADOR'] = 'SELIC'
        df_selic_final['SG_UF'] = 'BR'

        return df_selic_final[['SG_UF', 'DT_REFERENCIA', 'NM_INDICADOR', 'VL_INDICADOR']]

    except Exception as e:
        print(f"   ❌ Erro ao buscar Selic: {e}")
        # Retorna DataFrame vazio se der erro, para não quebrar o sistema
        return pd.DataFrame()


def get_dolar():
    """
    Busca a Cotação do Dólar (Série 3698 - Média Mensal) do Banco Central.
    Substitui o arquivo: PTAX.csv
    """
    print("   🌐 [API] Consultando Dólar (PTAX) no Banco Central...")
    # Série 3698 = Dólar (Venda) - Média Mensal
    # URL com filtro de data (usamos o mesmo padrão da Selic)
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.3698/dados?formato=json&dataInicial={data_inicial}"

    try:
        # Fazendo a requisição
        response = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()

        # Transferindo dados para o DataFrame
        dados = response.json()
        df_ptax = pd.DataFrame(dados)

        # Limpeza
        df_ptax['data'] = pd.to_datetime(df_ptax['data'], format='%d/%m/%Y')
        df_ptax['valor'] = pd.to_numeric(df_ptax['valor'])

        # Padronização
        df_ptax_final = pd.DataFrame()
        df_ptax_final['DT_REFERENCIA'] = df_ptax['data']
        df_ptax_final['VL_INDICADOR'] = df_ptax['valor']
        df_ptax_final['NM_INDICADOR'] = 'DOLAR'
        df_ptax_final['SG_UF'] = 'BR'

        return df_ptax_final[['SG_UF', 'DT_REFERENCIA', 'NM_INDICADOR', 'VL_INDICADOR']]

    except Exception as e:
        print(f"   ❌ Erro ao buscar Dólar: {e}")
        return pd.DataFrame()


def get_ipca():
    """
    Busca o IPCA Mensal (Série 433) do Banco Central.
    Substitui o arquivo: IPCA.csv
    """
    print("   🌐 [API] Consultando IPCA (Inflação) no Banco Central...")

    # Série 433 = IPCA Mensal (%)
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json&dataInicial={data_inicial}"

    try:
        response = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()

        df_ipca = pd.DataFrame(response.json())
        df_ipca['data'] = pd.to_datetime(df_ipca['data'], format='%d/%m/%Y')
        df_ipca['valor'] = pd.to_numeric(df_ipca['valor'])

        df_ipca_final = pd.DataFrame()
        df_ipca_final['DT_REFERENCIA'] = df_ipca['data']
        df_ipca_final['VL_INDICADOR'] = df_ipca['valor']
        df_ipca_final['NM_INDICADOR'] = 'IPCA'
        df_ipca_final['SG_UF'] = 'BR'

        # Filtro
        return df_ipca_final[['SG_UF', 'DT_REFERENCIA', 'NM_INDICADOR', 'VL_INDICADOR']]

    except Exception as e:
        print(f"   ❌ Erro ao buscar IPCA: {e}")
        return pd.DataFrame()


def get_ibcbr():
    """
    Busca o IBC-Br (Série 24363) do Banco Central.
    É a 'Prévia do PIB' mensal. Substitui: IBC_Br.csv
    """
    print("   🌐 [API] Consultando IBC-Br (Atividade Econômica)...")

    # Série 24363 = IBC-Br com ajuste sazonal
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.24363/dados?formato=json&dataInicial={data_inicial}"

    try:
        response = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()

        df_ibcbr = pd.DataFrame(response.json())
        df_ibcbr['data'] = pd.to_datetime(df_ibcbr['data'], format='%d/%m/%Y')
        df_ibcbr['valor'] = pd.to_numeric(df_ibcbr['valor'])

        df_ibcbr_final = pd.DataFrame()
        df_ibcbr_final['DT_REFERENCIA'] = df_ibcbr['data']
        df_ibcbr_final['VL_INDICADOR'] = df_ibcbr['valor']
        df_ibcbr_final['NM_INDICADOR'] = 'IBC-BR'
        df_ibcbr_final['SG_UF'] = 'BR'

        # Filtro
        return df_ibcbr_final[['SG_UF', 'DT_REFERENCIA', 'NM_INDICADOR', 'VL_INDICADOR']]

    except Exception as e:
        print(f"   ❌ Erro ao buscar IBC-Br: {e}")
        return pd.DataFrame()


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


def get_dados_sidra(tabela, variavel, classificacao='', nome_indicador='INDICADOR'):
    """
    Busca dados na API SIDRA do IBGE.
    Parâmetros:
      - tabela: Código da tabela (ex: 8888 para Indústria)
      - variavel: Código da variável (ex: 12606 para Número-índice)
      - classificacao: Filtros extras (ex: '/c544/129314' para Indústria Geral)
      - nome_indicador: Nome para salvar no banco (ex: 'IND_INDUSTRIA')
    """
    print(f"   🌐 [API IBGE] Consultando {nome_indicador} (Tabela {tabela})...")

    # URL da API SIDRA

    url = f"https://apisidra.ibge.gov.br/values/t/{tabela}/p/last%2034/v/{variavel}{classificacao}?formato=json"

    try:
        response = requests.get(url, timeout=20)  # IBGE às vezes é lento
        data = response.json()

        # Se vier vazio ou com erro
        if not isinstance(data, list) or len(data) <= 1:
            print(f"   ⚠️ Aviso: Retorno vazio ou erro para {nome_indicador}")
            return pd.DataFrame()

        # O SIDRA manda o cabeçalho na primeira linha, então pulamos ela (data[1:])
        df_ibge = pd.DataFrame(data[1:])

        df_final = pd.DataFrame()

        # Valor
        df_final['VL_INDICADOR'] = pd.to_numeric(df_ibge['V'], errors='coerce')

        # Tratamento da Data (Vem como "202401" na coluna D2C)
        # Às vezes muda a coluna (D2C, D3C...), vamos garantir
        df_data = pd.DataFrame()
        df_data['DT_REFERENCIA'] = df_ibge['D1N']
        df_final['DT_REFERENCIA'] = df_data['DT_REFERENCIA'].apply(tratar_data_ibge)

        # Nome
        df_final['NM_INDICADOR'] = nome_indicador

        # Estado (UF)
        # O IBGE manda o nome ("Acre", "São Paulo"). Precisamos converter para Sigla.
        col_local = 'D4N' if 'D4N' in df_ibge.columns else 'D3N'
        df_final['SG_UF'] = df_ibge[col_local].map(mapa_uf).fillna('ND')

        return df_final[['SG_UF', 'DT_REFERENCIA', 'NM_INDICADOR', 'VL_INDICADOR']]

    except Exception as e:
        print(f"   ❌ Erro na API SIDRA ({nome_indicador}): {e}")
        return pd.DataFrame()

def alimentar_tabela_macro():

    conn = get_connection()
    if not conn: return
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE T_BF_MACRO_ECONOMIA")

    sql_insert_macro = """
                       INSERT INTO T_BF_MACRO_ECONOMIA (sg_uf, dt_referencia, nm_indicador, vl_indicador)
                       VALUES (:1, :2, :3, :4) \
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

def carregar_api():
    print("\n--- UTILIZANDO API DADOS EXTERNOS ---")

    dfs_finais.append(get_selic())
    dfs_finais.append(get_dolar())
    dfs_finais.append(get_ipca())
    dfs_finais.append(get_ibcbr())
    dfs_finais.append(get_dados_sidra(tabela=8888, variavel=12606,classificacao='/c544/129314/N1/all/N3/all', nome_indicador='IND_INDUSTRIA'))
    dfs_finais.append(get_dados_sidra(tabela=8880, variavel=7169,classificacao='/c11046/56734/N1/all/N3/all', nome_indicador='IND_VAREJO'))
    dfs_finais.append(get_dados_sidra(tabela=5906, variavel=7167, classificacao='/c11046/56726/N1/all/N3/all', nome_indicador='IND_SERVICOS'))
    dfs_finais.append(get_dados_sidra(tabela=6381, variavel=4099, classificacao='/N1/all', nome_indicador='TAX_DESEMPREGO'))
    alimentar_tabela_macro()