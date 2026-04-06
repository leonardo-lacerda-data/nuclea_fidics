import numpy as np
import os
import pandas as pd
from datetime import datetime, timedelta

# ================= CONFIGURAÇÕES =================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
ARQUIVO_ENTRADA = 'boletos.csv'
ARQUIVO_SAIDA = 'boletos_datas_variadas.csv'

# Janela de Tempo para Espalhar (Ex: Últimos 2 anos até hoje)
# Usamos datetime completo para compatibilidade com Pandas
DATA_FIM = datetime.now()
DATA_INICIO = DATA_FIM - timedelta(days=730)


# =================================================

def variar_datas_apenas(seed=None):
    # --- TRAVA DE SEGURANÇA (SEED) ---
    if seed is not None:
        np.random.seed(seed)

    print(f"📂 Lendo original: {ARQUIVO_ENTRADA}...")
    arquivo_entrada = os.path.join(DATA_DIR, ARQUIVO_ENTRADA)

    # 1. Leitura Robustez (Tenta vírgula, se falhar tenta ponto e vírgula)
    try:
        df = pd.read_csv(arquivo_entrada, sep=',')
        if len(df.columns) < 2:
            df = pd.read_csv(arquivo_entrada, sep=';')
    except Exception as e:
        print(f"❌ Erro na leitura: {e}")
        return

    print(f"   Processando {len(df)} boletos...")

    # Colunas esperadas
    COLUNAS_DATAS = ['dt_emissao', 'dt_vencimento', 'dt_pagamento']

    # Validação de Colunas
    faltantes = [c for c in COLUNAS_DATAS if c not in df.columns]
    if faltantes:
        print(f"❌ Erro: Colunas obrigatórias ausentes: {faltantes}")
        return

    # Converter para datetime do Pandas
    for col in COLUNAS_DATAS:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # 2. Calcular Deltas (Diferença entre as datas originais)
    # dias_para_pagar: Positivo = Atraso, Negativo = Adiantado, NaT = Não pago
    df['dias_para_pagar'] = df['dt_pagamento'] - df['dt_vencimento']
    df['dias_emissao_antes'] = df['dt_vencimento'] - df['dt_emissao']

    # 3. Gerar Novas Datas de Vencimento (Aleatórias no período)
    dias_totais = (DATA_FIM - DATA_INICIO).days

    # Gera vetor de dias aleatórios
    random_days = np.random.randint(0, dias_totais, size=len(df))

    # Cria nova série de vencimentos convertendo para datetime64[ns]
    # Isso é crucial para a soma subsequente funcionar corretamente
    novos_vencimentos = [DATA_INICIO + timedelta(days=int(d)) for d in random_days]
    df['dt_vencimento'] = pd.to_datetime(novos_vencimentos)

    # 4. Recalcular Emissão e Pagamento usando os deltas originais
    # Pandas gerencia automaticamente a soma de Data + Timedelta
    df['dt_pagamento'] = df['dt_vencimento'] + df['dias_para_pagar']
    df['dt_emissao'] = df['dt_vencimento'] - df['dias_emissao_antes']

    # 5. Limpeza e Formatação
    # Remove colunas auxiliares
    df.drop(columns=['dias_para_pagar', 'dias_emissao_antes'], inplace=True)

    # Formata para YYYY-MM-DD (Padrão Banco de Dados)
    for col in COLUNAS_DATAS:
        # .dt.strftime converte para string formatada
        # fillna('') garante que datas nulas (NaT) fiquem vazias no CSV, não 'NaT'
        df[col] = df[col].dt.strftime('%Y-%m-%d').fillna('')

    # 6. Salvar
    print(f"💾 Salvando {ARQUIVO_SAIDA}...")
    arquivo_saida = os.path.join(DATA_DIR, ARQUIVO_SAIDA)

    # index=False para não criar coluna de índice extra
    df.to_csv(arquivo_saida, sep=',', index=False)

    print(f"✅ FEITO! As datas dos {len(df)} boletos foram espalhadas entre {DATA_INICIO.year} e {DATA_FIM.year}.")