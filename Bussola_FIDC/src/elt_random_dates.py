import numpy as np
import os
import pandas as pd
from datetime import datetime, timedelta

# ================= CONFIGURAÇÕES =================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
ARQUIVO_ENTRADA = 'boletos.csv'
ARQUIVO_SAIDA = 'boletos_datas_variadas.csv'

# Janela de Tempo para Espalhar (Ex: Jan/2023 até Hoje)
DATA_FIM = datetime.now()
DATA_INICIO = datetime(2024, 1, 1)

# =================================================

def variar_datas_apenas():
    # --- TRAVA DE SEGURANÇA (SEED) ---
    # Se você descomentar a linha abaixo, o sorteio será SEMPRE IGUAL
    np.random.seed(35)

    print(f"📂 Lendo original: {ARQUIVO_ENTRADA}...")

    arquivo = os.path.join(DATA_DIR, ARQUIVO_ENTRADA)

    # 1. Leitura (Tenta vírgula, se falhar tenta ponto e vírgula)
    try:
        df = pd.read_csv(arquivo, sep=',')
        if len(df.columns) < 2:
            df = pd.read_csv(arquivo, sep=';')
    except Exception as e:
        print(f"❌ Erro na leitura: {e}")
        return

    print(f"   Processando {len(df)} boletos...")

    # Converter para data real
    for col in ['dt_vencimento', 'dt_emissao', 'dt_pagamento']:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # 2. Calcular Deltas (Para manter a lógica original: pagou 3 dias depois? continua 3 dias depois)
    df['dias_para_pagar'] = df['dt_pagamento'] - df['dt_vencimento']
    df['dias_emissao_antes'] = df['dt_vencimento'] - df['dt_emissao']

    # 3. Gerar Novas Datas de Vencimento (Aleatórias no período)
    dias_totais = (DATA_FIM - DATA_INICIO).days

    # Gera um vetor de dias aleatórios (um para cada linha)
    random_days = np.random.randint(0, dias_totais, size=len(df))

    # Aplica a nova data base
    novos_vencimentos = [DATA_INICIO + timedelta(days=int(d)) for d in random_days]
    df['dt_vencimento'] = novos_vencimentos

    # 4. Recalcular Emissão e Pagamento usando os deltas
    df['dt_pagamento'] = df['dt_vencimento'] + df['dias_para_pagar']
    df['dt_emissao'] = df['dt_vencimento'] - df['dias_emissao_antes']

    # 5. Limpeza e Formatação
    df.drop(columns=['dias_para_pagar', 'dias_emissao_antes'], inplace=True)

    # Formata para YYYY-MM-DD
    for col in ['dt_vencimento', 'dt_emissao', 'dt_pagamento']:
        # Se for NaT (não pago), fica vazio. Se for data, formata.
        df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if not pd.isnull(x) else '')

    # 6. Salvar (Separado por VÍRGULA para ser igual ao original)
    print(f"💾 Salvando {ARQUIVO_SAIDA}...")
    arquivo = os.path.join(DATA_DIR, ARQUIVO_SAIDA)
    df.to_csv(arquivo, sep=',', index=False)

    print(f"✅ FEITO! As datas dos {len(df)} boletos foram espalhadas entre 2023 e hoje.")