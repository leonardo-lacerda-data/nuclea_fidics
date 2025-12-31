import feedparser
import os
import pandas as pd
import spacy
import time
from datetime import datetime
from pysentimiento import create_analyzer
from src.database import get_connection

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

TOPICOS = {
    'INDUSTRIA': 'https://news.google.com/rss/search?q=ind%C3%BAstria+brasil+desempenho&hl=pt-BR&gl=BR&ceid=BR:pt-419',
    'VAREJO': 'https://news.google.com/rss/search?q=varejo+vendas+brasil+economia&hl=pt-BR&gl=BR&ceid=BR:pt-419',
    'SERVICOS': 'https://news.google.com/rss/search?q=setor+servi%C3%A7os+crescimento+brasil&hl=pt-BR&gl=BR&ceid=BR:pt-419',
    'MERCADO': 'https://news.google.com/rss/search?q=mercado+financeiro+ibovespa+dolar&hl=pt-BR&gl=BR&ceid=BR:pt-419'
}

# Para carregar o csv com as notícias históricas
def carregar_noticias_historicas(nlp, bert_analyzer):
    print("-> Processando notícias reais...")

    arquivo = os.path.join(DATA_DIR, 'noticias_historicas.csv')
    lista_processada = []

    if os.path.exists(arquivo):
        try:
            rows = []
            with open(arquivo, "r", encoding="utf-8-sig") as f:
                next(f)  # pula: "sep=;"
                header = next(f).strip().strip('"').split(";")  # dt_publicacao;ds_setor;tx_titulo

                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    line = line.strip('"')  # remove aspas externas
                    parts = line.split(";", 2)  # divide só em 3 colunas
                    if len(parts) == 3:
                        rows.append(parts)

            df_news = pd.DataFrame(rows, columns=header)

            for _, row in df_news.iterrows():
                texto_limpo, score = pipeline_nlp(row['tx_titulo'], nlp, bert_analyzer)

                data_real = pd.to_datetime(row['dt_publicacao']).date()

                lista_processada.append({
                    'DS_SETOR': row['ds_setor'],
                    'TX_TITULO': texto_limpo[:300],
                    'VL_SENTIMENTO': score,
                    'DT_PUBLICACAO': data_real
                })

            print(f"      ✅ {len(lista_processada)} notícias históricas carregadas.")
        except Exception as e:
            print(f"      ⚠️ Erro no CSV histórico: {e}")

    return lista_processada


def carregar_rss_tempo_real(nlp, bert_analyzer):
    """
    Baixa notícias do Google News
    """
    print("-> Processando notícias do Google...")
    lista_rss = []

    for setor, url in TOPICOS.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:100]:
                texto_limpo, score = pipeline_nlp(entry.title, nlp, bert_analyzer)

                # Pegando a data
                if hasattr(entry, 'published_parsed'):
                    data_struct = entry.published_parsed
                    data_publicacao = datetime.fromtimestamp(time.mktime(data_struct))
                else:
                    data_publicacao = datetime.now().date()

                lista_rss.append({
                    'DS_SETOR': setor,
                    'TX_TITULO': texto_limpo[:300],
                    'VL_SENTIMENTO': score,
                    'DT_PUBLICACAO': data_publicacao
                })
        except Exception as e:
            print(f"      ⚠️ Erro ao ler RSS de {setor}: {e}")
            continue

    print(f"      ✅ {len(lista_rss)} notícias recentes baixadas.")
    return lista_rss

def pipeline_nlp(texto_bruto, nlp, bert_analyzer):
    """
    1. spaCy limpa (ex: quebras de linha estranhas, espaços duplos).
    2. BERT analisa o sentimento do texto limpo.
    """
    # O spaCy lê o texto e separa em tokens
    doc = nlp(texto_bruto)

    # Reconstrói a frase removendo espaços extras e quebras de linha
    texto_limpo = " ".join([token.text for token in doc if not token.is_space])
    try:
    # PySentimiento (Inaleingência)
        resultado = bert_analyzer.predict(texto_limpo)
        probs = resultado.probas

        # Score Composto (-1 a 1)
        score = probs.get('POS', 0) - probs.get('NEG', 0)
    except:
        score = 0.0

    return texto_limpo, score

def executar_etl_noticias():
    print("\n📰 [ETL NLP] Iniciando Pipeline Híbrido (Histórico + RealTime)...")

    print("   -> 🧠 Carregando modelos de IA (pode demorar um pouco)...")

    try:
        bert_analyzer = create_analyzer(task="sentiment", lang="pt")
        nlp = spacy.load("pt_core_news_sm", disable=['ner', 'parser'])
    except Exception as e:
        print(f"   ❌ Erro ao carregar modelos. Rode 'python -m spacy download pt_core_news_sm'. Erro: {e}")
        return

    lista_final = []

    # Carregando o CSV com notícias históricas (CSV)
    lista_final.extend(carregar_noticias_historicas(nlp, bert_analyzer))

    # Carregando as notícias atuais (RSS)
    lista_final.extend(carregar_rss_tempo_real(nlp, bert_analyzer))

    # Conectando ao BD para informar os dados
    try:
        conn = get_connection()
        if conn:
            cursor = conn.cursor()

            sql_insert = """
                INSERT INTO T_BF_NOTICIAS (ds_setor, tx_titulo, vl_sentimento, dt_publicacao)
                VALUES (:1, :2, :3, :4)
            """
            dados = [[x['DS_SETOR'], x['TX_TITULO'], x['VL_SENTIMENTO'], x['DT_PUBLICACAO']] for x in lista_final]
            cursor.executemany(sql_insert, dados)
            conn.commit()
            print(f"✅ Pipeline concluído! {len(dados)} notícias gravadas.")

    except Exception as e:
        print(f"❌ Erro: {e}")

    finally:
        conn.close()