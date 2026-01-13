import feedparser
import os
import random
import time
import dateparser
from datetime import datetime, timedelta
from duckduckgo_search import DDGS
from pysentimiento import create_analyzer
from src.db_connection import get_connection

# CONFIGURAÇÕES
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# RSS Links
TOPICOS_RSS = {
    'AGRO': 'https://news.google.com/rss/search?q=agroneg%C3%B3cio+brasil+safra&hl=pt-BR&gl=BR&ceid=BR:pt-419',
    'INDUSTRIA': 'https://news.google.com/rss/search?q=ind%C3%BAstria+brasil+desempenho&hl=pt-BR&gl=BR&ceid=BR:pt-419',
    'VAREJO': 'https://news.google.com/rss/search?q=varejo+vendas+brasil+economia&hl=pt-BR&gl=BR&ceid=BR:pt-419',
    'SERVICOS': 'https://news.google.com/rss/search?q=setor+servi%C3%A7os+crescimento+brasil&hl=pt-BR&gl=BR&ceid=BR:pt-419',
    'MERCADO': 'https://news.google.com/rss/search?q=mercado+financeiro+ibovespa+dolar&hl=pt-BR&gl=BR&ceid=BR:pt-419'
}

# TERMOS
TERMOS_HISTORICO = {
    'AGRO': [
        'Safra de soja Brasil resultados',
        'Agronegócio exportação desempenho',
        'Crédito rural Plano Safra',
        'Preço commodities agrícolas hoje',
        'Colheita Brasil'
    ],
    'INDUSTRIA': [
        'Produção industrial IBGE desempenho',
        'Indústria automobilística Brasil',
        'Investimentos indústria nacional',
        'Sondagem industrial CNI'
    ],
    'VAREJO': [
        'Vendas varejo Brasil desempenho',
        'Balanço e-commerce Brasil',
        'Expansão atacarejo Brasil',
        'Índice de consumo das famílias'
    ],
    'SERVICOS': [
        'Volume de serviços PMS IBGE',
        'Turismo Brasil faturamento',
        'Setor de logística e transportes',
        'Mercado de trabalho serviços'
    ],
    'MERCADO': [
        'Ibovespa fechamento',
        'Relatório Focus Banco Central',
        'Balança comercial Brasil',
        'Resultado PIB trimestral'
    ]
}

# FONTES QUE ACEITAMOS
FONTES_ACEITAS = [
    'uol', 'globo', 'cnn', 'estadao', 'folha', 'veja', 'bbc', 'terra', 'r7',
    'metropoles', 'band', 'correio', 'infomoney', 'money', 'exame', 'valor',
    'forbes', 'sun', 'investing', 'agrolink', 'canal rural', 'cni', 'fdr',
    'monitor', 'seudinheiro', 'poder360', 'jota', 'migalhas', 'conjur',
    'valor economico', 'g1', 'bloomberg', 'reuters', 'ibge', 'ipea', 'cnn brasil'
]


def validar_fonte_por_texto(texto):
    texto = str(texto).lower()
    for fonte in FONTES_ACEITAS:
        if fonte in texto:
            return True
    return False


def analisar_sentimento(texto, analyzer):
    if not texto or len(texto) < 5: return 0.0
    try:
        resultado = analyzer.predict(texto)
        probs = resultado.probas
        return probs.get('POS', 0) - probs.get('NEG', 0)
    except:
        return 0.0


def validar_recencia(data_pub, dias_max=730):
    if not data_pub: return False
    data_corte = datetime.now().date() - timedelta(days=dias_max)
    if data_pub < data_corte:
        return False
    return True


def limpar_data_ddg(data_raw):
    """Trata datas do DuckDuckGo que vêm em formatos variados."""
    if not data_raw: return None
    try:
        # DDG às vezes manda timestamp ISO, às vezes texto relativo
        dt = dateparser.parse(str(data_raw))
        return dt.date() if dt else None
    except:
        return None


# ==============================================================================
# CARGA RSS
# ==============================================================================
def carregar_rss_tempo_real(bert_analyzer):
    print("-> 📡 Buscando RSS Tempo Real...")
    dados = []

    for setor, url in TOPICOS_RSS.items():
        print(f"   ...lendo RSS de {setor}")
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                titulo = entry.title
                link = entry.link
                fonte_rss = entry.source.get('title', '').lower() if 'source' in entry else ''

                if not validar_fonte_por_texto(fonte_rss) and not validar_fonte_por_texto(titulo):
                    continue

                try:
                    if hasattr(entry, 'published_parsed'):
                        data_pub = datetime.fromtimestamp(time.mktime(entry.published_parsed)).date()
                    else:
                        data_pub = datetime.now().date()
                except:
                    data_pub = datetime.now().date()

                if not validar_recencia(data_pub):
                    continue

                score = analisar_sentimento(titulo, bert_analyzer)
                dados.append((setor, titulo, score, data_pub, link))
        except Exception as e:
            print(f"   ⚠️ Erro RSS {setor}: {e}")
    return dados


# ==============================================================================
# CARGA DUCKDUCKGO
# ==============================================================================
def carregar_historico_completo(bert_analyzer, dias_atras=730):
    print(f"-> 🕰️ Iniciando Busca Histórica via DuckDuckGo (Sem 429!)...")
    dados = []
    ids_vistos = set()

    # Instancia o buscador
    ddgs = DDGS()

    for setor, lista_termos in TERMOS_HISTORICO.items():
        print(f"   🔎 Setor: {setor}...")

        for termo in lista_termos:
            print(f"      Busca: '{termo}'")
            try:
                # max_results=25 por termo garante um bom volume histórico sem travar
                # region="br-pt" foca no Brasil
                resultados = ddgs.news(
                    keywords=termo,
                    region="br-pt",
                    safesearch="off",
                    max_results=30
                )

                if resultados:
                    count_termo = 0
                    for item in resultados:
                        # O DDG retorna dict: {'date':..., 'title':..., 'body':..., 'url':..., 'source':...}
                        titulo = item.get('title')
                        link = item.get('url')
                        source = item.get('source', '')
                        data_raw = item.get('date')

                        # Deduplicação
                        if not titulo or titulo in ids_vistos: continue

                        # Validação de Fonte
                        if not validar_fonte_por_texto(link) and not validar_fonte_por_texto(source):
                            continue

                        # Tratamento de Data
                        data_pub = limpar_data_ddg(data_raw)

                        # Se não conseguiu ler a data do DDG, assume uma aleatória recente (fallback)
                        # ou descarta. Vamos assumir aleatória nos últimos 6 meses para não perder o dado.
                        if not data_pub:
                            dias_rand = random.randint(1, 180)
                            data_pub = (datetime.now() - timedelta(days=dias_rand)).date()

                        # Filtro de Recência
                        if not validar_recencia(data_pub, dias_atras):
                            continue

                        # Sentimento
                        score = analisar_sentimento(titulo, bert_analyzer)

                        dados.append((setor, titulo, score, data_pub, link))
                        ids_vistos.add(titulo)
                        count_termo += 1

                    # print(f"         ✅ {count_termo} notícias coletadas.")

                # Pausa leve (DuckDuckGo é rápido, 2s é suficiente)
                time.sleep(2)

            except Exception as e:
                print(f"         ⚠️ Erro no termo '{termo}': {e}")
                time.sleep(5)  # Pausa um pouco maior se der erro

        # Pausa entre setores
        print(f"      ☕ Mudando de setor... (5s)")
        time.sleep(5)

    return dados


# ==============================================================================
# ORQUESTRAÇÃO
# ==============================================================================
def executar_etl_noticias():
    print("\n📰 [ETL NLP] Iniciando Pipeline (DuckDuckGo Edition)...")

    try:
        bert_analyzer = create_analyzer(task="sentiment", lang="pt")
    except Exception as e:
        print(f"   ❌ Erro IA: {e}")
        return

    lista_final = []

    # 1. RSS
    news_rss = carregar_rss_tempo_real(bert_analyzer)
    lista_final.extend(news_rss)
    print(f"   📊 Notícias RSS: {len(news_rss)}")

    # 2. Histórico (DuckDuckGo)
    # Aumentei para 730 dias (2 anos) já que o DDG aguenta
    news_hist = carregar_historico_completo(bert_analyzer, dias_atras=730)
    lista_final.extend(news_hist)
    print(f"   🕰️ Notícias Históricas: {len(news_hist)}")

    if not lista_final:
        print("   ⚠️ Nenhuma notícia coletada.")
        return

    try:
        conn = get_connection()
        if not conn: return

        if conn:
            cursor = conn.cursor()
            print("   🧹 Limpando tabela de notícias...")

            # Limpa tudo antes de inserir
            cursor.execute("DELETE FROM T_BF_NOTICIAS")

            print(f"   💾 Salvando {len(lista_final)} notícias...")

            sql_insert = """
                         INSERT INTO T_BF_NOTICIAS (ds_setor, tx_titulo, vl_sentimento, dt_publicacao, tx_link)
                         VALUES (:1, :2, :3, :4, :5)
                         """

            batch_size = 500
            for i in range(0, len(lista_final), batch_size):
                batch = lista_final[i:i + batch_size]
                cursor.executemany(sql_insert, batch)

            conn.commit()
            print("   ✅ Sucesso! Banco atualizado.")

    except Exception as e:
        if 'conn' in locals() and conn:
            conn.rollback()
        print(f"❌ Erro banco: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()