import feedparser
import os
import random
import time
import dateparser
import sys
import tqdm
from datetime import datetime, timedelta
from duckduckgo_search import DDGS
from pysentimiento import create_analyzer
from src.db_connection import get_connection


# ==============================================================================
# CLASSE DE SILENCIAMENTO TOTAL (A Mordaça)
# ==============================================================================
class SuppressStderr:
    """
    Context Manager que desvia a saída de erro (stderr) para o limbo (devnull).
    Isso impede que bibliotecas imprimam avisos vermelhos no console.
    """

    def __enter__(self):
        self.err = sys.stderr
        sys.stderr = open(os.devnull, 'w')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stderr.close()
        sys.stderr = self.err


# CONFIGURAÇÕES
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TOPICOS_RSS = {
    'AGRO': 'https://news.google.com/rss/search?q=agroneg%C3%B3cio+brasil+safra&hl=pt-BR&gl=BR&ceid=BR:pt-419',
    'INDUSTRIA': 'https://news.google.com/rss/search?q=ind%C3%BAstria+brasil+desempenho&hl=pt-BR&gl=BR&ceid=BR:pt-419',
    'VAREJO': 'https://news.google.com/rss/search?q=varejo+vendas+brasil+economia&hl=pt-BR&gl=BR&ceid=BR:pt-419',
    'SERVICOS': 'https://news.google.com/rss/search?q=setor+servi%C3%A7os+crescimento+brasil&hl=pt-BR&gl=BR&ceid=BR:pt-419',
    'MERCADO': 'https://news.google.com/rss/search?q=mercado+financeiro+ibovespa+dolar&hl=pt-BR&gl=BR&ceid=BR:pt-419'
}

TERMOS_HISTORICO = {
    'AGRO': ['Safra soja Brasil', 'Agronegócio exportação', 'Crédito rural', 'Colheita'],
    'INDUSTRIA': ['Produção industrial IBGE', 'Indústria automobilística', 'Sondagem CNI'],
    'VAREJO': ['Vendas varejo', 'Balanço e-commerce', 'Consumo famílias'],
    'SERVICOS': ['Volume serviços IBGE', 'Turismo faturamento', 'Setor logística'],
    'MERCADO': ['Ibovespa hoje', 'Relatório Focus', 'PIB Brasil']
}

FONTES_ACEITAS = ['uol', 'globo', 'cnn', 'estadao', 'folha', 'veja', 'bbc', 'terra', 'r7', 'metropoles', 'band',
                  'correio', 'infomoney', 'money', 'exame', 'valor', 'forbes', 'sun', 'investing', 'agrolink',
                  'canal rural', 'cni', 'fdr', 'monitor', 'seudinheiro', 'poder360', 'jota', 'migalhas', 'conjur',
                  'valor economico', 'g1', 'bloomberg', 'reuters', 'ibge', 'ipea', 'cnn brasil']


def validar_fonte_por_texto(texto):
    txt = str(texto).lower()
    for fonte in FONTES_ACEITAS:
        if fonte in txt: return True
    return False


def analisar_sentimento(texto, analyzer):
    if not texto or len(texto) < 5: return 0.0
    try:
        resultado = analyzer.predict(texto)
        probs = resultado.probas
        score = probs.get('POS', 0) - probs.get('NEG', 0)
        return round(score, 4)
    except:
        return 0.0


def validar_recencia(data_pub, dias_max=730):
    if not data_pub: return False
    if isinstance(data_pub, datetime):
        data_pub = data_pub.date()
    data_corte = datetime.now().date() - timedelta(days=dias_max)
    return data_pub >= data_corte


def limpar_data_ddg(data_raw):
    if not data_raw: return None
    # Silencia o DateParser
    try:
        with SuppressStderr():
            dt = dateparser.parse(str(data_raw), settings={'PREFER_DATES_FROM': 'past'})
            return dt if dt else None
    except:
        return None


def carregar_rss_tempo_real(bert_analyzer):
    print("-> 📡 Buscando RSS Tempo Real...")
    dados = []
    for setor, url in TOPICOS_RSS.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                titulo = entry.title
                link = entry.link
                fonte_rss = entry.source.get('title', '').lower() if 'source' in entry else ''

                if not validar_fonte_por_texto(fonte_rss) and not validar_fonte_por_texto(titulo): continue

                try:
                    # Silencia avisos de data no RSS
                    with SuppressStderr():
                        if hasattr(entry, 'published_parsed'):
                            dt = datetime.fromtimestamp(time.mktime(entry.published_parsed))
                        else:
                            dt = datetime.now()
                except:
                    dt = datetime.now()

                if not validar_recencia(dt): continue

                score = analisar_sentimento(titulo, bert_analyzer)

                titulo_seguro = titulo[:390]
                link_seguro = link[:1990]

                dados.append((setor, titulo_seguro, score, dt, link_seguro))
        except Exception as e:
            print(f"   ⚠️ Erro RSS {setor}: {e}")
    return dados


def carregar_historico_completo(bert_analyzer, dias_atras=730):
    print(f"-> 🕰️ Iniciando Busca Histórica (Modo Silencioso)...")
    dados = []
    ids_vistos = set()

    for setor, lista_termos in TERMOS_HISTORICO.items():
        print(f"   🔎 Setor: {setor}...")
        for termo in lista_termos:
            sucesso = False
            tentativas = 0
            while not sucesso and tentativas < 3:
                try:
                    # AQUI ESTÁ A CORREÇÃO PRINCIPAL
                    # O 'SuppressStderr' engole qualquer print de erro ou warning
                    with SuppressStderr():
                        with DDGS() as ddgs:
                            resultados = ddgs.news(termo, region="br-pt", safesearch="off", max_results=10)

                            if resultados:
                                for item in resultados:
                                    titulo = item.get('title')
                                    link = item.get('url')
                                    source = item.get('source', '')
                                    data_raw = item.get('date')

                                    if not titulo or titulo in ids_vistos: continue
                                    if not validar_fonte_por_texto(link) and not validar_fonte_por_texto(
                                        source): continue

                                    data_pub = limpar_data_ddg(data_raw)
                                    if not data_pub:
                                        dias_rand = random.randint(1, 180)
                                        data_pub = datetime.now() - timedelta(days=dias_rand)

                                    if not validar_recencia(data_pub, dias_atras): continue

                                    score = analisar_sentimento(titulo, bert_analyzer)

                                    titulo_seguro = titulo[:390]
                                    link_seguro = link[:1990]

                                    dados.append((setor, titulo_seguro, score, data_pub, link_seguro))
                                    ids_vistos.add(titulo)

                    # Marcamos sucesso fora do bloco silenciado para a lógica continuar
                    sucesso = True

                except Exception as e:
                    erro_str = str(e).lower()
                    if "202" in erro_str or "ratelimit" in erro_str:
                        print(f"      🛑 Rate Limit. Dormindo 10s...")
                        time.sleep(10)
                        tentativas += 1
                    else:
                        break

            time.sleep(1.5)
        time.sleep(2)

    return dados


def executar_etl_noticias():
    print("\n📰 [ETL NLP] Iniciando Pipeline...")

    tqdm.tqdm = lambda *args, **kwargs: tqdm.tqdm(*args, **kwargs, disable=True)

    for stream in [sys.stdout, sys.stderr]:
        if not hasattr(stream, 'isatty'):
            stream.isatty = lambda: False

    try:
        bert_analyzer = create_analyzer(task="sentiment", lang="pt")
    except Exception as e:
        print(f"   ❌ Erro IA: {e}")
        return

    lista_final = []
    lista_final.extend(carregar_rss_tempo_real(bert_analyzer))

    # Busca histórica
    lista_final.extend(carregar_historico_completo(bert_analyzer, dias_atras=730))

    if not lista_final:
        print("   ⚠️ Nenhuma notícia coletada. O banco não será alterado.")
        return

    conn = get_connection()
    if not conn:
        print("   ❌ Sem conexão com o banco.")
        return

    try:
        cursor = conn.cursor()
        print(f"   🧹 Limpando tabela de notícias...")
        cursor.execute("DELETE FROM T_BF_NOTICIAS")

        print(f"   💾 Tentando salvar {len(lista_final)} notícias...")

        sql = "INSERT INTO T_BF_NOTICIAS (ds_setor, tx_titulo, vl_sentimento, dt_publicacao, tx_link) VALUES (:1, :2, :3, :4, :5)"

        batch_size = 100
        for i in range(0, len(lista_final), batch_size):
            batch = lista_final[i:i + batch_size]
            cursor.executemany(sql, batch)

        conn.commit()
        print("   ✅ SUCESSO! Banco atualizado.")

    except Exception as e:
        if 'conn' in locals() and conn: conn.rollback()
        print(f"❌ ERRO NO BANCO: {e}")
    finally:
        if 'conn' in locals() and conn: conn.close()