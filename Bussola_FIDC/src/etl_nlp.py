import dateparser
import feedparser
import os
import pandas as pd
import random
import re
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime
from GoogleNews import GoogleNews
from pysentimiento import create_analyzer
from src.db_connection import get_connection

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

lista_final = []

HEADERS_FAKE = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

FONTES_CONFIAVEIS = [
    # --- GRANDES PORTAIS (Notícias Gerais) ---
    'globo.com', 'uol.com.br', 'cnnbrasil.com.br', 'estadao.com.br',
    'folha.uol.com.br', 'veja.abril.com.br', 'bbc.com', 'terra.com.br',
    'r7.com', 'metropoles.com', 'band.uol.com.br', 'correiobraziliense.com.br',

    # --- ECONOMIA, MERCADO & INVESTIMENTOS ---
    'infomoney.com.br', 'moneytimes.com.br', 'exame.com', 'valor.globo.com',
    'forbes.com.br', 'istoedinheiro.com.br', 'sunoresearch.com.br',
    'br.investing.com', 'inteligenciafinanceira.com.br', 'valorinveste.globo.com',
    'seudinheiro.com', 'fdr.com.br', 'monitoramercado.com.br',

    # --- SETORIAL: AGRO & INDÚSTRIA (Essencial para FIDCs) ---
    'canalrural.com.br', 'noticiasagricolas.com.br', 'agrolink.com.br',
    'globorural.globo.com', 'portaldaindustria.com.br', 'industrianews.com.br',
    'cni.com.br',

    # --- SETORIAL: VAREJO & CONSUMO ---
    'mercadoeconsumo.com.br', 'ecommercebrasil.com.br', 'novarejo.com.br',
    'supervarejo.com.br',

    # --- JURÍDICO & REGULATÓRIO (Risco de Crédito/Leis) ---
    'conjur.com.br', 'migalhas.com.br', 'jota.info'
]

TOPICOS = {
    'INDUSTRIA': 'https://news.google.com/rss/search?q=ind%C3%BAstria+brasil+desempenho&hl=pt-BR&gl=BR&ceid=BR:pt-419',
    'VAREJO': 'https://news.google.com/rss/search?q=varejo+vendas+brasil+economia&hl=pt-BR&gl=BR&ceid=BR:pt-419',
    'SERVICOS': 'https://news.google.com/rss/search?q=setor+servi%C3%A7os+crescimento+brasil&hl=pt-BR&gl=BR&ceid=BR:pt-419',
    'MERCADO': 'https://news.google.com/rss/search?q=mercado+financeiro+ibovespa+dolar&hl=pt-BR&gl=BR&ceid=BR:pt-419'
}

def validar_fonte_confiavel(link):
    """
    Verifica se o link pertence a um dos domínios da Whitelist.
    Retorna True se for confiável, False se for desconhecido.
    """
    link = str(link).lower()
    for fonte in FONTES_CONFIAVEIS:
        if fonte in link:
            return True
    return False

def converter_data_relativa(data_raw):
    """
    Abordagem Híbrida:
    1. Usa REGEX para corrigir erros de português ('á 2 dias' -> 'há 2 dias').
    2. Usa DATEPARSER para entender a lógica temporal.
    """
    if not data_raw:
        return datetime.now().date()

    texto_limpo = str(data_raw).lower().strip()

    # --- PASSO 1: LIMPEZA COM REGEX ---
    # O Google News às vezes devolve 'á 2 dias' (erro de PT). O dateparser não gosta disso.
    # Substituímos 'á ' no início ou meio da frase por 'há '
    texto_limpo = re.sub(r'^á\s', 'há ', texto_limpo)     # No começo da string
    texto_limpo = re.sub(r'\sá\s', ' há ', texto_limpo)   # No meio da string

    # Remove caracteres estranhos que possam vir no scraping
    texto_limpo = texto_limpo.replace('relacionado', '').strip()

    # --- PASSO 2: INTERPRETAÇÃO COM DATEPARSER ---
    try:
        # settings={'RELATIVE_BASE': ...} garante que 'há 2 dias' conta a partir de HOJE
        dt = dateparser.parse(
            texto_limpo,
            languages=['pt'],
            settings={'RELATIVE_BASE': datetime.now(), 'PREFER_DATES_FROM': 'past'}
        )

        if dt and dt.date():
            return dt.date()
        else:
            # Se o dateparser falhar, retornamos hoje como fallback
            return datetime.now().date()

    except Exception:
        return datetime.now().date()


def fazer_web_scraping_resumo(url):
    """
    Entra no link e busca o resumo (meta description).
    """
    try:
        response = requests.get(url, headers=HEADERS_FAKE, timeout=3)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'lxml')

            # Tenta pegar meta description
            meta = soup.find('meta', attrs={'name': 'description'}) or soup.find('meta',
                                                                                 attrs={'property': 'og:description'})
            if meta and meta.get('content'):
                return meta.get('content')

            # Se falhar, pega parágrafos
            paragrafos = soup.find_all('p')
            for p in paragrafos:
                texto = p.get_text().strip()
                if len(texto) > 60:
                    return texto[:500]
        return ""
    except:
        return ""


def analisar_sentimento(texto, analyzer):
    if not texto or len(texto) < 5: return 0.0
    try:
        resultado = analyzer.predict(texto)
        probs = resultado.probas
        # Score = Positivo - Negativo (-1 a 1)
        return probs.get('POS', 0) - probs.get('NEG', 0)
    except:
        return 0.0


def carregar_rss_tempo_real(bert_analyzer):
    print("-> 📡 Buscando RSS Tempo Real...")
    dados = []

    for setor, url in TOPICOS.items():
        feed = feedparser.parse(url)
        for entry in feed.entries[:5]:
            titulo = entry.title
            link = entry.link

            if not validar_fonte_confiavel(link):
                continue

            # Tenta pegar data do RSS
            try:
                if hasattr(entry, 'published_parsed'):
                    data_pub = datetime.fromtimestamp(time.mktime(entry.published_parsed)).date()
                else:
                    data_pub = datetime.now().date()
            except:
                data_pub = datetime.now().date()

            resumo_site = fazer_web_scraping_resumo(link)
            texto_completo = f"{titulo}. {resumo_site}"
            score = analisar_sentimento(texto_completo, bert_analyzer)

            dados.append((setor, titulo, score, data_pub, link))

    return dados


def carregar_historico_google(bert_analyzer, dias_atras=730):
    """
    Busca notícias antigas com paginação e datas corrigidas.
    """
    print(f"-> 🕰️ Iniciando Busca Histórica (Google News) - {dias_atras} dias...")

    googlenews = GoogleNews(lang='pt', region='BR')

    # Define Janela de Tempo
    start_date = (datetime.now() - pd.DateOffset(days=dias_atras)).strftime('%m/%d/%Y')
    end_date = (datetime.now() - pd.DateOffset(days=1)).strftime('%m/%d/%Y')
    googlenews.set_time_range(start_date, end_date)

    dados = []
    ids_vistos = set()

    termos = {
        'INDUSTRIA': 'Indústria Brasil produção',
        'VAREJO': 'Varejo vendas Brasil queda',
        'SERVICOS': 'Setor serviços Brasil',
        'MERCADO': 'Mercado financeiro Ibovespa economia'
    }

    for setor, termo in termos.items():
        print(f"   🔎 Buscando: {setor}...")
        googlenews.clear()
        googlenews.search(termo)

        # Percorre até 5 páginas para ter volume (~50 news por setor)
        for pagina in range(1, 11):
            googlenews.get_page(pagina)
            resultados = googlenews.result()

            for item in resultados:
                titulo = item['title']
                link = item['link']

                if not validar_fonte_confiavel(link):
                    continue

                data_raw = item.get('date')

                if titulo in ids_vistos: continue
                ids_vistos.add(titulo)

                # Correção de datas
                data_pub = converter_data_relativa(data_raw)

                if data_pub < start_date:
                    continue
                # Se a data for hoje, mas estamos buscando histórico,
                # pode ser que o parser falhou

                # Web Scraping do Conteúdo
                resumo_site = fazer_web_scraping_resumo(link)
                texto_completo = f"{titulo}. {resumo_site}"

                if len(texto_completo) > 30:
                    score = analisar_sentimento(texto_completo, bert_analyzer)
                    dados.append((setor, titulo, score, data_pub, link))

            # Pausa para evitar bloqueio
            time.sleep(random.uniform(2, 4))

    return dados

def alimentando_banco_dados():
    try:
        conn = get_connection()
        if conn:
            cursor = conn.cursor()

            # DICA: Se for rodar histórico, talvez não queira TRUNCATE (apagar tudo).
            # Se quiser acumular, tire o TRUNCATE. Se quiser limpar e refazer, deixe.
            print("   🧹 Limpando tabela antiga...")
            cursor.execute("TRUNCATE TABLE T_BF_NOTICIAS")

            print(f"   💾 Salvando {len(lista_final)} notícias no Oracle...")
            sql_insert = """
                         INSERT INTO T_BF_NOTICIAS (ds_setor, tx_titulo, vl_sentimento, dt_publicacao, tx_link)
                         VALUES (:1, :2, :3, :4, :5) \
                         """
            cursor.executemany(sql_insert, lista_final)
            conn.commit()
            print("   ✅ Sucesso!")

    except Exception as e:
        conn.rollback()
        print(f"❌ Erro banco: {e}")
    finally:
        conn.close()

def executar_etl_noticias():
    print("\n📰 [ETL NLP] Iniciando Pipeline...")

    try:
        bert_analyzer = create_analyzer(task="sentiment", lang="pt")
    except Exception as e:
        print(f"   ❌ Erro IA: {e}")
        return

    # 1. Busca Histórica (Google)
    # Se quiser forçar a recarga do histórico, mantenha descomentado
    lista_final.extend(carregar_historico_google(bert_analyzer, dias_atras=730))

    # 2. Busca Recente (RSS)
    lista_final.extend(carregar_rss_tempo_real(bert_analyzer))

    if not lista_final:
        print("   ⚠️ Nenhuma notícia encontrada.")
        return


    alimentando_banco_dados()
