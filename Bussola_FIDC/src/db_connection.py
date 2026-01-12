
import os
import getpass

# Cache de conexão e credenciais para solicitar input apenas uma vez
_cached_conn = None
_cached_creds = None


def get_connection():
    """
    Abre e retorna a conexão com o OracleDB
    Não esquecer de fechar a conexão quando terminar de usar nos códigos
    """
    try:
        connection = oracledb.connect(
            user=os.getenv('ORACLE_USER'),
            password=os.getenv('ORACLE_PASSWORD'),
            dsn=os.getenv('ORACLE_DSN')
        )
        return connection

    except Exception as e:
        print(f"Erro ao conectar ao Oracle: {e}")
        return None


def close_connection():
    """Fecha a conexão cacheada (se existir) e limpa o cache do objeto de conexão.

    Não remove as credenciais em cache — elas permanecem para reconexões subsequentes.
    """
    global _cached_conn
    if _cached_conn:
        try:
            _cached_conn.close()
        except Exception:
            pass
        _cached_conn = None

