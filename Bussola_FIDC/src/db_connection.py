
import os
import getpass

# Cache de conexão e credenciais para solicitar input apenas uma vez
_cached_conn = None
_cached_creds = None


def get_connection():
    """Abre e retorna a conexão Oracle.

    Prioriza variáveis de ambiente (definidas pela GUI):
      ORA_USER, ORA_PASSWORD, ORA_HOST, ORA_PORT, ORA_SERVICE

    Se todas as variáveis estiverem presentes, usa-as sem prompt interativo.
    Caso contrário, mantém o comportamento antigo de pedir `input()` e `getpass()`.

    Mantém cache de conexão e credenciais para reutilização.
    """
    global _cached_conn, _cached_creds

    # Retorna conexão em cache se existir e estiver ativa
    if _cached_conn:
        try:
            _cached_conn.cursor()
            return _cached_conn
        except Exception:
            try:
                _cached_conn.close()
            except Exception:
                pass
            _cached_conn = None

    # Tenta obter credenciais a partir de variáveis de ambiente (mais conveniente para a GUI)
    env_user = os.environ.get("ORA_USER")
    env_password = os.environ.get("ORA_PASSWORD")
    env_host = os.environ.get("ORA_HOST")
    env_port = os.environ.get("ORA_PORT")
    env_service = os.environ.get("ORA_SERVICE")

    if env_user and env_password and env_host and env_port and env_service:
        user = env_user
        password = env_password
        dsn = f"{env_host}:{env_port}/{env_service}"
        _cached_creds = (user, password, dsn)
    else:
        if not _cached_creds:
            user = input("Oracle user: ").strip()
            password = getpass.getpass("Oracle password: ")
            dsn = input("Oracle DSN (host:port/service): ").strip()
            _cached_creds = (user, password, dsn)
        else:
            user, password, dsn = _cached_creds

    try:
        import oracledb

        conn = oracledb.connect(user=user, password=password, dsn=dsn)
        _cached_conn = conn
        return conn
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

