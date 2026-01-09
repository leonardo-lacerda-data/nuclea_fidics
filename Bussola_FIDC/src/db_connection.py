
import os
import getpass

# Cache de conexão e credenciais para solicitar input apenas uma vez
_cached_conn = None
_cached_creds = None


def get_connection():
    """Solicita ao usuário (uma vez) `user`, `password` e `dsn`, abre e retorna a conexão Oracle.

    O método faz cache das credenciais e da conexão para chamadas subsequentes.
    Não utiliza dotenv ou variáveis de ambiente internamente.
    """
    global _cached_conn, _cached_creds

    # Retorna conexão em cache se existir e estiver ativa
    if _cached_conn:
        try:
            # Tenta criar um cursor para validar a conexão
            _cached_conn.cursor()
            return _cached_conn
        except Exception:
            # Conexão estava fechada ou inválida — limpa cache e tenta reconectar
            try:
                _cached_conn.close()
            except Exception:
                pass
            _cached_conn = None

    # Solicita credenciais apenas uma vez
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

