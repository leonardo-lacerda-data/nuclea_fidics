import os
import oracledb

# Tenta carregar .env, mas não quebra se não existir (pois usamos GUI)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def get_connection():
    """
    Abre uma NOVA conexão usando as credenciais injetadas pela GUI (os.environ).
    IMPORTANTE: Quem chamar esta função DEVE fechar a conexão usando conn.close().
    """
    try:
        # A GUI preenche estas variáveis em tempo de execução
        user = os.getenv('ORACLE_USER')
        password = os.getenv('ORACLE_PASSWORD')
        dsn = os.getenv('ORACLE_DSN')

        if not user or not password or not dsn:
            # Silencia o erro se for apenas uma verificação de import,
            # mas avisa se for tentativa real de conexão.
            return None

        connection = oracledb.connect(
            user=user,
            password=password,
            dsn=dsn
        )
        return connection

    except oracledb.DatabaseError as e:
        error, = e.args
        print(f"❌ Erro Oracle ORA-{error.code}: {error.message}")
        return None
    except Exception as e:
        print(f"❌ Erro genérico de conexão: {e}")
        return None