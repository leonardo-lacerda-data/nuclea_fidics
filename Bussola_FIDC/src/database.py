import os
import oracledb
from dotenv import load_dotenv

# Carrega as variáveis de ambiente (.env)
load_dotenv()

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
        print(f"❌ Erro fatal de conexão: {e}")
        return None