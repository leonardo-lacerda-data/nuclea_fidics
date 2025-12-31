from src.setup_tabelas import recriar_banco_dados
from src.setup_views import atualizar_view_ml, atualizar_view_pbi
from src.ml_cluster import segmentar_clientes
from src.ml_risco import calcular_risco_credito
from src.database import get_connection
from src.etl_ingestion import carregar_dados
from src.etl_nlp import executar_etl_noticias


if __name__ == "__main__":
    print("--- INICIANDO BÚSSOLA DE FIDCS (SISTEMA INTEGRADO)---")
    recriar_banco_dados()

    carregar_dados()
    executar_etl_noticias()
    atualizar_view_ml()
    calcular_risco_credito()
    segmentar_clientes()
    atualizar_view_pbi()

    print("🏁--- PROCESSO FINALIZADO COM SUCESSO ---")