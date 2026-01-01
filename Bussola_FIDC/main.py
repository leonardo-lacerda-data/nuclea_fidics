from src.setup_tables import recriar_banco_dados
from src.setup_views import atualizar_view_ml, atualizar_view_pbi
from src.ml_cluster import segmentar_clientes
from src.ml_risk import calcular_risco_credito
from src.db_connection import get_connection
from src.etl_api import carregar_api
from src.etl_ingestion import carregar_dados
from src.etl_nlp import executar_etl_noticias


if __name__ == "__main__":
    print("--- INICIANDO BÚSSOLA DE FIDCS (SISTEMA INTEGRADO)---")
    resposta_cria_tabela = input("   ❓ Precisa criar as tabelas? (s/n): ").lower()
    if  resposta_cria_tabela == 's':
    # Função para criar os bancos de dados
        recriar_banco_dados()
        carregar_dados()
        carregar_api()
        executar_etl_noticias()
    elif resposta_cria_tabela == 'n':
        # Função para ler os dados fornecidos pela Nuclea e os dados Macro Econômicos
        if input(f"   ❓ Precisa ler os dados novamente? (s/n): ").lower() == 's':
            carregar_dados()
            carregar_api()
        # Função para ler as notícias
        if input(f"   ❓ Precisa ler as notícias novamente? (s/n): ").lower() == 's':
            executar_etl_noticias()

    # Função para criar a View do ML
    atualizar_view_ml()

    reposta_ml = input("   ❓ Deseja retreinar o Machine Learning? (s/n): ").lower()
    if reposta_ml == 's':
        # Função para calcular o Risco a partir de Regressão Logística
        calcular_risco_credito(force_retrain = True)
        # Função para o clustering a partir de K-Means
        segmentar_clientes(force_retrain = True)
    else:
        calcular_risco_credito()
        segmentar_clientes()

    # Função para criar a view para o Power BI
    atualizar_view_pbi()

    print("🏁--- PROCESSO FINALIZADO COM SUCESSO ---")