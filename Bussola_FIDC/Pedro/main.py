import argparse
from src.setup_tables import recriar_banco_dados
from src.setup_views import atualizar_view_ml, atualizar_view_pbi
from src.ml_cluster import segmentar_clientes
from src.ml_risk import calcular_risco_credito
from src.etl_api import carregar_api
from src.etl_ingestion import carregar_dados
from src.etl_nlp import executar_etl_noticias


def run_pipeline(mode = 'full'):
    print(f"🚀 Executando pipeline no modo: {mode}")
    if mode == 'full':
        recriar_banco_dados()
        carregar_dados()
        carregar_api()
        executar_etl_noticias()
        atualizar_view_ml()
        calcular_risco_credito(force_retrain = True)
        segmentar_clientes(force_retrain=True)
        atualizar_view_pbi()
    elif mode == 'ml_only':
        calcular_risco_credito()
        segmentar_clientes()
        atualizar_view_pbi()
    elif mode == 'ml_only_retrain':
        calcular_risco_credito(force_retrain=True)
        segmentar_clientes(force_retrain=True)
        atualizar_view_pbi()
    else:
        raise ValueError(
            "Modo inválido. Use: full | ml_only | ml_only_retrain"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Modo que será rodado o código.")
    parser.add_argument(
        "-m",
        "--mode",
        type=str,
        default="full",
        choices=["full", "ml_only", "ml_only_retrain"],
        help="Modo de execução do pipeline"
    )
    args = parser.parse_args()

    print("--- INICIANDO BÚSSOLA DE FIDCS (SISTEMA INTEGRADO)---")
    run_pipeline(args.mode)
    print("🏁--- PROCESSO FINALIZADO COM SUCESSO ---")