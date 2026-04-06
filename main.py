import argparse
from src.setup_tables import recriar_banco_dados
from src.setup_views import atualizar_view_ml, atualizar_view_pbi
from src.ml_cluster import segmentar_clientes
from src.ml_risk import calcular_risco_credito
from src.etl_api import carregar_api
from src.etl_ingestion import carregar_dados
from src.etl_nlp import executar_etl_noticias
from src.gui import run_gui


def run_pipeline(mode='full'):
    print(f"🚀 Executando pipeline no modo: {mode}")

    if mode == 'full':
        # 1. Infraestrutura
        recriar_banco_dados()

        # 2. ETL (Extração e Carga)
        carregar_dados()  # CSVs
        carregar_api()  # Macroeconomia
        executar_etl_noticias()  # NLP

        # 3. Preparação para IA
        atualizar_view_ml()

        # 4. Inteligência Artificial (Treino Forçado)
        calcular_risco_credito(force_retrain=True)
        segmentar_clientes(force_retrain=True)

        # 5. Visualização
        atualizar_view_pbi()

    elif mode == 'ml_only':
        # Usa modelos salvos se existirem
        calcular_risco_credito()
        segmentar_clientes()
        atualizar_view_pbi()

    elif mode == 'ml_only_retrain':
        # Força o re-treino dos modelos
        calcular_risco_credito(force_retrain=True)
        segmentar_clientes(force_retrain=True)
        atualizar_view_pbi()

    else:
        raise ValueError("Modo inválido. Use: full | ml_only | ml_only_retrain")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Bússola de FIDCs")
    parser.add_argument("--gui", action="store_true", help="Executa a interface Tkinter")
    parser.add_argument("--cli", action="store_true", help="Executa no terminal (sem GUI)")
    parser.add_argument(
        "-m", "--mode",
        type=str,
        default="full",
        choices=["full", "ml_only", "ml_only_retrain"],
        help="Modo de execução do pipeline"
    )

    args = parser.parse_args()

    # Default: GUI
    if args.cli and not args.gui:
        print("--- INICIANDO BÚSSOLA DE FIDCS (SISTEMA INTEGRADO)---")
        run_pipeline(args.mode)
        print("🏁--- PROCESSO FINALIZADO COM SUCESSO ---")
    else:
        run_gui()