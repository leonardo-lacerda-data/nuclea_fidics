import os
import sys
import threading
import customtkinter as ctk
from tkinter import scrolledtext

# Importando seus módulos originais
from src.setup_tables import recriar_banco_dados
from src.setup_views import atualizar_view_ml, atualizar_view_pbi
from src.ml_cluster import segmentar_clientes
from src.ml_risk import calcular_risco_credito
from src.etl_api import carregar_api
from src.etl_ingestion import carregar_dados
from src.etl_nlp import executar_etl_noticias


class TextRedirector:
    """Classe auxiliar para jogar os prints do terminal para a telinha da GUI"""

    def __init__(self, textbox):
        self.textbox = textbox

    def write(self, s):
        if not s: return

        def append():
            try:
                self.textbox.insert('end', s)
                self.textbox.see('end')
            except:
                pass

        try:
            self.textbox.after(0, append)
        except:
            pass

    def flush(self):
        return


class BussolaGUI(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Bússola FIDC — Painel de Controle")
        self.geometry("800x600")

        ctk.set_appearance_mode("System")
        ctk.set_default_color_theme("blue")

        # --- Área de Credenciais ---
        frame = ctk.CTkFrame(master=self)
        frame.pack(padx=20, pady=20, fill="both", expand=True)

        ctk.CTkLabel(frame, text="Credenciais do Banco (Oracle)", font=ctk.CTkFont(size=18, weight="bold")).grid(row=0,
                                                                                                                 column=0,
                                                                                                                 columnspan=2,
                                                                                                                 pady=(
                                                                                                                     10,
                                                                                                                     15))

        # Campos de texto
        self._criar_campo(frame, 1, "Usuário:", "entry_user")
        self._criar_campo(frame, 2, "Senha:", "entry_password", show="*")
        self._criar_campo(frame, 3, "Host (IP):", "entry_host", placeholder="oracle.fiap.com.br")
        self._criar_campo(frame, 4, "Porta:", "entry_port", placeholder="1521")
        self._criar_campo(frame, 5, "Service/SID:", "entry_service", placeholder="ORCL")

        # --- Botões de Ação ---
        btn_frame = ctk.CTkFrame(master=frame, fg_color="transparent")
        btn_frame.grid(row=6, column=0, columnspan=2, pady=(20, 10), sticky="ew")

        self.btn_create = ctk.CTkButton(btn_frame, text="1. Criar Tabelas", command=self._wrap(self.create_tables),
                                        fg_color="#D35400", hover_color="#A04000")
        self.btn_create.pack(side="left", padx=5, expand=True)

        self.btn_read = ctk.CTkButton(btn_frame, text="2. Ingerir Dados", command=self._wrap(self.read_data))
        self.btn_read.pack(side="left", padx=5, expand=True)

        self.btn_news = ctk.CTkButton(btn_frame, text="3. Buscar Notícias", command=self._wrap(self.read_news))
        self.btn_news.pack(side="left", padx=5, expand=True)

        self.btn_process = ctk.CTkButton(btn_frame, text="4. Rodar IA / ML", command=self._wrap(self.process_ml),
                                         fg_color="#27AE60", hover_color="#1E8449")
        self.btn_process.pack(side="left", padx=5, expand=True)

        # --- Barra de Progresso e Status ---
        self.progress = ctk.CTkProgressBar(master=frame)
        self.progress.grid(row=7, column=0, columnspan=2, sticky="ew", padx=20, pady=(10, 5))
        self.progress.set(0)

        self.status_label = ctk.CTkLabel(frame, text="Aguardando comando...", text_color="gray")
        self.status_label.grid(row=8, column=0, columnspan=2, sticky="w", padx=20, pady=(0, 10))

        frame.grid_columnconfigure(1, weight=1)

        # --- Console de Saída (Logs) ---
        ctk.CTkLabel(frame, text="Log de Execução:", font=ctk.CTkFont(size=12, weight="bold")).grid(row=9, column=0,
                                                                                                    sticky="w", padx=20)

        self.output_text = scrolledtext.ScrolledText(frame, height=12, bg="#2B2B2B", fg="#FFFFFF",
                                                     font=("Consolas", 10))
        self.output_text.grid(row=10, column=0, columnspan=2, sticky="nsew", padx=20, pady=(0, 10))

        frame.grid_rowconfigure(10, weight=1)

        # Botão limpar log
        ctk.CTkButton(frame, text="Limpar Log", height=20, fg_color="gray",
                      command=lambda: self.output_text.delete('1.0', 'end')).grid(row=11, column=1, sticky="e", padx=20,
                                                                                  pady=(0, 10))

    def _criar_campo(self, parent, row, label, attr_name, show=None, placeholder=""):
        ctk.CTkLabel(parent, text=label).grid(row=row, column=0, sticky="w", padx=20, pady=5)
        entry = ctk.CTkEntry(parent, show=show, placeholder_text=placeholder)
        entry.grid(row=row, column=1, sticky="ew", padx=20, pady=5)
        setattr(self, attr_name, entry)

    def _wrap(self, func):
        """Envelopa a função para rodar em Thread separada e não travar a janela"""

        def inner():
            # Trava botões
            botoes = [self.btn_create, self.btn_read, self.btn_news, self.btn_process]
            for b in botoes: b.configure(state="disabled")

            t = threading.Thread(target=self._run_and_finalize, args=(func, botoes))
            t.start()

        return inner

    def _run_and_finalize(self, func, botoes):
        # Redireciona print() para o widget de texto
        old_stdout, old_stderr = sys.stdout, sys.stderr
        sys.stdout = sys.stderr = TextRedirector(self.output_text)

        try:
            self._apply_credentials_to_env()  # <--- APLICA AS CREDENCIAIS AQUI
            print(f"\n--- ▶️ Iniciando: {func.__name__} ---")
            func()
            print(f"--- ✅ Finalizado: {func.__name__} ---")
            self._update_status("Concluído com sucesso!")
            self._set_progress(1.0)
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO: {e}")
            self._update_status("Erro na execução (veja o log).")
            self._set_progress(0)
        finally:
            sys.stdout, sys.stderr = old_stdout, old_stderr
            self.after(0, lambda: [b.configure(state="normal") for b in botoes])

    def _update_status(self, text):
        self.after(0, lambda: self.status_label.configure(text=text))

    def _set_progress(self, value):
        self.after(0, lambda: self.progress.set(value))

    # =========================================================================
    # AQUI ESTÁ A CORREÇÃO PRINCIPAL DE CONEXÃO 🛠️
    # =========================================================================
    def _apply_credentials_to_env(self):
        """Lê os campos da tela e configura as variáveis que o db_connection.py espera."""
        user = self.entry_user.get().strip()
        pwd = self.entry_password.get().strip()
        host = self.entry_host.get().strip()
        port = self.entry_port.get().strip()
        sid = self.entry_service.get().strip()

        # Se o usuário preencheu, sobrescreve o .env
        if user: os.environ["ORACLE_USER"] = user
        if pwd: os.environ["ORACLE_PASSWORD"] = pwd

        # Reconstrói o DSN (Data Source Name) no formato que o oracledb espera
        if host and port and sid:
            # Formato padrão: host:port/service_name
            dsn_montado = f"{host}:{port}/{sid}"
            os.environ["ORACLE_DSN"] = dsn_montado
            print(f"🔧 Configurando conexão para: {host}:{port}/{sid}")

    # --- Funções dos Botões ---
    def create_tables(self):
        self._update_status("Recriando tabelas no Oracle...")
        self._set_progress(0.2)
        recriar_banco_dados()

    def read_data(self):
        self._update_status("Carregando CSVs e APIs...")
        self._set_progress(0.2)
        carregar_dados()  # CSVs
        self._set_progress(0.6)
        carregar_api()  # Selic/Dólar

    def read_news(self):
        self._update_status("Buscando notícias (Isso pode demorar)...")
        self._set_progress(0.1)
        executar_etl_noticias()

    def process_ml(self):
        self._update_status("Treinando Robôs de Risco e Cluster...")
        self._set_progress(0.1)
        atualizar_view_ml()
        self._set_progress(0.3)
        calcular_risco_credito(force_retrain=True)
        self._set_progress(0.6)
        segmentar_clientes(force_retrain=True)
        self._set_progress(0.9)
        atualizar_view_pbi()

def run_gui():
    app = BussolaGUI()
    app.mainloop()

if __name__ == "__main__":
    app = BussolaGUI()
    app.mainloop()