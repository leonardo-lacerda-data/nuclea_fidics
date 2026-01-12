import os
import sys
import threading
import time
import customtkinter as ctk
from tkinter import messagebox, scrolledtext


class TextRedirector:
    def __init__(self, textbox):
        self.textbox = textbox

    def write(self, s):
        if not s:
            return

        def append():
            try:
                self.textbox.insert('end', s)
                self.textbox.see('end')
            except Exception:
                pass

        try:
            self.textbox.after(0, append)
        except Exception:
            pass

    def flush(self):
        return

from src.setup_tables import recriar_banco_dados
from src.setup_views import atualizar_view_ml, atualizar_view_pbi
from src.ml_cluster import segmentar_clientes
from src.ml_risk import calcular_risco_credito
from src.etl_api import carregar_api
from src.etl_ingestion import carregar_dados
from src.etl_nlp import executar_etl_noticias


class BussolaGUI(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Bússola FIDC — Interface")
        self.geometry("720x480")

        ctk.set_appearance_mode("System")
        ctk.set_default_color_theme("blue")

        # Credenciais frame
        frame = ctk.CTkFrame(master=self)
        frame.pack(padx=20, pady=20, fill="both", expand=True)

        ctk.CTkLabel(frame, text="Credenciais OracleDB", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=(0, 10))

        ctk.CTkLabel(frame, text="Usuário:").grid(row=1, column=0, sticky="w", padx=8, pady=4)
        self.entry_user = ctk.CTkEntry(frame)
        self.entry_user.grid(row=1, column=1, sticky="ew", padx=8, pady=4)

        ctk.CTkLabel(frame, text="Senha:").grid(row=2, column=0, sticky="w", padx=8, pady=4)
        self.entry_password = ctk.CTkEntry(frame, show="*")
        self.entry_password.grid(row=2, column=1, sticky="ew", padx=8, pady=4)

        ctk.CTkLabel(frame, text="Host:").grid(row=3, column=0, sticky="w", padx=8, pady=4)
        self.entry_host = ctk.CTkEntry(frame)
        self.entry_host.grid(row=3, column=1, sticky="ew", padx=8, pady=4)

        ctk.CTkLabel(frame, text="Porta:").grid(row=4, column=0, sticky="w", padx=8, pady=4)
        self.entry_port = ctk.CTkEntry(frame)
        self.entry_port.grid(row=4, column=1, sticky="ew", padx=8, pady=4)

        ctk.CTkLabel(frame, text="Service Name / SID:").grid(row=5, column=0, sticky="w", padx=8, pady=4)
        self.entry_service = ctk.CTkEntry(frame)
        self.entry_service.grid(row=5, column=1, sticky="ew", padx=8, pady=4)

        # Buttons
        btn_frame = ctk.CTkFrame(master=frame)
        btn_frame.grid(row=6, column=0, columnspan=2, pady=(12, 0), sticky="ew")

        self.btn_create = ctk.CTkButton(btn_frame, text="Criar Tabelas", command=self._wrap(self.create_tables))
        self.btn_create.grid(row=0, column=0, padx=8, pady=6)

        self.btn_read = ctk.CTkButton(btn_frame, text="Ler Dados", command=self._wrap(self.read_data))
        self.btn_read.grid(row=0, column=1, padx=8, pady=6)

        self.btn_news = ctk.CTkButton(btn_frame, text="Buscar Notícias", command=self._wrap(self.read_news))
        self.btn_news.grid(row=0, column=2, padx=8, pady=6)

        self.btn_process = ctk.CTkButton(btn_frame, text="Processar Dados (Machine Learning)", command=self._wrap(self.process_ml))
        self.btn_process.grid(row=0, column=3, padx=8, pady=6)

        # Progress and status
        self.progress = ctk.CTkProgressBar(master=frame)
        self.progress.grid(row=7, column=0, columnspan=2, sticky="ew", padx=8, pady=(16, 4))
        self.progress.set(0)

        self.status_label = ctk.CTkLabel(frame, text="Pronto.")
        self.status_label.grid(row=8, column=0, columnspan=2, sticky="w", padx=8, pady=(4, 8))

        frame.grid_columnconfigure(1, weight=1)

        # Output area (Saída)
        ctk.CTkLabel(frame, text="Saída:", font=ctk.CTkFont(size=12, weight="bold")).grid(row=9, column=0, sticky="w", padx=8, pady=(8, 2))
        self.output_text = scrolledtext.ScrolledText(frame, height=10, wrap='word')
        self.output_text.grid(row=10, column=0, columnspan=2, sticky="nsew", padx=8, pady=(0, 8))
        frame.grid_rowconfigure(10, weight=1)

        # Small clear button for output
        self.btn_clear_output = ctk.CTkButton(frame, text="Limpar Saída", command=lambda: self._clear_output())
        self.btn_clear_output.grid(row=11, column=1, sticky="e", padx=8, pady=(0, 8))

    def _wrap(self, func):
        def inner():
            # desabilitar botões enquanto roda
            for b in (self.btn_create, self.btn_read, self.btn_news, self.btn_process):
                b.configure(state="disabled")
            t = threading.Thread(target=self._run_and_finalize, args=(func,))
            t.start()

        return inner

    def _run_and_finalize(self, func):
        # Redireciona stdout/stderr para a área de saída enquanto a função roda
        real_stdout = sys.stdout
        real_stderr = sys.stderr
        redirector = TextRedirector(self.output_text)
        sys.stdout = redirector
        sys.stderr = redirector
        try:
            # marca início no painel de saída
            self.after(0, lambda: self.output_text.insert('end', f"--- Iniciando {getattr(func,'__name__', 'tarefa')}... ---\n"))
            func()
        except Exception as e:
            print(f"Erro: {e}")
            self._update_status(f"Erro: {e}")
        finally:
            # restaura stdout/stderr e habilita botões
            sys.stdout = real_stdout
            sys.stderr = real_stderr
            self.after(0, lambda: [b.configure(state="normal") for b in (self.btn_create, self.btn_read, self.btn_news, self.btn_process)])
            self.after(0, lambda: self.progress.set(0))

    def _update_status(self, text: str):
        self.after(0, lambda: self.status_label.configure(text=text))

    def _set_progress(self, value: float):
        # value between 0 and 1
        self.after(0, lambda: self.progress.set(value))

    def _apply_credentials_to_env(self):
        # Ajusta variáveis de ambiente para que os módulos usem automaticamente
        user = self.entry_user.get().strip()
        password = self.entry_password.get().strip()
        host = self.entry_host.get().strip()
        port = self.entry_port.get().strip()
        service = self.entry_service.get().strip()
        if user:
            os.environ["ORA_USER"] = user
        if password:
            os.environ["ORA_PASSWORD"] = password
        if host:
            os.environ["ORA_HOST"] = host
        if port:
            os.environ["ORA_PORT"] = port
        if service:
            os.environ["ORA_SERVICE"] = service

    def _clear_output(self):
        try:
            self.output_text.delete('1.0', 'end')
        except Exception:
            pass

    def create_tables(self):
        self._update_status("Criando tabelas...")
        self._set_progress(0.05)
        self._apply_credentials_to_env()
        try:
            recriar_banco_dados()
            self._set_progress(1.0)
            self._update_status("Criação de tabelas concluída com sucesso.")
        except Exception as e:
            self._update_status(f"Erro criando tabelas: {e}")
            messagebox.showerror("Erro", f"Erro criando tabelas: {e}")

    def read_data(self):
        self._update_status("Lendo dados...")
        self._set_progress(0.05)
        self._apply_credentials_to_env()
        try:
            carregar_dados()
            self._set_progress(0.6)
            carregar_api()
            self._set_progress(1.0)
            self._update_status("Leitura de dados concluída.")
        except Exception as e:
            self._update_status(f"Erro lendo dados: {e}")
            messagebox.showerror("Erro", f"Erro lendo dados: {e}")

    def read_news(self):
        self._update_status("Lendo notícias...")
        self._set_progress(0.05)
        self._apply_credentials_to_env()
        try:
            executar_etl_noticias()
            self._set_progress(1.0)
            self._update_status("Leitura de notícias concluída.")
        except Exception as e:
            self._update_status(f"Erro lendo notícias: {e}")
            messagebox.showerror("Erro", f"Erro lendo notícias: {e}")

    def process_ml(self):
        self._update_status("Processando ML (resposta)...")
        self._set_progress(0.02)
        self._apply_credentials_to_env()
        try:
            atualizar_view_ml()
            self._set_progress(0.2)
            # treinar/atualizar modelos
            calcular_risco_credito(force_retrain=True)
            self._set_progress(0.6)
            segmentar_clientes(force_retrain=True)
            self._set_progress(0.9)
            atualizar_view_pbi()
            self._set_progress(1.0)
            self._update_status("Processamento ML concluído.")
        except Exception as e:
            self._update_status(f"Erro no processamento ML: {e}")
            messagebox.showerror("Erro", f"Erro no processamento ML: {e}")


def run():
    app = BussolaGUI()
    app.mainloop()


if __name__ == "__main__":
    run()
