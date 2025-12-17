import os
import sys
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
import customtkinter as ctk

ctk.set_appearance_mode("System")
ctk.set_default_color_theme("blue")

def read_csv_file(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)

        # Conferência defensiva da coluna
        if "dt_pagamento" in df.columns:
            df["tipo_baixa"] = df["dt_pagamento"].isnull().map(
                {True: "Não pago", False: "Pago"}
            )
        else:
            print(f"Aviso: coluna 'dt_pagamento' não encontrada em {path}")
            df["tipo_baixa"] = "Desconhecido"

        return df

    except Exception as e:
        print(f"Falha ao ler {path}: {e}")
        return pd.DataFrame()


class DataIngestionApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Importação de CSV - Ingestão")
        self.geometry("800x600")

        self.selected_files = []
        self.dataframes = []

        # Layout
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=2)
        self.grid_rowconfigure(0, weight=1)

        left_frame = ctk.CTkFrame(self)
        left_frame.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        right_frame = ctk.CTkFrame(self)
        right_frame.grid(row=0, column=1, sticky="nsew", padx=10, pady=10)

        # Left: controls
        self.btn_import = ctk.CTkButton(left_frame, text="Importar arquivos", command=self.select_files)
        self.btn_import.pack(pady=(10, 6), padx=10, fill="x")


        self.lbl_status = ctk.CTkLabel(left_frame, text="Nenhum arquivo selecionado.")
        self.lbl_status.pack(pady=6, padx=10)

        self.files_box = ctk.CTkTextbox(left_frame, width=300, height=300)
        self.files_box.pack(pady=6, padx=10, fill="both", expand=True)

        # Right: preview
        self.preview_label = ctk.CTkLabel(right_frame, text="Preview (primeiras linhas)")
        self.preview_label.pack(pady=(10, 6))

        self.preview_box = ctk.CTkTextbox(right_frame, width=600, height=500)
        self.preview_box.pack(padx=10, pady=6, fill="both", expand=True)

        # Bind double-click on files_box to preview first file
        self.files_box.bind("<Double-Button-1>", lambda e: self.preview_first())

    def select_files(self):
        paths = filedialog.askopenfilenames(title="Selecione arquivos CSV", filetypes=[("CSV files", "*.csv")])
        if not paths:
            return
        self.selected_files = list(paths)
        self.files_box.delete("0.0", "end")
        self.dataframes = []
        for p in self.selected_files:
            self.files_box.insert("end", p + "\n")
            df = read_csv_file(p)
            self.dataframes.append((p, df))

        self.lbl_status.configure(text=f"{len(self.selected_files)} arquivo(s) selecionado(s)")
        # Show preview of first non-empty
        self.preview_first()

    def preview_first(self):
        self.preview_box.delete("0.0", "end")
        for p, df in self.dataframes:
            if not df.empty:
                self.preview_box.insert("end", f"Arquivo: {p}\n")
                self.preview_box.insert("end", df.head(10).to_string(index=False))
                self.preview_box.insert("end", "\n\nDtypes:\n")
                self.preview_box.insert("end", df.dtypes.to_string())
                return
        self.preview_box.insert("end", "Nenhum DataFrame válido para pré-visualização.")


def main():
    app = DataIngestionApp()
    app.mainloop()

if __name__ == "__main__":
    main()
