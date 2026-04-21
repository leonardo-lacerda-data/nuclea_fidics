# 🧭 Bússola de FIDCs - Data Science Challenge 2025

**Parceria:** FIAP & Núclea  
**Turma:** 1TSCOR - Data Science  
**Equipe:** Welcome To The DataFrame

---

## 📖 Sobre o Projeto
O **Bússola de FIDCs** é uma solução de inteligência de dados desenvolvida para transformar a gestão de Fundos de Investimento em Direitos Creditórios (FIDCs). 

O objetivo é superar a visão puramente reativa e tabular do mercado atual, integrando dados internos financeiros com sinais externos (Macroeconomia e Notícias) para antecipar riscos e identificar oportunidades de originação de crédito.

### 🚀 Diferenciais da Solução
1.  **Visão Híbrida de Dados:** Cruzamento de dados transacionais (Boletos/Sacados) com dados públicos do **IBGE (PIB Regional)** e **BACEN (Selic/Inadimplência)**.
2.  **Predição de Risco:** Modelo de Machine Learning (Regressão Logística) que calcula a probabilidade de atraso futuro, não apenas reportando o passado.
3.  **Inteligência de Mercado (NLP):** Monitoramento de sentimento setorial através de Processamento de Linguagem Natural (spaCy).
4.  **Centralização:** Data Warehouse estruturado em Oracle Database alimentando Dashboards no Power BI.

---

## 🛠️ Arquitetura e Tecnologias

O projeto está dividido em três camadas principais, desenvolvidas em paralelo pelo squad:

* **Ingestão & Engenharia (Python + Oracle):** * Limpeza e normalização de dados brutos (`.csv`).
    * Criação de *Target* (Regra de Negócio de Atraso).
    * Persistência em banco relacional (Oracle Database).
* **Analytics & Data Science (Python + Scikit-learn):**
    * Enriquecimento com dados macroeconômicos.
    * Treinamento de modelos preditivos.
    * Análise de sentimento de notícias.
* **Visualização (Power BI):**
    * Dashboards interativos (Visão Executiva, Operacional e Oportunidades).

## 🛠️ Pré-requisitos de Instalação

Para que o projeto funcione (tanto o Script Python quanto o Power BI), você precisa configurar o ambiente abaixo:

### 1. Dependências do Sistema (Obrigatório)
Antes de rodar qualquer coisa, instale o driver que permite a conexão com o Oracle:
* **Oracle Client for Microsoft Tools (64-bit)**
    * [Clique aqui para baixar](https://www.oracle.com/database/technologies/appdev/ocmt.html)
    * **Importante:** Após instalar, reinicie o computador. Sem isso, o Power BI dará erro de "Driver não encontrado".

### 2. Softwares Necessários
* **Microsoft Power BI Desktop** (Para abrir os relatórios visuais)
    * Necessário para visualizar e editar o arquivo `.pbix`.
    * [Download Oficial Microsoft](https://www.microsoft.com/pt-br/download/details.aspx?id=58494)
 
## 🚀 Instalação e Execução

Siga os passos abaixo no seu terminal (Git Bash ou VS Code):

### Passo 1: Clonar o Repositório
Baixe o código para a sua máquina:
```bash
git clone https://github.com/leonardo-lacerda-data/nuclea_fidics.git
```

### Stack Tecnológica
* ![Python](https://img.shields.io/badge/Python-3.9+-blue) **Linguagem Principal** (Pandas, NumPy, Matplotlib).
* ![Oracle](https://img.shields.io/badge/Oracle-Database-red) **Armazenamento** (Driver `oracledb`).
* ![Scikit-Learn](https://img.shields.io/badge/ML-Scikit_Learn-orange) **Machine Learning**.
* ![Power BI](https://img.shields.io/badge/PowerBI-Microsoft-yellow) **Dashboards**.
│   └── dicionario_dados.md  # Explicação das variáveis
│
└── README.md
