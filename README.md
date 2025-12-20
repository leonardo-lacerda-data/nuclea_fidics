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

### Stack Tecnológica
* ![Python](https://img.shields.io/badge/Python-3.9+-blue) **Linguagem Principal** (Pandas, NumPy, Matplotlib).
* ![Oracle](https://img.shields.io/badge/Oracle-Database-red) **Armazenamento** (Driver `oracledb`).
* ![Scikit-Learn](https://img.shields.io/badge/ML-Scikit_Learn-orange) **Machine Learning**.
* ![Power BI](https://img.shields.io/badge/PowerBI-Microsoft-yellow) **Dashboards**.
│   └── dicionario_dados.md  # Explicação das variáveis
│
└── README.md
