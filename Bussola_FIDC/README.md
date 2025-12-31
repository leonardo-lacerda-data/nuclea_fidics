# 🧭 Bússola FIDC - Sistema Inteligente de Crédito

Projeto de Engenharia de Dados e Machine Learning para análise de risco e concessão de crédito em FIDCs.

## 🚀 Funcionalidades
1. **Infraestrutura Automática**: Criação de tabelas e views no Oracle via Python.
2. **ETL Híbrido**: Ingestão de CSVs e Crawler de Notícias (Google News RSS).
3. **NLP (Processamento de Linguagem Natural)**: Análise de sentimento de notícias (BERT) para compor o risco.
4. **Machine Learning**:
   - Classificação de Risco (Regressão Logística).
   - Clusterização de Clientes (K-Means).

## 🛠️ Tecnologias
- Python 3.10+
- Oracle Database
- Bibliotecas: Pandas, Spacy, Scikit-Learn, PySentimiento, OracleDB.

## ▶️ Como Rodar
1. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   python -m spacy download pt_core_news_sm
