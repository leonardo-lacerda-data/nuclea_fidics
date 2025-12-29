# 🧭 Bússola de FIDCs - Análise de Risco de Crédito (FIAP + Núclea)

Este repositório contém a solução de **Engenharia de Dados e Machine Learning** desenvolvida para o Enterprise Challenge da FIAP, em parceria com a **Núclea**.

O objetivo do projeto é prever a inadimplência de títulos (boletos) utilizando uma abordagem de **Lakehouse Oracle**, enriquecida com indicadores macroeconômicos reais (IBGE/Bacen) para calcular o risco sistêmico de diferentes setores da economia.

## 🏗️ Arquitetura do Projeto

O projeto segue um pipeline linear de dados:
1.  **Ingestão:** Dados transacionais (Núclea) e Externos (Gov).
2.  **Armazenamento:** Oracle Database (Cloud).
3.  **Refinaria (Views):** Feature Engineering via SQL para cruzar CNPJ (Setor) com Economia.
4.  **Inteligência:** Modelo de Regressão Logística para Score de Risco.

## 📂 Estrutura dos Arquivos

Os scripts devem ser executados na ordem abaixo para garantir a integridade referencial do banco de dados:

### 1. Infraestrutura (SQL)
* `BF_Cria_Tabelas.sql`: Script DDL. Cria a estrutura das tabelas (`T_BF_BOLETO`, `T_BF_EMPRESA`, `T_BF_MACRO_ECONOMIA`, `T_BF_PREDICOES`) e sequences.
* `BF_Cria_Views.sql`: Script DML. Cria a inteligência do projeto (`V_BF_TREINO_ML`), responsável por cruzar os dados da empresa com os indicadores econômicos da data de vencimento do boleto.

### 2. ETL e Engenharia de Dados (Python/Jupyter)
* `BF_ETL_IBGE_BACEN_Versão_2.ipynb`: **(Executar Primeiro)** Coleta, trata e insere indicadores econômicos reais:
    * **Financeiros:** Selic, Dólar (PTAX), PIB Mensal (IBC-Br).
    * **Setoriais:** Varejo (PMC), Indústria (PIM), Serviços (PMS).
    * **Sociais:** Desemprego e Inflação (IPCA).
* `BF_ETL_Dados_Nuclea_Versão_2_Dados_Falsos.ipynb`: **(Executar Segundo)** Processa a base de boletos da Nuclea.
    * *Nota:* Este script contém uma engine de **Data Augmentation** ("Máquina do Tempo") que distribui os boletos aleatoriamente entre 2023 e 2024 para simular sazonalidade econômica e permitir o aprendizado do modelo.

### 3. Machine Learning
* `BF_ML_Regressão_Logística_Versão_1.ipynb`: Conecta na View do Oracle, treina o modelo preditivo considerando variáveis macroeconômicas e salva o `Score de Risco` e a `Probabilidade de Default` na tabela de predições.

---

## 🚀 Como Executar

1.  **Banco de Dados:** Rode os scripts `.sql` no Oracle SQL Developer para criar tabelas e views.
2.  **Ambiente Python:** Instale as dependências:
    ```bash
    pip install pandas oracledb scikit-learn numpy python-dotenv
    ```
3.  **Carga de Dados:**
    * Execute o notebook `BF_ETL_IBGE_BACEN_Versão_2.ipynb` para popular a tabela macroeconômica.
    * Execute o notebook `BF_ETL_Dados_Nuclea_Versão_2_Dados_Falsos.ipynb` para popular os boletos e simular o histórico.
4.  **Predição:**
    * Execute o notebook `BF_ML_Regressão_Logística_Versão_1.ipynb`.
    * Ao final, consulte a tabela `T_BF_PREDICOES` para ver os resultados.

## 📊 Destaques Técnicos

* **Enriquecimento Macro:** O modelo não olha apenas para o boleto, mas entende se o setor da empresa (Indústria/Comércio/Serviços) está em crise no momento do vencimento.
* **Tratamento Temporal:** Solução para evitar *Look-ahead Bias* usando a lógica `FETCH FIRST 1 ROW ONLY` nas Views SQL, garantindo que o modelo só veja dados disponíveis até a data do vencimento.
* **Data Augmentation:** Algoritmo desenvolvido para transformar um dataset estático em uma série temporal rica para treinamento de IA.
