# 🏗️ Módulo de Engenharia de Dados - Bússola de FIDCs

Este diretório contém os scripts responsáveis pela **Ingestão, Tratamento (ETL) e Persistência** dos dados transacionais da parceria **FIAP + Núclea**.

O objetivo deste pipeline é transformar arquivos brutos (`.csv`) em tabelas estruturadas no Oracle Database, criando a "Fonte de Verdade" para os modelos de Machine Learning e Dashboards do projeto.

---

## 📂 Arquivos do Repositório

| Arquivo | Descrição |
| :--- | :--- |
| `cria_tabelas_bussola.sql` | **DDL (Data Definition Language):** Script SQL que cria a estrutura do banco de dados, definindo chaves primárias, estrangeiras e constraints de validação (ex: UF válida, Flag 0/1). |
| `Ingestao_Tratamento_Dados_Nuclea.ipynb` | **ETL Pipeline:** Jupyter Notebook que lê os CSVs, aplica regras de negócio, limpa inconsistências e realiza a carga em lote (*Bulk Insert*) no Oracle. |

---

## ⚙️ Regras de Negócio e Tratamento de Dados

Durante o processo de ETL, foram aplicadas as seguintes regras para garantir a integridade da análise de risco:

### 1. Cálculo de Atraso e Target (Alvo)
A variável alvo para o modelo de risco (`alvo_inadimplencia`) foi calculada na engenharia para garantir consistência entre DS e BI:
* **Boletos Pagos:** `Data Pagamento - Data Vencimento`.
* **Boletos em Aberto:** `Data Atual (Hoje) - Data Vencimento`.
* **Regra:** Se `Dias de Atraso > 0`, o boleto é marcado como **Inadimplente (1)**. Caso contrário, **Em dia (0)**.

### 2. Saneamento de Valores Monetários
* Campos com formatação de texto (ex: `R$ 1.200,50`) foram convertidos para `FLOAT`.
* **Baixa Nula:** Registros com "Tipo de Baixa" mas sem "Valor de Baixa" foram preenchidos com **0.00**, assumindo-se baixa contábil (devolução/cancelamento) e não financeira.

### 3. Tratamento de Localidade (Geospatial)
* Empresas sem UF informada na base auxiliar não foram descartadas para preservar seus Scores de Crédito.
* **Imputação:** O campo UF foi preenchido com a sigla **'ND'** (Não Definido), permitindo análise segregada no Dashboard.

---

## 🚀 Como Executar

### Pré-requisitos
* Python 3.x
* Bibliotecas: `pandas`, `numpy`, `oracledb`
* Acesso a uma instância Oracle Database.

### Passo a Passo
1.  **Preparação do Banco:**
    Execute o script `cria_tabelas_bussola_fidics.sql` no seu cliente Oracle (SQL Developer, DBeaver, etc) para criar as tabelas `T_BF_EMPRESA` e `T_BF_BOLETO`.

2.  **Execução do Pipeline:**
    Abra o notebook `Ingestao_Tratamento_Dados_Nuclea.ipynb`. Certifique-se de que os arquivos `base_boletos.csv` e `base_auxiliar.csv` estejam no mesmo diretório (ou ajustados no caminho do código).
    
3.  **Configuração de Credenciais:**
    No notebook, ajuste as variáveis `db_user`, `db_pass` e `db_dsn` com suas credenciais Oracle.

4.  **Run All:**
    Execute todas as células. O script finalizará com a mensagem:
    > `✅ CARGA FINALIZADA COM SUCESSO!`

---

## 📊 Estrutura do Banco de Dados (Schema)

* **T_BF_BOLETO:** Tabela Fato contendo as transações, datas, valores e flags de atraso.
* **T_BF_EMPRESA:** Tabela Dimensão contendo dados cadastrais, CNAE e Scores de Liquidez/Maturidade da Núclea.

---
*Desenvolvido pela equipe Welcome To The DataFrame - FIAP 2025*
