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

## 📊 O Dashboard

A solução entrega três visões conectadas no Power BI, cada uma desenhada para um momento de decisão diferente.

🔗 **[Acessar o dashboard interativo](https://app.powerbi.com/view?r=eyJrIjoiMjJkNDE1NmYtYjhiYi00YzliLWJmZjctOTJiOWE2MjI5YzZiIiwidCI6IjExZGJiZmUyLTg5YjgtNDU0OS1iZTEwLWNlYzM2NGU1OTU1MSIsImMiOjR9)**

### 1. Painel de Controle de Risco — *onde está o risco?*

Visão executiva da carteira. Mapa coroplético de inadimplência por UF, distribuição da carteira em faixas de risco (Baixo / Médio / Alto) calculadas pelo modelo preditivo, e a relação entre média de dias de atraso e taxa de inadimplência ao longo do tempo. Os KPIs à direita consolidam volume total, score médio de risco, % de inadimplência e o índice de sentimento de mercado.

![Painel de Controle de Risco](docs/img/dashboard-01-controle-risco.png)

### 2. Visão de Oportunidades — *onde originar crédito?*

Inverte a leitura: em vez de mostrar perda, mapeia potencial. Traz o volume mapeado como oportunidade, o ranking de sentimento setorial extraído por NLP a partir de notícias, e uma nuvem de tendências do mercado. É a camada que permite decidir em quais setores e regiões expandir a originação.

![Visão de Oportunidades](docs/img/dashboard-02-oportunidades.png)

### 3. Visão Operacional — *o que fazer agora?*

Nível de cliente. Monitoramento de risco por sacado ordenado por score, com um painel de alerta que gera a leitura contextual do caso — cruzando o score de risco calculado com o sentimento externo e os indicadores econômicos do período. Fecha o ciclo: do panorama da carteira até a ação sobre um sacado específico.

![Visão Operacional](docs/img/dashboard-03-operacional.png)

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
    * [Dashboard interativo](https://app.powerbi.com/view?r=eyJrIjoiMjJkNDE1NmYtYjhiYi00YzliLWJmZjctOTJiOWE2MjI5YzZiIiwidCI6IjExZGJiZmUyLTg5YjgtNDU0OS1iZTEwLWNlYzM2NGU1OTU1MSIsImMiOjR9) (Visão Executiva, Operacional e Oportunidades).

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
git clone [https://github.com/leonardo-lacerda-data/nuclea_fidics.git](https://github.com/leonardo-lacerda-data/nuclea_fidics.git)
cd nuclea_fidics
git clone https://github.com/leonardo-lacerda-data/nuclea_fidics.git
```

### Stack Tecnológica
* ![Python](https://img.shields.io/badge/Python-3.9+-blue) **Linguagem Principal** (Pandas, NumPy, Scikit-Learn).
* ![Oracle](https://img.shields.io/badge/Oracle-Database-red) **Armazenamento** (Driver `oracledb`).
* ![Scikit-Learn](https://img.shields.io/badge/ML-Scikit_Learn-orange) **Machine Learning**.
* ![Power BI](https://img.shields.io/badge/PowerBI-Microsoft-yellow) **Dashboards**.
│   └── dicionario_dados.md  # Explicação das variáveis
│
└── README.md
