# Bússola de FIDCs - Solução Final (Sprint 4)

**Equipe:** Welcome To DataFrame  
**Parceiro:** Núclea | **Instituição:** FIAP  

## 📌 Sobre o Projeto
O **Bússola de FIDCs** é um ecossistema analítico híbrido desenvolvido para transformar o monitoramento de recebíveis de reativo para proativo. A solução integra dados transacionais (histórico de pagamentos) com processamento de linguagem natural (NLP) de notícias setoriais, entregando segmentação e predição de risco explicável.

## 🛠️ Stack Tecnológico
* **Orquestração e ETL:** Python (Pandas)
* **Banco de Dados:** Oracle SQL (Star Schema instanciado dinamicamente via pipeline Python)
* **Machine Learning:** Scikit-Learn (Random Forest, K-Means, DBSCAN)
* **NLP (Análise de Sentimento):** PySentimiento (BERT)
* **Visualização:** Power BI

## 📂 Estrutura do Repositório / Entrega
* `/01_codigo_fonte`: Contém a Interface Gráfica (GUI) e todos os scripts da pipeline de ETL, ingestão no Oracle e treinamento dos modelos de IA.
* `/02_dashboards`: Arquivo `.pbix` contendo as visões executivas de Risco, Oportunidade e Explicabilidade de IA.
* `/03_documentacao_e_pitch`: Apresentação oficial do projeto em PDF detalhando a arquitetura, metodologia e indicadores de negócio.
* `/04_dados_de_amostra`: Base de dados amostral utilizada para o treinamento e validação da Sprint 4.

## 🚀 Como Executar
1. Instale as dependências listadas no projeto (ex: `pip install pandas scikit-learn pysentimiento`).
2. Configure as credenciais de acesso ao banco Oracle nas variáveis de ambiente ou no arquivo de configuração correspondente.
3. Execute o script principal da Interface Gráfica (GUI) para inicializar o painel de controle e acionar a pipeline de ingestão e predição.

---
*Nota Técnica: O agendamento em nuvem está mapeado como próximo passo de arquitetura, sendo a execução atual orquestrada localmente através da GUI desenvolvida pela equipe.*
