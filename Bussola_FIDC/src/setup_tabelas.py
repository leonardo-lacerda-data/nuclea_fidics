import oracledb
from src.database import get_connection


def executar_ddl(cursor, sql, mensagem):
    try:
        cursor.execute(sql)
        print(f"   ✅ {mensagem}")
    except oracledb.DatabaseError as e:
        error, = e.args
        # Erro 942 = Tabela não existe (ignoramos no DROP)
        # Erro 2289 = Sequence não existe
        if error.code in [942, 2289]:
            print(f"   ℹ️ {mensagem} (Objeto não existia)")
        else:
            print(f"   ❌ Erro ao {mensagem}: {e}")


def recriar_banco_dados():
    print("\n🏗️ [SETUP] Recriando Estrutura do Banco de Dados...")
    conn = get_connection()
    if not conn: return

    cursor = conn.cursor()

    # =========================================================================
    # 1. LIMPEZA (DROPS)
    # =========================================================================
    print("\n   --- Limpando Ambiente ---")
    executar_ddl(cursor, "DROP TABLE T_BF_PREDICOES CASCADE CONSTRAINTS", "Drop T_BF_PREDICOES")
    executar_ddl(cursor, "DROP TABLE T_BF_NOTICIAS CASCADE CONSTRAINTS", "Drop T_BF_NOTICIAS")
    executar_ddl(cursor, "DROP TABLE T_BF_MACRO_ECONOMIA CASCADE CONSTRAINTS", "Drop T_BF_MACRO_ECONOMIA")
    executar_ddl(cursor, "DROP TABLE T_BF_CLUSTER CASCADE CONSTRAINTS", "Drop T_BF_CLUSTER")
    executar_ddl(cursor, "DROP TABLE T_BF_BOLETO CASCADE CONSTRAINTS", "Drop T_BF_BOLETO")
    executar_ddl(cursor, "DROP TABLE T_BF_EMPRESA CASCADE CONSTRAINTS", "Drop T_BF_EMPRESA")

    executar_ddl(cursor, "DROP SEQUENCE SQ_BF_PREDICOES", "Drop SQ_BF_PREDICOES")
    executar_ddl(cursor, "DROP SEQUENCE SQ_BF_NOTICIAS", "Drop SQ_BF_NOTICIAS")
    executar_ddl(cursor, "DROP SEQUENCE SQ_BF_MACRO_ECONOMIA", "Drop SQ_BF_MACRO_ECONOMIA")

    # =========================================================================
    # 2. SEQUENCES
    # =========================================================================
    print("\n   --- Criando Sequences ---")
    executar_ddl(cursor, """
        CREATE SEQUENCE SQ_BF_MACRO_ECONOMIA START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE
    """, "Sequence Macro")

    executar_ddl(cursor, """
        CREATE SEQUENCE SQ_BF_NOTICIAS START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE
    """, "Sequence Notícias")

    executar_ddl(cursor, """
        CREATE SEQUENCE SQ_BF_PREDICOES START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE
    """, "Sequence Predições")

    # =========================================================================
    # 3. TABELAS
    # =========================================================================
    print("\n   --- Criando Tabelas ---")

    # EMPRESA
    sql_empresa = """
                  CREATE TABLE T_BF_EMPRESA \
                  ( \
                      id_empresa             VARCHAR2(64) NOT NULL, \
                      cd_cnae                VARCHAR2(7) NOT NULL, \
                      sg_uf                  CHAR(2) NOT NULL, \
                      vl_score_materialidade NUMBER(10,2), \
                      vl_score_quantidade    NUMBER(10,2), \
                      CONSTRAINT PK_BF_EMPRESA PRIMARY KEY (id_empresa), \
                      CONSTRAINT CK_BF_EMPRESA_SG_UF CHECK (sg_uf IN \
                                                            ('AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', \
                                                             'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', \
                                                             'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO', 'ND', \
                                                             'BR'))
                  ) \
                  """
    executar_ddl(cursor, sql_empresa, "Tabela T_BF_EMPRESA")

    # BOLETO
    sql_boleto = """
                 CREATE TABLE T_BF_BOLETO \
                 ( \
                     id_boleto        VARCHAR2(64) NOT NULL, \
                     id_pagador       VARCHAR2(64) NOT NULL, \
                     id_beneficiario  VARCHAR2(64) NOT NULL, \
                     vl_nominal       NUMBER(15,2) NOT NULL, \
                     vl_baixa         NUMBER(15,2), \
                     dt_emissao       DATE NOT NULL, \
                     dt_vencimento    DATE NOT NULL, \
                     dt_pagamento     DATE, \
                     tp_baixa         VARCHAR2(100), \
                     nr_dias_atraso   NUMBER(5) NOT NULL, \
                     vl_inadimplencia NUMBER(1) NOT NULL, \
                     CONSTRAINT PK_BF_BOLETO PRIMARY KEY (id_boleto), \
                     CONSTRAINT FK_BF_BOLETO_EMPRESA FOREIGN KEY (id_pagador) REFERENCES T_BF_EMPRESA (id_empresa), \
                     CONSTRAINT CK_BF_VL_INADIMPLENCIA CHECK (vl_inadimplencia IN (0, 1))
                 ) \
                 """
    executar_ddl(cursor, sql_boleto, "Tabela T_BF_BOLETO")

    # CLUSTER
    sql_cluster = """
                  CREATE TABLE T_BF_CLUSTER \
                  ( \
                      id_boleto        VARCHAR2(64) NOT NULL, \
                      dt_processamento DATE DEFAULT SYSDATE, \
                      nr_cluster       NUMBER(2) NOT NULL, \
                      ds_perfil        VARCHAR2(100) NOT NULL, \
                      CONSTRAINT PK_BF_CLUSTER PRIMARY KEY (id_boleto), \
                      CONSTRAINT FK_BF_CLUSTER_BOLETO FOREIGN KEY (id_boleto) REFERENCES T_BF_BOLETO (id_boleto)
                  ) \
                  """
    executar_ddl(cursor, sql_cluster, "Tabela T_BF_CLUSTER")

    # MACRO ECONOMIA
    sql_macro = """
                CREATE TABLE T_BF_MACRO_ECONOMIA \
                ( \
                    id_macro_economia NUMBER(10) DEFAULT SQ_BF_MACRO_ECONOMIA.NEXTVAL NOT NULL, \
                    sg_uf             CHAR(2) NOT NULL, \
                    dt_referencia     DATE    NOT NULL, \
                    nm_indicador      VARCHAR2(50) NOT NULL, \
                    vl_indicador      NUMBER(20, 6) NOT NULL, \
                    CONSTRAINT PK_BF_MACRO_ECONOMIA PRIMARY KEY (id_macro_economia), \
                    CONSTRAINT CK_BF_MACRO_SG_UF CHECK (sg_uf IN ('AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', \
                                                                  'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', \
                                                                  'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO', \
                                                                  'ND', 'BR'))
                ) \
                """
    executar_ddl(cursor, sql_macro, "Tabela T_BF_MACRO_ECONOMIA")

    # NOTICIAS
    sql_noticias = """
                   CREATE TABLE T_BF_NOTICIAS \
                   ( \
                       id_noticia    NUMBER(10) DEFAULT SQ_BF_NOTICIAS.NEXTVAL NOT NULL, \
                       ds_setor      VARCHAR2(20) NOT NULL, \
                       tx_titulo     VARCHAR2(400), \
                       vl_sentimento NUMBER(5,2), \
                       dt_publicacao DATE DEFAULT SYSDATE, \
                       tx_link       VARCHAR2(500), \
                       CONSTRAINT PK_BF_NOTICIAS PRIMARY KEY (id_noticia), \
                       CONSTRAINT CK_BF_NOTICIAS_VL_SENTIMENTO CHECK (vl_sentimento >= -1.00 AND vl_sentimento <= 1.00)
                   ) \
                   """
    executar_ddl(cursor, sql_noticias, "Tabela T_BF_NOTICIAS")

    # PREDICOES
    sql_predicoes = """
                    CREATE TABLE T_BF_PREDICOES \
                    ( \
                        id_predicao                    NUMBER(10) DEFAULT SQ_BF_PREDICOES.NEXTVAL NOT NULL, \
                        id_boleto                      VARCHAR2(64) NOT NULL, \
                        dt_processamento               DATE DEFAULT SYSDATE, \
                        vl_probabilidade_inadimplencia NUMBER(5, 4), \
                        st_faixa_risco                 VARCHAR2(20), \
                        ds_principal                   VARCHAR2(100), \
                        CONSTRAINT UN_BF_PREDICOES UNIQUE (id_boleto, dt_processamento), \
                        CONSTRAINT FK_BF_PREDICOES_BOLETO FOREIGN KEY (id_boleto) REFERENCES T_BF_BOLETO (id_boleto), \
                        CONSTRAINT CK_BF_PRED_ST_RISCO CHECK (st_faixa_risco IN ('ALTO', 'MEDIO', 'BAIXO'))
                    ) \
                    """
    executar_ddl(cursor, sql_predicoes, "Tabela T_BF_PREDICOES")

    conn.commit()
    conn.close()
    print("\n✅ Estrutura de Banco de Dados finalizada com sucesso!")


if __name__ == "__main__":
    recriar_banco_dados()