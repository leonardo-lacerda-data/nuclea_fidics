DROP TABLE T_BF_BOLETO CASCADE CONSTRAINTS;
DROP TABLE T_BF_EMPRESA CASCADE CONSTRAINTS;
DROP TABLE T_BF_MACRO_ECONOMIA CASCADE CONSTRAINTS;
DROP TABLE T_BF_PREDICOES CASCADE CONSTRAINTS;

DROP SEQUENCE SQ_BF_MACRO_ECONOMIA;
DROP SEQUENCE SQ_BF_PREDICOES;

CREATE SEQUENCE SQ_BF_MACRO_ECONOMIA 
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

CREATE SEQUENCE SQ_BF_PREDICOES 
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

CREATE TABLE T_BF_BOLETO (
    id_boleto VARCHAR2 (64) NOT NULL,
    id_pagador VARCHAR2 (64) NOT NULL,
    id_beneficiario VARCHAR (64) NOT NULL,
    vl_nominal NUMBER(15,2) NOT NULL,
    vl_baixa NUMBER (15,2),
    dt_emissao DATE NOT NULL,
    dt_vencimento DATE NOT NULL,
    dt_pagamento DATE,
    tp_baixa VARCHAR2(100),
    nr_dias_atraso NUMBER(5) NOT NULL,
    vl_inadimplencia NUMBER(1) NOT NULL
);

ALTER TABLE T_BF_BOLETO 
    ADD CONSTRAINT PK_BF_BOLETO PRIMARY KEY ( id_boleto );
    
ALTER TABLE T_BF_BOLETO
    ADD CONSTRAINT CK_BF_VL_INADIMPLENCIA 
    CHECK (vl_inadimplencia IN (0, 1));

CREATE TABLE T_BF_EMPRESA (
    id_empresa VARCHAR2(64) NOT NULL,
    cd_cnae VARCHAR2(7) NOT NULL,
    sg_uf CHAR(2) NOT NULL,
    vl_score_materialidade NUMBER(10,2),
    vl_score_quantidade NUMBER(10,2)
);

ALTER TABLE T_BF_EMPRESA 
    ADD CONSTRAINT PK_BF_EMPRESA PRIMARY KEY ( id_empresa );

ALTER TABLE T_BF_EMPRESA
    ADD CONSTRAINT CK_BF_EMPRESA_SG_UF
    CHECK (sg_uf IN ('AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO',
    'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI',
    'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO', 'ND', 'BR'));

CREATE TABLE T_BF_MACRO_ECONOMIA (
    id_macro_economia NUMBER(10) DEFAULT
                      SQ_BF_MACRO_ECONOMIA.NEXTVAL NOT NULL,
    sg_uf CHAR(2) NOT NULL,
    dt_referencia DATE NOT NULL,
    nm_indicador VARCHAR (50) NOT NULL,
    vl_indicador NUMBER (20, 6) NOT NULL
);

ALTER TABLE T_BF_MACRO_ECONOMIA
    ADD CONSTRAINT PK_BF_MACRO_ECONOMIA PRIMARY KEY ( id_macro_economia );
    
ALTER TABLE T_BF_MACRO_ECONOMIA
    ADD CONSTRAINT CK_BF_MACRO_SG_UF
    CHECK (sg_uf IN ('AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO',
    'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI',
    'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO', 'ND', 'BR'));

CREATE TABLE T_BF_PREDICOES (
    id_predicao NUMBER (10) DEFAULT
                SQ_BF_PREDICOES.NEXTVAL NOT NULL,
    id_boleto VARCHAR2 (64) NOT NULL,
    dt_processamento DATE DEFAULT SYSDATE,
    vl_probabilidade_inadimplencia NUMBER(5, 4), -- Ex: 0.8540 (85,4%)
    st_faixa_risco VARCHAR2 (20),                 -- 'ALTO', 'MEDIO', 'BAIXO'
    ds_principal VARCHAR2 (100)     -- Ex: 'Score Baixo e Varejo em Queda'
);

ALTER TABLE T_BF_PREDICOES 
    ADD CONSTRAINT UN_BF_PREDICOES UNIQUE (id_boleto, dt_processamento);
ALTER TABLE T_BF_PREDICOES
    ADD CONSTRAINT CK_BF_PRED_ST_RISCO CHECK (st_faixa_risco IN ('ALTO', 'MEDIO', 'BAIXO'));

ALTER TABLE T_BF_BOLETO
    ADD CONSTRAINT FK_BF_BOLETO_EMPRESA FOREIGN KEY ( id_pagador ) 
    REFERENCES T_BF_EMPRESA ( id_empresa ) 
    NOT DEFERRABLE
;

ALTER TABLE T_BF_PREDICOES
    ADD CONSTRAINT FK_BF_PREDICOES_BOLETO FOREIGN KEY ( id_boleto ) 
    REFERENCES T_BF_BOLETO ( id_boleto ) 
    NOT DEFERRABLE
;
