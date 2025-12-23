DROP TABLE T_BF_BOLETO CASCADE CONSTRAINTS;
DROP TABLE T_BF_EMPRESA CASCADE CONSTRAINTS;
DROP TABLE T_BF_MACRO_ECONOMIA CASCADE CONSTRAINTS;

DROP SEQUENCE SQ_BF_MACRO_ECONOMIA;

CREATE SEQUENCE SQ_BF_MACRO_ECONOMIA 
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
    cd_cnae VARCHAR(7) NOT NULL,
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

ALTER TABLE T_BF_BOLETO
    ADD CONSTRAINT FK_BF_BOLETO_EMPRESA FOREIGN KEY ( id_pagador ) 
    REFERENCES T_BF_EMPRESA ( id_empresa ) 
    NOT DEFERRABLE
;
