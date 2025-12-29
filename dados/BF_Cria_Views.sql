DROP VIEW V_BF_ANALISE_PBI;
DROP VIEW V_BF_TREINO_ML;

CREATE OR REPLACE VIEW V_BF_ANALISE_PBI AS
SELECT
    b.id_boleto,
    b.dt_vencimento,
    b.dt_pagamento,
    b.vl_nominal AS vl_original,
    b.vl_baixa AS vl_pago,
    b.nr_dias_atraso,
    CASE WHEN b.vl_inadimplencia = 1 THEN 'Inadimplente'
        ELSE 'Em dia'
        END AS st_pagamento,
    CASE WHEN b.nr_dias_atraso <= 0 THEN '0. No prazo'
         WHEN b.nr_dias_atraso <= 5 AND b.nr_dias_atraso > 0 THEN '1. Atraso Técnico (até 5 dias)'
         WHEN b.nr_dias_atraso > 5 AND b.nr_dias_atraso <= 30 THEN '2. Atraso Curto (6 - 30 dias)'
         WHEN b.nr_dias_atraso > 30 AND b.nr_dias_atraso <= 60 THEN '3. Atraso Médio (31 - 60 dias)'
         ELSE '4. Atraso Longo (+60 dias)'
         END AS fx_atraso,
    e.sg_uf,
    e.cd_cnae,
    e.vl_score_materialidade,
    e.vl_score_quantidade
FROM T_BF_BOLETO b
LEFT OUTER JOIN T_BF_EMPRESA e
ON b.id_pagador = e.id_empresa;
    
CREATE OR REPLACE VIEW V_BF_TREINO_ML AS
SELECT
    -- DADOS DA TABELA DE BOLETO
    b.id_boleto,
    b.vl_nominal,
    (b.dt_vencimento - b.dt_emissao) as nr_prazo_dias,
    
    -- DADOS DA TABELA DE EMPRESA
    e.cd_cnae,
    e.sg_uf,
    COALESCE(e.vl_score_materialidade, 0) as vl_score_materialidade,
    COALESCE(e.vl_score_quantidade, 0) as vl_score_quantidade,
    
    -- SELIC
    (SELECT m.vl_indicador FROM T_BF_MACRO_ECONOMIA m 
     WHERE m.nm_indicador = 'SELIC' AND m.sg_uf = 'BR' AND m.dt_referencia <= b.dt_vencimento 
     ORDER BY m.dt_referencia DESC FETCH FIRST 1 ROW ONLY) as tax_selic,
     
    -- DÓLAR
    (SELECT m.vl_indicador FROM T_BF_MACRO_ECONOMIA m 
     WHERE m.nm_indicador = 'DOLAR' AND m.sg_uf = 'BR' AND m.dt_referencia <= b.dt_vencimento 
     ORDER BY m.dt_referencia DESC FETCH FIRST 1 ROW ONLY) as tax_dolar,

    -- DESEMPREGO
    (SELECT m.vl_indicador FROM T_BF_MACRO_ECONOMIA m 
     WHERE m.nm_indicador = 'DESEMPREGO' AND m.sg_uf = 'BR' AND m.dt_referencia <= b.dt_vencimento 
     ORDER BY m.dt_referencia DESC FETCH FIRST 1 ROW ONLY) as tax_desemprego,
     
    -- PIB
    (SELECT m.vl_indicador FROM T_BF_MACRO_ECONOMIA m 
     WHERE m.nm_indicador = 'PIB' AND m.sg_uf = 'BR' AND m.dt_referencia <= b.dt_vencimento 
     ORDER BY m.dt_referencia DESC FETCH FIRST 1 ROW ONLY) as indice_pib,
    
    -- VAREJO
    COALESCE(
        (SELECT m.vl_indicador FROM T_BF_MACRO_ECONOMIA m 
         WHERE m.nm_indicador = 'PMC' AND m.sg_uf = e.sg_uf AND m.dt_referencia <= b.dt_vencimento
         ORDER BY m.dt_referencia DESC FETCH FIRST 1 ROW ONLY),
        (SELECT AVG(m2.vl_indicador) FROM T_BF_MACRO_ECONOMIA m2 WHERE m2.nm_indicador = 'PMC')
    ) as var_varejo,

    -- INDÚSTRIA
    COALESCE(
        (SELECT m.vl_indicador FROM T_BF_MACRO_ECONOMIA m 
         WHERE m.nm_indicador = 'PIM' AND m.sg_uf = e.sg_uf AND m.dt_referencia <= b.dt_vencimento
         ORDER BY m.dt_referencia DESC FETCH FIRST 1 ROW ONLY),
        (SELECT m2.vl_indicador FROM T_BF_MACRO_ECONOMIA m2 
         WHERE m2.nm_indicador = 'PIM' AND m2.sg_uf = 'BR' -- Coringa Brasil
           AND m2.dt_referencia <= b.dt_vencimento
         ORDER BY m2.dt_referencia DESC FETCH FIRST 1 ROW ONLY)
    ) as var_industria,

    -- SERVIÇOS
    COALESCE(
        (SELECT m.vl_indicador FROM T_BF_MACRO_ECONOMIA m 
         WHERE m.nm_indicador = 'PMS' AND m.sg_uf = e.sg_uf AND m.dt_referencia <= b.dt_vencimento
         ORDER BY m.dt_referencia DESC FETCH FIRST 1 ROW ONLY),
        (SELECT AVG(m2.vl_indicador) FROM T_BF_MACRO_ECONOMIA m2 WHERE m2.nm_indicador = 'PMS')
    ) as var_servicos,

    -- TARGET
    b.vl_inadimplencia as target

FROM T_BF_BOLETO b
JOIN T_BF_EMPRESA e ON b.id_pagador = e.id_empresa;
