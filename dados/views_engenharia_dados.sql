DROP VIEW V_BF_ANALISE;

CREATE OR REPLACE VIEW V_BF_ANALISE AS
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
LEFT JOIN T_BF_EMPRESA e
ON b.id_pagador = e.id_empresa;
