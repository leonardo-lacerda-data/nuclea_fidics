from src.database import get_connection

def atualizar_view_ml():
    print("📊 Atualizando View do ML..")
    conn = get_connection()

    if conn:
        cursor = conn.cursor()

        sql_view_ml = """
        CREATE OR REPLACE VIEW V_BF_TREINO_ML AS
        SELECT
            -- Informações do boleto
            b.id_boleto,
            b.vl_nominal,
            (b.dt_vencimento - b.dt_emissao) as nr_prazo_dias,

            -- Informações da empresa
            e.cd_cnae,
            e.sg_uf,
            COALESCE(e.vl_score_materialidade, 0) as vl_score_materialidade,
            COALESCE(e.vl_score_quantidade, 0) as vl_score_quantidade,

            -- Para ligar com as notícias
            CASE 
                WHEN SUBSTR(e.cd_cnae, 1, 2) BETWEEN '05' AND '33' THEN 'INDUSTRIA'
                WHEN SUBSTR(e.cd_cnae, 1, 2) BETWEEN '45' AND '47' THEN 'VAREJO'
                WHEN SUBSTR(e.cd_cnae, 1, 2) >= '49' THEN 'SERVICOS'
                ELSE 'OUTROS'
            END as ds_setor_macro,

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
                 WHERE m2.nm_indicador = 'PIM' AND m2.sg_uf = 'BR'
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

            -- ANÁLISE DE SENTIMENTOS
            COALESCE(
                (SELECT AVG(n.vl_sentimento)
                 FROM T_BF_NOTICIAS n
                 WHERE n.ds_setor = 
                       CASE 
                           WHEN SUBSTR(e.cd_cnae, 1, 2) BETWEEN '05' AND '33' THEN 'INDUSTRIA'
                           WHEN SUBSTR(e.cd_cnae, 1, 2) BETWEEN '45' AND '47' THEN 'VAREJO'
                           WHEN SUBSTR(e.cd_cnae, 1, 2) >= '49' THEN 'SERVICOS'
                           ELSE 'MERCADO'
                       END
                   AND n.dt_publicacao BETWEEN (b.dt_vencimento - 30) AND b.dt_vencimento
                ), 0
            ) as vl_sentimento_setorial,

            -- TARGET
            b.vl_inadimplencia as target

        FROM T_BF_BOLETO b
        JOIN T_BF_EMPRESA e ON b.id_pagador = e.id_empresa
        """

        try:
            cursor.execute(sql_view_ml)
            print("   ✅ View V_BF_TREINO_ML atualizada.")
        except Exception as e:
            print(f"   ❌ Erro View ML: {e}")
        finally:
            conn.close()

def atualizar_view_pbi():
    print("📊 Atualizando View do Power BI..")
    conn = get_connection()

    if conn:
        cursor = conn.cursor()

        sql_view_pbi = """
        CREATE OR REPLACE VIEW V_BF_ANALISE_PBI AS
        SELECT 
            -- Dados do boleto
            b.id_boleto,
            b.vl_nominal,
            b.dt_emissao,
            b.dt_vencimento,
            b.nr_dias_atraso,
            CASE 
                WHEN b.vl_inadimplencia = 0 THEN 'Em Dia' 
                ELSE 'Inadimplente' 
            END as st_pagamento,
            
            -- Dados da empresa
            e.id_empresa,
            e.cd_cnae,
            e.sg_uf,
            
            -- Qual o setor da empresa
            CASE 
                WHEN SUBSTR(e.cd_cnae, 1, 2) BETWEEN '05' AND '33' THEN 'Indústria'
                WHEN SUBSTR(e.cd_cnae, 1, 2) BETWEEN '45' AND '47' THEN 'Varejo'
                WHEN SUBSTR(e.cd_cnae, 1, 2) >= '49' THEN 'Serviços'
                ELSE 'Outros'
            END as ds_setor_economico,
        
            -- 3. INTELIGÊNCIA PREDITIVA (O Futuro - Random Forest)
            COALESCE(p.vl_probabilidade_inadimplencia, 0) as vl_score_risco,
            COALESCE(p.st_faixa_risco, 'N/A') as ds_faixa_risco,
            COALESCE(p.ds_principal, 'Em análise') as ds_motivo_risco,
        
            -- 4. INTELIGÊNCIA DE PERFIL (O Cluster)
            COALESCE(c.ds_perfil, 'Não Classificado') as ds_perfil_comportamental,
            
            -- 5. TERMÔMETRO DE MERCADO (O NLP - Sentimento)
            COALESCE(
                (SELECT AVG(n.vl_sentimento)
                 FROM T_BF_NOTICIAS n
                 WHERE n.ds_setor = 
                       CASE 
                           WHEN SUBSTR(e.cd_cnae, 1, 2) BETWEEN '05' AND '33' THEN 'INDUSTRIA'
                           WHEN SUBSTR(e.cd_cnae, 1, 2) BETWEEN '45' AND '47' THEN 'VAREJO'
                           WHEN SUBSTR(e.cd_cnae, 1, 2) >= '49' THEN 'SERVICOS'
                           ELSE 'MERCADO'
                       END
                   AND n.dt_publicacao BETWEEN (b.dt_vencimento - 30) AND b.dt_vencimento
                ), 0
            ) as vl_sentimento_setor,
        
            -- Classificação Visual do Sentimento (Para cor no Power BI)
            CASE 
                WHEN (SELECT AVG(n.vl_sentimento) FROM T_BF_NOTICIAS n WHERE n.dt_publicacao BETWEEN (b.dt_vencimento - 30) AND b.dt_vencimento) > 0.05 THEN 'Otimista'
                WHEN (SELECT AVG(n.vl_sentimento) FROM T_BF_NOTICIAS n WHERE n.dt_publicacao BETWEEN (b.dt_vencimento - 30) AND b.dt_vencimento) < -0.05 THEN 'Pessimista'
                ELSE 'Neutro'
            END as ds_humor_mercado
        
        FROM T_BF_BOLETO b
        JOIN T_BF_EMPRESA e ON b.id_pagador = e.id_empresa
        -- Left Join: Se o boleto for novo e não tiver passado na IA ainda, não some, só fica NULL
        LEFT JOIN T_BF_PREDICOES p ON b.id_boleto = p.id_boleto
        LEFT JOIN T_BF_CLUSTER c ON b.id_boleto = c.id_boleto 
        """

        try:
            cursor.execute(sql_view_pbi)
            print("   ✅ View V_BF_ANALISE_PBI atualizada.")
        except Exception as e:
            print(f"   ❌ Erro View Power BI: {e}")
        finally:
            conn.close()