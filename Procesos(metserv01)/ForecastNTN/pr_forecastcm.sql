/****** Object:  StoredProcedure [METDB].[PR_FORECASTCM]    Script Date: 25/06/2025 11:55:39 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
 ALTER PROCEDURE [METDB].[PR_FORECASTCM] AS
 DECLARE
 	@V_DIA       date,
	@V_Fecalt    date,
	@V_Fecven    date,
	@V_Cup       varchar(24),
	@V_Cliente   varchar(15),
	@V_Provincia varchar(2),
	@V_Tarifa    varchar(10),
	@V_Presion   varchar(2),
	@V_CAUDAL    float,
	@V_CM        float,
	@D_Fecejc    Date,
	@N_Version   int,
	@v_Contrato  Varchar(15);
   Declare PrevCM Cursor For 
    SELECT CNT_CODIGO,
        DIA_1,
        CNT_CLI_NIF, 
        CNT_CUP_CODIGO, 
        CUP_PRV_CODIGO, 
        CNT_TAR_CODIGO, 
        CNT_CAUDAL, 
        CNT_PRESION,
        CASE
            WHEN CNT_TAR_CODIGO IN ('RL02','RL01','RL03','RLPS3','RLPS1','RLPS2','RLPS3') AND (CAST(CNT_PRESION AS INT) in (1,2,null)) THEN 
                (
                    SELECT PFE_PERFIL / DAY(EOMONTH(DIA_1)) 
                    FROM METDB.MET_PERFENAGAS
                    WHERE PFE_PRV_CODIGO = CUP_PRV_CODIGO
                        AND PFE_MONTH = FORMAT(DIA_1, 'MM')
                        AND PFE_TAR_CODIGO = CASE 
                                                WHEN CNT_TAR_CODIGO IN ('RL01','RLPS1') THEN '3.1'
                                                WHEN CNT_TAR_CODIGO IN ('RL02','RLPS2','RL03','RLPS3') THEN '3.2'
                                            END
                ) 
            ELSE 
                ISNULL((SELECT CASE 
                                WHEN CNT_PRESION IN ('03','04','06') AND MAX(A80_TIPODIA) OVER (PARTITION BY A80_FECREP)='L' THEN
                                       A80_CD* METDB.FT_DIASLAB(CUP_PRV_CODIGO, A80_FECREP, 'N')/0.85
                                WHEN CNT_PRESION IN ('03','04','06') AND MAX(A80_TIPODIA) OVER (PARTITION BY A80_FECREP)<>'L' THEN 
                                       A80_CD* METDB.FT_DIASLAB(CUP_PRV_CODIGO, A80_FECREP, 'S')/0.15
                               WHEN (CNT_PRESION = '01'  OR CNT_PRESION IS null) AND CNT_TAR_CODIGO IN ('RL01','RL02','RL03','RLPS1','RLPS2','RLPS3') THEN
                                       A80_CD * FORMAT(EOMONTH(A80_FECREP), 'dd')/
									   ISNULL((SELECT COF_T2 
                                                 FROM METDB.MET_COEFTEMP 
                                                WHERE COF_FECHA = A80_FECREP
                                                 AND COF_PRV_CODIGO = CUP_PRV_CODIGO
						                         AND COF_ORIGEN = 'I1'),
						                        ISNULL((SELECT COF_T2 
                                                          FROM METDB.MET_COEFTEMP 
                                                          WHERE COF_FECHA = A80_FECREP
                                                            AND COF_PRV_CODIGO = CUP_PRV_CODIGO
						                                    AND COF_ORIGEN = 'PR'),
                                                        (SELECT COF_T2 
                                                           FROM METDB.MET_COEFTEMP C1
                                                          WHERE COF_PRV_CODIGO = CUP_PRV_CODIGO
								                            AND COF_ORIGEN = 'I1'
									                        AND COF_FECHA = (SELECT MAX(COF_FECHA) 
									                                           FROM METDB.MET_COEFTEMP C2 
													                           WHERE C2.COF_PRV_CODIGO = C1.COF_PRV_CODIGO
																			     And COF_ORIGEN = 'I1'))))
                                ELSE NULL END / DAY(EOMONTH(DIA_1)) CM
                        FROM METDB.MET_REPARTA80
                       where A80_CUP=CNT_CUP_CODIGO 
                         AND A80_TIPTELE='N'
                         AND MONTH( A80_FECREP)=  MONTH(DIA_1)
                         AND YEAR(A80_FECREP) = YEAR(DIA_1)
                         AND DAY(A80_FECREP) = '02'),
                                                                 ISNULL(
                    (

                                                                          SELECT 
                                               SUM(CSD_CONSUMO) / DAY(EOMONTH(DATEADD(DAY, -365, DIA_1))) AS ConsumoPromedio
  
                                        FROM (
                                               SELECT 
                                                      CSD_CUP_CODIGO,
                                                      DAY(MIN(CSD_DIA) OVER (PARTITION BY CSD_CUP_CODIGO ORDER BY CSD_DIA)) AS PrimerDiaConsumo,
                                                      CSD_CONSUMO
                                               FROM [METDB].[MET_CONSUMDIARIO] 
                                               WHERE 
                                                      MONTH(DATEADD(DAY, -365, DIA_1)) = MONTH(CSD_DIA)      
                            AND YEAR(DATEADD(DAY, -365, DIA_1)) = YEAR(CSD_DIA)
                                               AND  [MET_CONSUMDIARIO].CSD_CUP_CODIGO = CNT_CUP_CODIGO 
       
                                               GROUP BY CSD_CUP_CODIGO, CSD_DIA, CSD_CONSUMO
                                        ) AS Subconsulta 
                                        GROUP BY PrimerDiaConsumo HAVING PrimerDiaConsumo = 1
                                                                                                                                                                                                                                                                  
                    ) 
                                                
                                    
                    * (SELECT PAR_NUMERO FROM METDB.MET_PARAMETROS WHERE PAR_CODIGO = 'COEFC'), 
                    ISNULL(
                        (
                            SELECT SUM(CONSUMO) 
                            FROM (
                                SELECT (
                                    (SCS_CONSP1 / (DATEDIFF(DAY, SCS_FECINI, SCS_FECFIN) + 1)) 
                                    * CASE 
                                        WHEN MONTH(SCS_FECINI) = MONTH(DIA_1) THEN DATEDIFF(DAY, SCS_FECINI, EOMONTH(SCS_FECINI)) + 1
                                        WHEN MONTH(SCS_FECFIN) = MONTH(DIA_1) THEN DATEDIFF(DAY, DATEADD(MONTH, DATEDIFF(MONTH, 0, DATEADD(DAY, -365, DIA_1)), 0), SCS_FECFIN) + 1
                                        ELSE DAY(EOMONTH(DATEADD(DAY, -365, DIA_1)))
                                    END
                                ) / DAY(EOMONTH(DATEADD(DAY, -365, DIA_1))) AS CONSUMO
                                FROM metdb.SIPS_CONSUMOS 
                                WHERE SCS_CUPS = CNT_CUP_CODIGO 
                                    AND (
                                        FORMAT(DATEADD(DAY, -365, DIA_1), 'MM/yyyy') = FORMAT(SCS_FECINI, 'MM/yyyy') 
                                        OR FORMAT(DATEADD(DAY, -365, DIA_1), 'MM/yyyy') = FORMAT(SCS_FECFIN, 'MM/yyyy') 
                                        OR DATEADD(DAY, -365, DIA_1) BETWEEN SCS_FECINI AND SCS_FECFIN
                                    )
                            ) T
                        ) 
                        * (SELECT PAR_NUMERO FROM METDB.MET_PARAMETROS WHERE PAR_CODIGO = 'COEFC'), 
                        ISNULL(
                            (
                                SELECT CONSP1 
                                FROM (
                                    SELECT scs_consp1 / (DATEDIFF(DAY, SCS_fECINI, SCS_FECFIN) + 1) AS CONSP1 
                                    FROM METDB.SIPS_CONSUMOS 
                                    WHERE SCS_CUPS = CNT_CUP_CODIGO 
                                    ORDER BY SCS_FECINI DESC 
                                    OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
                                ) T
                            ), 
                            ISNULL(
                                (
                                    SELECT CONSUMO 
                                    FROM (
                                        SELECT CONSUMO_KWH / (DATEDIFF(DAY, FECDESDE, FECHASTA) + 1) AS CONSUMO 
                                        FROM METDB.VW_CONSUMO_FACTURACION 
                                        WHERE CUPS = CNT_CUP_CODIGO 
                                        ORDER BY FECHASTA DESC 
                                        OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
                                    ) T
                                ), 
                                                                                                                 ISNULL((Select top(1) CSM_CONSANUAL 
                                  From metdb.met_contsml
                                                                                                                              Where CSM_CUP_CODIGO = CNT_CUP_CODIGO
                                                                                                                                 And Dia_1 Between CSM_FECINI AND CSM_FECFIN),CNT_CAUDAL) /365
                            )
                        )
                    )
                ))
        END * DAY(EOMONTH(DIA_1)) AS CM 
    FROM metdb.MET_CONTRATOS c, METDB.MET_CUPS,
        (SELECT DIA DIA_1 FROM METDB.VW_DIASANYO WHERE DIA BETWEEN  convert(date,GETDATE()) AND eomonth(dateadd(MONTH, 1,datetrunc(dayofyear,getdate())))) D
    WHERE CNT_CLI_NIF != '77777777B' 
	  AND CNT_CNS_CODIGO NOT IN ('A','R','D') 
	  AND CNT_TAR_CODIGO NOT IN ('RLPS7','RLPS8','RLTA7','RLTB7','RL11','RL10','RL09','RL08') 
	  AND cnt_telemedido = 'N' 
	  AND CNT_POTP1 IS NULL 
	  AND (DIA_1 BETWEEN CNT_FECALT AND ISNULL(CNT_FECBAJ,CNT_FECVEN) OR (DIA_1>= CNT_FECALT and  CNT_CNS_CODIGO NOT IN ('B','M')))
      AND NOT EXISTS (SELECT 'X' 
                        FROM METDB.MET_CONTRATOS T1 
                       WHERE T1.CNT_CUP_CODIGO = C.CNT_CUP_CODIGO 
                         AND CNT_CNS_CODIGO = 'C' 
                          AND T1.CNT_CODIGO != C.CNT_CODIGO) 
      AND CNT_CUP_CODIGO = CUP_CODIGO
 BEGIN	 
    TRUNCATE TABLE METDB.MET_FORECASTCM
	Open PrevCM
	FETCH NEXT FROM PrevCM Into @V_Contrato,@V_Dia, @V_Cliente, @V_Cup, @V_Provincia, @V_Tarifa, @V_CAUDAL, @V_Presion, @V_CM; 
	WHILE @@FETCH_STATUS = 0  
	 Begin
	   Insert Into METDB.MET_FORECASTCM(FTC_DIA,        FTC_CUP_CODIGO, FTC_CLI_NIF, FTC_PRV_CODIGO,
	                                        FTC_TAR_CODIGO, FTC_PRESION,    FTC_CAUDAL,  FTC_CM,
	                                        FTC_CONTRATO)
								    Values (@V_Dia,         @V_Cup,         @V_Cliente,  @V_Provincia,   
											@V_Tarifa,      @V_Presion,     @V_CAUDAL,      @V_CM,
                           				    @V_Contrato)
										
	    FETCH NEXT FROM PrevCM Into @V_Contrato,@V_Dia, @V_Cliente, @V_Cup, @V_Provincia, @V_Tarifa, @V_CAUDAL, @V_Presion, @V_CM; 
	 End
	Close PrevCM
    Deallocate PrevCM
 END 