/****** Object:  StoredProcedure [METDB].[PR_FORECAST_NTM]    Script Date: 25/06/2025 12:13:48 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
 ALTER PROCEDURE [METDB].[PR_FORECAST_NTM] AS
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
	@V_CD        float,
	@D_Fecejc    Date,
	@N_Version   int,
	@v_Contrato  Varchar(15);
   Declare ForecastNTM Cursor For 

   SELECT FTC_CONTRATO,
    FTC_DIA, 
    FTC_CLI_NIF, 
    FTC_CUP_CODIGO, 
    FTC_PRV_CODIGO, 
    FTC_TAR_CODIGO,
                ISNULL((Select top(1) CSM_CONSANUAL 
                                  From metdb.met_contsml
                                                                                                                              Where CSM_CUP_CODIGO = FTC_CUP_CODIGO
                                                                                                                                 And FTC_DIA Between CSM_FECINI AND CSM_FECFIN),FTC_CAUDAL) CAUDAL,
    FTC_PRESION,  
    FTC_CM,
        CASE 
            WHEN (FTC_PRESION IN ('03','04','06') OR (FTC_TAR_CODIGO IN ('RLTA5','RLTA6') and  FTC_PRESION IS null ))  AND datepart(dw, FTC_DIA) NOT IN (1,7) THEN 
                (FTC_CM * (SELECT PAR_NUMERO FROM METDB.MET_PARAMETROS WHERE par_codigo = 'CFOR')) / METDB.FT_DIASLAB(FTC_PRV_CODIGO, FTC_DIA, 'N')
            WHEN (FTC_PRESION IN ('03','04','06') OR (FTC_TAR_CODIGO IN ('RLTA5','RLTA6') and  FTC_PRESION IS null )) AND datepart(dw, FTC_DIA) IN (1,7) THEN 
                (FTC_CM * (SELECT 1 - PAR_NUMERO FROM METDB.MET_PARAMETROS WHERE par_codigo = 'CFOR')) / METDB.FT_DIASLAB(FTC_PRV_CODIGO, FTC_DIA, 'S') 
            WHEN (FTC_PRESION IN ('01','02') OR FTC_PRESION IS null) AND FTC_TAR_CODIGO IN ('RLTB5','RLTB6','RLTB7','RL04','RLPS4','RLPS5','RLPS6') THEN 
                (FTC_CM / FORMAT(EOMONTH(FTC_DIA), 'dd')) * ISNULL((SELECT COF_T1 
                                                        FROM METDB.MET_COEFTEMP 
                                                       WHERE COF_FECHA = CASE 
                                                                          WHEN FTC_DIA = CONVERT(DATE, GETDATE()) THEN CONVERT(DATE, GETDATE()) 
                                                                          ELSE DATEADD(DAY, 1, CONVERT(DATE, GETDATE())) 
                                                                          END 
                                                         AND COF_PRV_CODIGO = FTC_PRV_CODIGO
														 AND COF_ORIGEN = 'I1'),
														 ISNULL((SELECT COF_T1 
                                                                   FROM METDB.MET_COEFTEMP 
                                                                   WHERE COF_FECHA = CASE 
                                                                          WHEN FTC_DIA = CONVERT(DATE, GETDATE()) THEN CONVERT(DATE, GETDATE()) 
                                                                          ELSE DATEADD(DAY, 1, CONVERT(DATE, GETDATE())) 
                                                                          END 
                                                                     AND COF_PRV_CODIGO = FTC_PRV_CODIGO
														             AND COF_ORIGEN = 'PR'),
																	 (SELECT COF_T2 
                                                                        FROM METDB.MET_COEFTEMP C1
                                                                       WHERE COF_PRV_CODIGO = FTC_PRV_CODIGO
								                                         AND COF_ORIGEN = 'I1'
									                                     AND COF_FECHA = (SELECT MAX(COF_FECHA) 
									                                                        FROM METDB.MET_COEFTEMP C2 
													                                       WHERE C2.COF_PRV_CODIGO = C1.COF_PRV_CODIGO
																			                 And COF_ORIGEN = 'I1'))))
            WHEN (FTC_PRESION IN ('01','02') OR FTC_PRESION IS null) AND FTC_TAR_CODIGO IN ('RL01','RL02','RL03','RLPS1','RLPS2','RLPS3') THEN
                (
                    SELECT PFE_PERFIL / DAY(EOMONTH(FTC_DIA))
                    FROM METDB.MET_PERFENAGAS
                    WHERE PFE_PRV_CODIGO = FTC_PRV_CODIGO
                        AND PFE_MONTH = CONVERT(NUMERIC(2), FORMAT(FTC_DIA, 'MM'))
                        AND PFE_TAR_CODIGO = CASE 
                                                WHEN FTC_TAR_CODIGO = 'RL01' THEN '3.1'
                                                WHEN FTC_TAR_CODIGO = 'RLPS1' THEN '3.1'
                                                WHEN FTC_TAR_CODIGO = 'RL02' THEN '3.2'
                                                WHEN FTC_TAR_CODIGO = 'RLPS2' THEN '3.2'
                                                WHEN FTC_TAR_CODIGO = 'RL03' THEN '3.3' 
                                                WHEN FTC_TAR_CODIGO = 'RLPS3' THEN '3.3' 
                                                ELSE FTC_TAR_CODIGO 
                                            END
                ) *  ISNULL((SELECT COF_T2 
                     FROM METDB.MET_COEFTEMP 
                     WHERE COF_FECHA = CASE 
                                            WHEN FTC_DIA = CONVERT(DATE, GETDATE()) THEN CONVERT(DATE, GETDATE()) 
                                            ELSE DATEADD(DAY, 1, CONVERT(DATE, GETDATE())) 
                                        END 
                        AND COF_PRV_CODIGO = FTC_PRV_CODIGO
						AND COF_ORIGEN = 'I1'),
						ISNULL((SELECT COF_T2 
                                  FROM METDB.MET_COEFTEMP 
                                  WHERE COF_FECHA = CASE 
                                                      WHEN FTC_DIA = CONVERT(DATE, GETDATE()) THEN CONVERT(DATE, GETDATE()) 
                                                    ELSE DATEADD(DAY, 1, CONVERT(DATE, GETDATE())) 
                                                   END 
                                   AND COF_PRV_CODIGO = FTC_PRV_CODIGO
						           AND COF_ORIGEN = 'PR'),
                                (SELECT COF_T2 
                                   FROM METDB.MET_COEFTEMP C1
                                  WHERE COF_PRV_CODIGO = FTC_PRV_CODIGO
								    AND COF_ORIGEN = 'I1'
									AND COF_FECHA = (SELECT MAX(COF_FECHA) 
									                   FROM METDB.MET_COEFTEMP C2 
													  WHERE C2.COF_PRV_CODIGO = C1.COF_PRV_CODIGO))))
        END AS CD
FROM METDB.MET_FORECASTCM
 BEGIN	 
    Delete METDB.TMP_FORECASTSHNTM
	 Where DIA >=  convert(date,GETDATE())
	Open ForecastNTM
	FETCH NEXT FROM FORECASTNTM Into @V_Contrato,@V_Dia, @V_Cliente, @V_Cup, @V_Provincia, @V_Tarifa, @V_CAUDAL, @V_Presion, @V_CM, @V_CD; 
	WHILE @@FETCH_STATUS = 0  
	 Begin
	   Insert Into METDB.MET_FORECASTSHNTM (FTN_FECSYS,     FTN_VERSION,    FTN_DIA,     FTN_CUP_CODIGO, FTN_CLI_NIF,
	                                        FTN_PRV_CODIGO, FTN_TAR_CODIGO, FTN_PRESION, FTN_CAUDAL,     FTN_CM,
	                                        FTN_CD,         FTN_CONTRATO) 
								    Values (CONVERT(DATE, GETDATE()), 1,    @V_Dia,      @V_Cup,         @V_Cliente,
	         							    @V_Provincia,   @V_Tarifa,      @V_Presion,  @V_CAUDAL,      @V_CM,
                           				    @V_CD,          @V_Contrato)
	  Insert Into METDB.TMP_FORECASTSHNTM(FECSYS,           VERSION,        DIA,         CUP,            CLIENTE,
	                                      PROVINCIA,        TARIFA,         PRESION,     CAUDAL,         CM,
	                                      CD,               CONTRATO)
                                    values(CONVERT(DATE, GETDATE()), 1,    @V_Dia,      @V_Cup,         @V_Cliente,
	         							    @V_Provincia,   @V_Tarifa,      @V_Presion,  @V_CAUDAL,      @V_CM,
                           				    @V_CD,          @V_Contrato)
										
	    FETCH NEXT FROM FORECASTNTM Into @V_Contrato,@V_Dia, @V_Cliente, @V_Cup, @V_Provincia, @V_Tarifa, @V_CAUDAL, @V_Presion, @V_CM, @V_CD; 
	 End
	Close ForecastNTM	
    Deallocate ForecastNTM
 END 
