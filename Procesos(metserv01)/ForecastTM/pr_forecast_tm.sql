/****** Object:  StoredProcedure [METDB].[PR_FORECAST_TM]    Script Date: 25/06/2025 12:11:57 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

 ALTER PROCEDURE [METDB].[PR_FORECAST_TM] AS
 DECLARE
 	@V_DIA       date,
	@V_Fecalt    date,
	@V_Fecven    date,
	@V_Cup       varchar(24),
	@V_Cliente   varchar(15),
	@V_Provincia varchar(2),
	@V_Tarifa    varchar(10),
	@V_Presion   varchar(2),
	@V_Cogen     Varchar(1),
	@V_CAUDAL    float,
	@V_DIFINTRA  float,
	@V_D07EST    float,
	@V_CONSMES   float,
	@V_CONSDIA   float,
	@columnasPivot NVARCHAR(MAX),
	@query NVARCHAR(MAX),
	@V_Contrato Varchar(15),
	@V_FechaDia varchar(max),
	@V_Version   Int;
   Declare Forecast Cursor For 
          Select dia, CNT_CODIGO, cnt_cup_codigo, cnt_cli_nif, CUP_QD, CNT_TAR_CODIGO, CNT_PRESION,
			          CUP_PRV_CODIGO, CNT_FECALT, CNT_FECVEN,(SELECT distinct CSM_COGEN 
					                                                FROM METDB.MET_CONTSML C1
					                                               WHERE CSM_CUP_CODIGO = CNT_CUP_CODIGO
																     AND CSM_FECINI = (SELECT MAX(CSM_FECINI) FROM METDB.MET_CONTSML C2 WHERE C2.CSM_CUP_CODIGO = C1.CSM_CUP_CODIGO)) GOGEN, 
             ROUND(ISNULL((SELECT RP8_TELEMEDIDA FROM METDB.MET_REPARTD08 WHERE RP8_CUP = CNT_CUP_CODIGO AND RP8_FECREP = datetrunc(dayofyear,getdate()))/
               NULLIF((SELECT RP8_TELEMEDIDA FROM METDB.MET_REPARTD08 WHERE RP8_CUP = CNT_CUP_CODIGO AND RP8_FECREP = DATEADD(DAY,-1,datetrunc(dayofyear,getdate()))),0) - 1,
               (SELECT RP7_TELEMEDIDA FROM METDB.MET_REPARTD07 WHERE RP7_CUP = CNT_CUP_CODIGO AND RP7_FECREP = datetrunc(dayofyear,getdate()))/
               NULLIF((SELECT RP7_TELEMEDIDA FROM METDB.MET_REPARTD07 WHERE RP7_CUP = CNT_CUP_CODIGO AND RP7_FECREP = DATEADD(DAY,-1,datetrunc(dayofyear,getdate()))),0) - 1),2)DIF_Intra,
               ROUND((SELECT RP7_TELEMEDIDA FROM METDB.MET_REPARTD07 WHERE RP7_CUP = CNT_CUP_CODIGO AND RP7_FECREP = datetrunc(dayofyear,getdate()))/
                 nullif((SELECT AVG(PORCENTAJE)
    FROM (
    SELECT CASE when DATEPART(weekday,FECREP) Not IN (1,2,6,7) THEN -1 ELSE  DATEPART(weekday,FECREP) END DIA_SEMANA, 
              SUM(RP7_TELEMEDIDA)/NULLIF(SUM(TELEMEDIDA),0) PORCENTAJE
       FROM METDB.VW_REPARTB80, METDB.MET_REPARTD07
       WHERE FECREP BETWEEN DATEADD(DAY, -30, datetrunc(dayofyear,getdate())) AND datetrunc(dayofyear,getdate())
          And RP7_TIPTELE IN ('R','E')
          AND FECREP = RP7_FECREP
          and CUP = CNT_CUP_CODIGO
          and CUP = RP7_CUP
          GROUP BY CASE when DATEPART(weekday,FECREP) Not IN (1,2,6,7) THEN -1 ELSE  DATEPART(weekday,FECREP) END) T),0),0) D07_ESTIMADO,
          CASE when DIA BETWEEN cnt_fecalt And CNT_FECVEN Then round(METDB.FT_CONSUMO_FORECAST(CNT_CUP_CODIGO, DIA, CUP_PRV_CODIGO, CUP_QD),0) else 0 end consumo
      From METDB.MET_CONTRATOS C1, METDB.MET_CUPS, METDB.VW_DIASANYO
     Where (CNT_CNS_CODIGO IN ('P','C') OR (CNT_CNS_CODIGO = 'T' And  NOT EXISTS (SELECT 'X' FROM METDB.MET_CONTRATOS C2 WHERE C2.CNT_CUP_CODIGO = C1.CNT_CUP_CODIGO AND CNT_CNS_CODIGO = 'C')))
	   And CNT_POTP1 IS NULL
       And (cnt_telemedido = 'S' or CNT_TAR_CODIGO IN('RL08','RL09','RL10','RL11','RLTA7','RLTB7'))
       And DIA Between datetrunc(dayofyear,getdate()) and eomonth(dateadd(MONTH, 1,datetrunc(dayofyear,getdate())))
	   And DIA BETWEEN CNT_FECALT AND ISNULL(CNT_FECBAJ,CNT_FECVEN)
       And cnt_cup_codigo = cup_codigo
     GROUP BY dia,CNT_CODIGO, cnt_cup_codigo,cnt_cli_nif, CUP_PRV_CODIGO, CUP_QD, cnt_fecalt, CNT_FECVEN, CNT_TAR_CODIGO, CNT_PRESION
	 order by cnt_codigo desc,  dia 
   Declare Actualiza_D07 Cursor For
      	   SELECT FTS_CUP_CODIGO,  rOUND(ISNULL(((SELECT RP8_TELEMEDIDA FROM METDB.MET_REPARTD08 WHERE RP8_CUP = FTS_CUP_CODIGO AND RP8_FECREP = datetrunc(dayofyear,getdate()))/
               NULLIF((SELECT RP8_TELEMEDIDA FROM METDB.MET_REPARTD08 WHERE RP8_CUP = FTS_CUP_CODIGO AND RP8_FECREP = DATEADD(DAY,-1,datetrunc(dayofyear,getdate()))),0))-1 ,
               ((SELECT RP7_TELEMEDIDA FROM METDB.MET_REPARTD07 WHERE RP7_CUP = FTS_CUP_CODIGO AND RP7_FECREP = datetrunc(dayofyear,getdate()))/
               NULLIF((SELECT RP7_TELEMEDIDA FROM METDB.MET_REPARTD07 WHERE RP7_CUP = FTS_CUP_CODIGO AND RP7_FECREP = DATEADD(DAY,-1,datetrunc(dayofyear,getdate()))),0))-1),2)DIF_Intra,
               ROUND((SELECT  RP7_TELEMEDIDA FROM METDB.MET_REPARTD07 WHERE RP7_CUP = FTS_CUP_CODIGO AND RP7_FECREP = datetrunc(dayofyear,getdate()))/
                 nullif((SELECT AVG(PORCENTAJE)
    FROM (
    SELECT CASE when DATEPART(weekday,FECREP) Not IN (1,2,6,7) THEN -1 ELSE  DATEPART(weekday,FECREP) END DIA_SEMANA, 
              SUM(RP7_TELEMEDIDA)/NULLIF(SUM(TELEMEDIDA),0) PORCENTAJE
       FROM METDB.VW_REPARTB80, METDB.MET_REPARTD07
       WHERE FECREP BETWEEN DATEADD(DAY, -30, datetrunc(dayofyear,getdate())) AND datetrunc(dayofyear,getdate())
          And RP7_TIPTELE = 'R'
          AND FECREP = RP7_FECREP
          and CUP = FTS_CUP_CODIGO
          and CUP = RP7_CUP
          GROUP BY CASE when DATEPART(weekday,FECREP) Not IN (1,2,6,7) THEN -1 ELSE  DATEPART(weekday,FECREP) END) T),0),0) D07_ESTIMADO
	    FROM (SELECT FTS_CUP_CODIGO 
		        FROM METDB.MET_FORECASTSH
		       WHERE FTS_FECSYS = CONVERT(date, GETDATE())
			   GROUP BY FTS_CUP_CODIGO)A
 BEGIN
   /* Para borrar previsiones que se generaron con la fecha erronea de fecha de alta
      	DECLARE
	 @V_Contrato Varchar(15),
	 @D_Fecalt date;
	BEGIN
	  Declare Contratos Cursor for
	     Select FTS_CONTRATO, CNT_FECALT
          From metdb.MET_FORECASTSH, metdb.met_contratos
         Where FTS_CONTRATO = CNT_CODIGO
	       And FTS_FECALT != CNT_FECALT
		   and FTS_DIA < CNT_FECALT
	     GROUP BY FTS_CONTRATO, CNT_FECALT     
      Open Contratos
	  Fetch Next From Contratos Into @V_Contrato, @D_Fecalt
	  WHILE @@FETCH_STATUS = 0  
	    Begin
		 Delete METDB.MET_FORECASTSH
		  Where FTS_CONTRATO = @V_Contrato
		    And FTS_DIA < @D_Fecalt
		 Fetch Next From Contratos Into @V_Contrato, @D_Fecalt
		End
	  Close Contratos
	  Deallocate Contratos
	END

   */

    If NOT EXISTS(Select 'x' FROM METDB.MET_FORECASTSH WHERE FTS_FECSYS = CONVERT(date, GETDATE()))
	    Begin
			Open Forecast
			FETCH NEXT FROM FORECAST Into @V_Dia, @V_Contrato,@V_Cup, @V_Cliente, @V_CAUDAL, @V_Tarifa, @V_Presion, @V_Provincia, @V_Fecalt, @V_fecven, @V_Cogen, @V_DIFINTRA, @V_D07EST, @V_CONSDIA; 
			WHILE @@FETCH_STATUS = 0  
			 Begin
			   Begin Try 
			       Insert Into METDB.MET_FORECASTSH (FTS_FECSYS,     FTS_VERSION,    FTS_DIA,     FTS_CUP_CODIGO, FTS_CLI_NIF,
					   							     FTS_PRV_CODIGO, FTS_TAR_CODIGO, FTS_PRESION, FTS_CAUDAL,     FTS_DIFINTRA,
												     FTS_D07EST,     FTS_CONSMES,    FTS_CONSDIA, FTS_FECALT,     FTS_FECVEN,
												     FTS_COGEN,      FTS_CONTRATO,   FTS_ORIGEN) 
										     Values (CONVERT(DATE, GETDATE()),       1,         @V_Dia,      @V_Cup,         @V_Cliente,
											    	 @V_Provincia,   @V_Tarifa,      @V_Presion,  @V_CAUDAL,      @V_DIFINTRA,
                                   				     @V_D07EST,       Null,          @V_CONSDIA,  @V_Fecalt,      @V_fecven,
												     @V_Cogen,       @V_Contrato,    'A') 								 
				End Try
				Begin CATCH 
				       IF ERROR_NUMBER() = 2627 -- Número de error para violación de clave única
                          BEGIN
                            PRINT 'El valor ya existe en la tabla.';
                          END
				End CATCH
			   FETCH NEXT FROM FORECAST Into @V_Dia,@V_Contrato, @V_Cup, @V_Cliente, @V_CAUDAL, @V_Tarifa, @V_Presion, @V_Provincia, @V_Fecalt, @V_fecven, @V_Cogen, @V_DIFINTRA, @V_D07EST, @V_CONSDIA;  									 
			 End
			Close Forecast	
		 Declare Version_Max Cursor For
		   Select fts_cup_codigo, FTS_DIA, max(fts_version), max(fts_fecsys)
		     From METDB.VW_DIASANYO, metdb.MET_FORECASTSH
		    Where DIA Between datetrunc(dayofyear,getdate()) and eomonth(dateadd(MONTH, 1,datetrunc(dayofyear,getdate())))
			  And dia = FTS_DIA
			  And fts_version != 1
			  And FTS_ORIGEN != 'M'
			Group by fts_cup_codigo,FTS_DIA
		 Open Version_Max
		 FETCH NEXT FROM Version_Max Into  @V_Cup, @V_Dia, @V_Version, @V_Fecalt;
		 WHILE @@FETCH_STATUS = 0  
		  Begin
		   Select @V_CONSDIA = FTS_CONSDIA 
		     From METDB.MET_FORECASTSH
			Where FTS_FECSYS = @V_Fecalt
			  And FTS_DIA = @V_Dia
			  And FTS_CUP_CODIGO = @V_Cup
			  And FTS_VERSION = @V_Version;

           Update METDB.MET_FORECASTSH
		      Set FTS_VERSION = @V_Version,
			      FTS_CONSDIA = @V_CONSDIA
            Where FTS_FECSYS = CONVERT(DATE, GETDATE())
			  And FTS_DIA = @V_Dia
			  And FTS_CUP_CODIGO = @V_Cup 
		   FETCH NEXT FROM Version_Max Into  @V_Cup, @V_Dia, @V_Version, @V_Fecalt;
		  End
        Close Version_Max
	    Deallocate Version_Max 
        End
	 Else
	    Begin
		   Open Actualiza_D07
		   FETCH NEXT FROM Actualiza_D07 Into @V_Cup, @V_DIFINTRA, @V_D07EST
		   While @@FETCH_STATUS = 0
		     Begin
			   Update METDB.MET_FORECASTSH
			      Set FTS_DIFINTRA = @V_DIFINTRA,
					  FTS_D07EST = @V_D07EST
                Where FTS_CUP_CODIGO = @V_Cup
				  And FTS_FECSYS = CONVERT(date, GETDATE());
			   FETCH NEXT FROM Actualiza_D07 Into @V_Cup, @V_DIFINTRA, @V_D07EST     
			 End
           Close Actualiza_D07
		 End
    Deallocate Forecast  
	Deallocate Actualiza_D07 
	set @V_FechaDia = (SELECT CONVERT(VARCHAR,CONVERT(DATE,GETDATE())))
	EXEC METDB.PR_CVW_FORECAST @V_FechaDia

 END 
