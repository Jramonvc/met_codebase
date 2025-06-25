/****** Object:  StoredProcedure [METDB].[PR_KYCTYPE_RECAL]    Script Date: 23/06/2025 12:23:08 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER  PROCEDURE [METDB].[PR_KYCTYPE_RECAL] AS
DECLARE 					
	 @N_CSMID int,
	 @V_Nif Varchar(30),  
	 @D_Fecini Date,
	 @V_Kyc Varchar(15),					
   	 @N_Valor_Pai_CNA Numeric(10,2),
	 @N_Valor_Vol Numeric(10,2),
	 @N_Volumen Numeric (10,2);
	Declare Contratos Cursor  For 
	    Select CNT_CLI_NIF,  
		       CASE WHEN PAI_RANK > 1 AND PAI_RANK <= 60 THEN 7
			        WHEN PAI_RANK > 60 AND PAI_RANK <= 120 THEN 17.5
				    ELSE 35 END + CNA_PUNTSCOR
		  From METDB.MET_CNAE, METDB.MET_PAISES, METDB.MET_CONTRATOS, METDB.MET_DIRECCIONES, METDB.MET_CLIENTES
         Where (CONVERT(DATE, GETDATE()) Between CNT_FECALT AND CNT_FECVEN
		      OR CNT_FECALT > CONVERT(DATE, GETDATE()))
          And CNT_CNS_CODIGO IN ('C','P','T')
		  And CNT_CNA_ID = CNA_ID
		  AND CNT_CLI_NIF = DIR_CLI_NIF
          And DIR_PAI_CODIGO = PAI_CODIGO 
		  And CNT_CLI_NIF = CLI_NIF
		  --And CLI_KTP_ID IS NULL 
 BEGIN
	 Open Contratos
	FETCH NEXT FROM Contratos INTO @V_Nif, @N_Valor_Pai_CNA
	WHILE @@FETCH_STATUS = 0  
	  Begin
	    Set @N_Volumen = isnull((SELECT SUM(VOLUMEN) 
		                        FROM (SELECT (SUM(VCD_VOLUMEN) / DATEDIFF(DAY, CSM_FECINI, CSM_FECFIN))*365 Volumen 
	                                    FROM METDB.MET_CONTSML, METDB.MET_VOLCONTDET
		                               Where CSM_CLI_NIF = @V_Nif
								         AND (CONVERT(DATE, GETDATE()) Between CSM_FECINI AND CSM_FECFIN
		                                  OR CSM_FECFIN > CONVERT(DATE, GETDATE()))
		                                 AND CSM_ID  = VCD_CSM_ID
								         AND (CONVERT(DATE, GETDATE()) Between CSM_FECINI And CSM_FECFIN Or CSM_FECINI > CONVERT(DATE, GETDATE()))
								       GROUP BY DATEDIFF(DAY, CSM_FECINI, CSM_FECFIN))A),0) 
					     + isnull((Select SUM(VOLUMEN)
						               From (Select (SUM(PVM_VOLUMEN)/DATEDIFF(DAY, CNT_FECALT, CNT_FECVEN))*365 Volumen
                                               From metdb.met_contratos, METDB.MET_POWVOLUMEN
	                                         Where CNT_CLI_NIF = @V_Nif
	                                           And cnt_cns_codigo in ('C','T','P')
	                                           And CNT_CUP_CODIGO = PVM_CUP_CODIGO
	                                         GROUP BY DATEDIFF(DAY, CNT_FECALT, CNT_FECVEN))B),0)
		If @N_Volumen <= 5000  
	        Set @V_Kyc  = '1' 
	    Else 
	      Begin
	        if @N_Volumen > 5000 AND @N_Volumen <= 100000 
		       Set @N_Valor_Vol = 8
            Else if  @N_Volumen > 100000 AND @N_Volumen <= 500000 
		       Set @N_Valor_Vol = 20
            Else
		       Set @N_Valor_Vol = 40 
            If @N_Valor_Pai_CNA + @N_Valor_Vol <= 30 
		        Set @V_Kyc  = '1' 
            Else if @N_Valor_Pai_CNA + @N_Valor_Vol > 30 And @N_Valor_Pai_CNA + @N_Valor_Vol < 50
		        Set @V_Kyc  = '2'
            Else
		        Set @V_Kyc  = '3'
           End
	    Update METDB.MET_CLIENTES 
		   Set CLI_KTP_ID = @V_Kyc
         Where CLI_NIF = @V_Nif
	    FETCH NEXT FROM Contratos INTO @V_Nif, @N_Valor_Pai_CNA
	  End
   	Close Contratos
    Deallocate Contratos
END

