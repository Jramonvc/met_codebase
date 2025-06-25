/****** Object:  StoredProcedure [METDB].[MET_CALCCOMISION_MENSUAL]    Script Date: 25/06/2025 12:08:02 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [METDB].[MET_CALCCOMISION_MENSUAL] AS
DECLARE
 @N_Codcontrato Varchar(15),
 @V_Cup Varchar(24),
 @V_Cliente Varchar(15),
 @V_Agente Varchar(6),
 @N_Potencia Float,
 @N_Comision Float,
 @N_Comision_D Float,
 @N_Decomision Float,
 @N_Id Float,
 @N_MargPot Float,
 @N_Consumo Float,
 @V_Numfact Varchar(255),
 @N_Comunit float,
 @D_Fecalt date,
 @N_Consumo_Fact numeric(18,2);

BEGIN
 Declare Comisiones_Con Cursor For
    Select FPW_CNT_CODIGO, FPW_CLI_NIF, FPW_CUP_CODIGO, CAG_COMISION, CAG_AGE_CODIGO, FPW_NUMFACT, FPW_TOTCONS, FPW_TOTCONS*CAG_COMISION
	  From metdb.MET_CONTRACTCOMIAG, METDB.MET_FACTPOWER
	 Where CAG_TIPCOMI = 'C'
	   And CAG_AGE_CODIGO  NOT IN ('118','165','161','231')
	   And CAG_CNT_CODIGO = FPW_CNT_CODIGO
	   And FPW_FECFACT between CAG_FECINI AND ISNULL(CAG_FECFIN, FPW_FECFACT)
	   And FPW_FECFACT between DATEFROMPARTS(YEAR(DATEADD(MONTH, -5, getdate())),MONTH(DATEADD(MONTH, -5, getdate())),1) And EOMONTH(DATEADD(MONTH, -1, getdate()))
	   And FPW_NUMFACT IS NOT NULL
	   And NOT EXISTS (SELECT 'X' FROM METDB.MET_CALCCOMISION WHERE CCM_NUMFACTURA = FPW_NUMFACT)
  Open Comisiones_Con
  FETCH NEXT From Comisiones_Con Into @N_Codcontrato, @V_Cliente, @V_Cup,  @N_Comunit, @V_Agente, @V_Numfact, @N_Consumo, @N_Comision;
  While @@Fetch_Status = 0 
    Begin
	  SELECT @N_ID = NEXT VALUE FOR METDB.CCM_SQ;
      Insert into METDB.MET_CALCCOMISION (CCM_ID,         CCM_FECPAGO,              CCM_CNT_CODIGO, CCM_CLI_NIF,  CCM_CUP_CODIGO, 
		                                  CCM_AGE_CODIGO, CCM_TIPCOMI,              CCM_COMIUNIT,   CCM_CONSUMO,  CCM_COMISION,
										  CCM_NUMFACTURA)
								  Values (@N_ID,          convert(date, getdate()), @N_Codcontrato, @V_Cliente,   @V_Cup,
                                          @V_Agente,      'F',                      @N_Comunit,     @N_Consumo,  @N_Comision,
										  @V_Numfact) 				 
	  FETCH NEXT From Comisiones_Con Into @N_Codcontrato, @V_Cliente, @V_Cup,  @N_Comunit, @V_Agente, @V_Numfact, @N_Consumo, @N_Comision;
      
    End 	
 CLOSE Comisiones_Con
 DEALLOCATE Comisiones_Con

 Declare Comisiones_C_T Cursor For
    Select cnt_codigo, CNT_CLI_NIF, CNT_CUP_CODIGO,CNT_POTP1,CPW_COMISION, CNT_AGE_CODIGO
      From metdb.MET_CONTRATOS, metdb.MET_GRTARIF, METDB.MET_COMIAGENTPW
     Where CNT_FECALT Between convert(date, '2024-05-01') and  getdate()
       And CNT_AGE_CODIGO NOT IN ('118','165','161','231')
       And CNT_POTP1 is not null
       And CNT_CNS_CODIGO IN ('B','C')
       And CNT_GTF_CODIGO = gtf_codigo
       And (CHARINDEX ('VSS', GTF_DENOM) != 0 Or CHARINDEX ('HH', GTF_DENOM) != 0)
       And CPW_AGE_CODIGO = CNT_AGE_CODIGO
       And CPW_TIPPROD = CASE when CHARINDEX ('PFIJO', GTF_DENOM) != 0 Then 'F' 
	                          when CHARINDEX ('PFUN', GTF_DENOM) != 0 Then 'F'
							  when CHARINDEX ('PFPER', GTF_DENOM) != 0 Then 'F'
	                         ELSE 'I' END
	   And CNT_TAR_CODIGO = CPW_TAR_CODIGO
	   And CNT_GTF_CODIGO = CPW_GTF_CODIGO
       And CNT_POTP1 Between CPW_MINPOT AND CPW_MAXPOT
	   And CNT_FECALT Between CPW_FECINI And IsNull(CPW_FECFIN, CNT_FECALT)
       And Not exists (Select 'X' From METDB.MET_CALCCOMISION Where CCM_CNT_CODIGO = CNT_CODIGO)
  Open Comisiones_C_T
  FETCH NEXT From Comisiones_C_T Into @N_Codcontrato, @V_Cliente, @V_Cup, @N_Potencia, @N_Comision, @V_Agente;
  While @@Fetch_Status = 0 
    Begin
	  SELECT @N_ID = NEXT VALUE FOR METDB.CCM_SQ;
      Insert into METDB.MET_CALCCOMISION (CCM_ID,         CCM_FECPAGO,              CCM_CNT_CODIGO, CCM_CLI_NIF,  CCM_CUP_CODIGO, 
		                                  CCM_AGE_CODIGO, CCM_TIPCOMI,              CCM_COMIUNIT,   CCM_POTENCIA, CCM_COMISION)
								  Values (@N_ID,          convert(date, getdate()), @N_Codcontrato, @V_Cliente,   @V_Cup,
                                          @V_Agente,      'C',                      @N_Comision,    @N_Potencia,  @N_Comision) 				 
	  FETCH NEXT From Comisiones_C_T Into @N_Codcontrato, @V_Cliente, @V_Cup, @N_Potencia, @N_Comision, @V_Agente; 
      
    End 	
 CLOSE Comisiones_C_T
 DEALLOCATE Comisiones_C_T

 Declare Comisiones_C Cursor For
    Select cnt_codigo, CNT_CLI_NIF, CNT_CUP_CODIGO,CNT_POTP1,CPW_COMISION, CNT_AGE_CODIGO
      From metdb.MET_CONTRATOS, metdb.MET_GRTARIF, METDB.MET_COMIAGENTPW
     Where CNT_FECALT Between convert(date, '2024-05-01') and  getdate()
       And CNT_AGE_CODIGO NOT IN ('118','165','161','231')
       And CNT_POTP1 is not null
       And CNT_CNS_CODIGO IN ('B','C')
       And CNT_GTF_CODIGO = gtf_codigo
       And (CHARINDEX ('VSS', GTF_DENOM) != 0 Or CHARINDEX ('HH', GTF_DENOM) != 0)
       And CPW_AGE_CODIGO = CNT_AGE_CODIGO
       And CPW_TIPPROD = CASE when CHARINDEX ('PFIJO', GTF_DENOM) != 0 Then 'F' 
	                          when CHARINDEX ('PFUN', GTF_DENOM) != 0 Then 'F'
							  when CHARINDEX ('PFPER', GTF_DENOM) != 0 Then 'F'
	                         ELSE 'I' END
       And CNT_POTP1 Between CPW_MINPOT AND CPW_MAXPOT
	   And CNT_FECALT Between CPW_FECINI And IsNull(CPW_FECFIN, CNT_FECALT)
       And Not exists (Select 'X' From METDB.MET_CALCCOMISION Where CCM_CNT_CODIGO = CNT_CODIGO)
 Open Comisiones_C
  FETCH NEXT From Comisiones_C Into @N_Codcontrato, @V_Cliente, @V_Cup, @N_Potencia, @N_Comision, @V_Agente;
  While @@Fetch_Status = 0 
    Begin
	  SELECT @N_ID = NEXT VALUE FOR METDB.CCM_SQ;
      Insert into METDB.MET_CALCCOMISION (CCM_ID,         CCM_FECPAGO,              CCM_CNT_CODIGO, CCM_CLI_NIF,  CCM_CUP_CODIGO, 
		                                  CCM_AGE_CODIGO, CCM_TIPCOMI,              CCM_COMIUNIT,   CCM_POTENCIA, CCM_COMISION)
								  Values (@N_ID,          convert(date, getdate()), @N_Codcontrato, @V_Cliente,   @V_Cup,
                                          @V_Agente,      'C',                      @N_Comision,    @N_Potencia,  @N_Comision) 				 
	  FETCH NEXT From Comisiones_C Into @N_Codcontrato, @V_Cliente, @V_Cup, @N_Potencia, @N_Comision, @V_Agente; 
      
    End 	
 CLOSE Comisiones_C
 DEALLOCATE Comisiones_C
 
 Declare Renovaciones Cursor For
     Select cnt_codigo, CNT_CLI_NIF, CNT_CUP_CODIGO,CNT_POTP1,CPW_RENOVA, CNT_AGE_CODIGO
       From metdb.MET_CONTRATOS, metdb.MET_GRTARIF, METDB.MET_COMIAGENTPW
      Where (CNT_CNS_CODIGO = 'C' 
		        Or (CNT_CNS_CODIGO = 'B' 
			    And (DATEDIFF (day,CNT_FECALT, CNT_FECVEN) > 365 And Year(CNT_FECVEN) = Year(dateadd(month, -1, getdate())))))
        And CNT_AGE_CODIGO NOT IN ('118','165','161','231')
        And CNT_POTP1 is not null
        And CNT_CNS_CODIGO IN ('B','C')
        And CNT_GTF_CODIGO = gtf_codigo
        And (CHARINDEX ('VSS', GTF_DENOM) != 0 Or CHARINDEX ('HH', GTF_DENOM) != 0)
        And CPW_AGE_CODIGO = CNT_AGE_CODIGO
        And CPW_TIPPROD = CASE CHARINDEX ('PFIJO', GTF_DENOM) WHEN '0' Then 'I' ELSE 'F' END
        And CNT_POTP1 Between CPW_MINPOT AND CPW_MAXPOT 
		And CNT_FECALT Between CPW_FECINI And IsNull(CPW_FECFIN, CNT_FECALT)
        And MONTH(CNT_FECALT) = MONTH(DATEADD(MONTH,-1,GETDATE()))
        And YEAR(CNT_FECALT) <= YEAR(DATEADD(MONTH,-1,GETDATE())) - 1 
        And Not exists (Select 'X' From METDB.MET_CALCCOMISION Where CCM_CNT_CODIGO = CNT_CODIGO AND YEAR(CCM_FECPAGO) = YEAR(getdate()) And CCM_COMISION > 0)
  Open Renovaciones
  FETCH NEXT From Renovaciones Into @N_Codcontrato, @V_Cliente, @V_Cup, @N_Potencia, @N_Comision, @V_Agente;
  While @@Fetch_Status = 0 
    Begin
      SELECT @N_ID = NEXT VALUE FOR METDB.CCM_SQ;
      Insert into METDB.MET_CALCCOMISION (CCM_ID,         CCM_FECPAGO,              CCM_CNT_CODIGO, CCM_CLI_NIF,  CCM_CUP_CODIGO, 
		                                  CCM_AGE_CODIGO, CCM_TIPCOMI,              CCM_COMIUNIT,   CCM_POTENCIA, CCM_COMISION)
								  Values (@N_ID,          convert(date, getdate()), @N_Codcontrato, @V_Cliente,   @V_Cup,
                                          @V_Agente,      'R',                      @N_Comision,    @N_Potencia,  @N_Comision)	
	  FETCH NEXT From Renovaciones Into  @N_Codcontrato, @V_Cliente, @V_Cup, @N_Potencia, @N_Comision, @V_Agente;
    End
 CLOSE Renovaciones
 DEALLOCATE Renovaciones
   
 Declare Decomisiones Cursor For
    Select cnt_codigo, CNT_CLI_NIF, CNT_CUP_CODIGO, CNT_POTP1, CCM_COMISION, CNT_AGE_CODIGO,
	        CASE WHEN DATEDIFF(DAY,CNT_FECALT, CNT_FECVEN) = 0 Then CCM_COMISION * -1
				ELSE
		        abs(datediff(day,isnull(CNT_FECBAJ,CNT_FECVEN), 
                    case when month(CNT_FECALT) - month(isnull(CNT_FECBAJ,CNT_FECVEN)) < 0 then dateadd(year,datediff(year, CNT_FECALT, isnull(CNT_FECBAJ,CNT_FECVEN))+1, CNT_FECALT)
					when month(CNT_FECALT) - month(isnull(CNT_FECBAJ,CNT_FECVEN)) = 0 And day(CNT_FECALT) - day(isnull(CNT_FECBAJ,CNT_FECVEN)) > 0 Then dateadd(year,datediff(year, CNT_FECALT, isnull(CNT_FECBAJ,CNT_FECVEN)), CNT_FECALT)
					when month(CNT_FECALT) - month(isnull(CNT_FECBAJ,CNT_FECVEN)) = 0 And day(CNT_FECALT) - day(isnull(CNT_FECBAJ,CNT_FECVEN)) <= 0 Then dateadd(year,datediff(year, CNT_FECALT, isnull(CNT_FECBAJ,CNT_FECVEN))+1, CNT_FECALT)
					else dateadd(year,datediff(year, CNT_FECALT, isnull(CNT_FECBAJ,CNT_FECVEN)), CNT_FECALT) end)* CCM_COMISION/365) * -1 END CCM_COMISION
      From metdb.MET_CONTRATOS C1, metdb.MET_GRTARIF, METDB.MET_CALCCOMISION
     Where isnull(CNT_FECBAJ,CNT_FECVEN) Between DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, getdate())),MONTH(DATEADD(MONTH, -1, getdate())),1) And EOMONTH(DATEADD(MONTH, -1, getdate()))
       And CNT_AGE_CODIGO NOT IN ('118','165','161','231')
       And CNT_POTP1 is not null
       And CNT_CNS_CODIGO = 'B'
       And CNT_GTF_CODIGO = gtf_codigo
       And CHARINDEX ('VSS', GTF_DENOM) != 0
       And CCM_ID = (Select Max(CCM_ID) From METDB.MET_CALCCOMISION Where CCM_CNT_CODIGO = CNT_CODIGO)
       And Not exists (Select 'X' From METDB.MET_CALCCOMISION Where CCM_CNT_CODIGO = CNT_CODIGO AND YEAR(CCM_FECPAGO) = YEAR(getdate()) And CCM_COMISION < 0)  
	   And NOT EXISTS (SELECT 'X' FROM METDB.MET_CONTRATOS C2 WHERE C2.CNT_CUP_CODIGO = C1.CNT_CUP_CODIGO AND CNT_CNS_CODIGO IN ('C','T'))
 
  Open Decomisiones
  FETCH NEXT From Decomisiones Into @N_Codcontrato, @V_Cliente, @V_Cup, @N_Potencia, @N_Comision, @V_Agente, @N_Decomision;
	
  While @@Fetch_Status = 0 
    Begin  
	  SELECT @N_ID = NEXT VALUE FOR METDB.CCM_SQ;
      Insert into METDB.MET_CALCCOMISION (CCM_ID,         CCM_FECPAGO,              CCM_CNT_CODIGO, CCM_CLI_NIF,  CCM_CUP_CODIGO, 
		                                  CCM_AGE_CODIGO, CCM_TIPCOMI,              CCM_COMIUNIT,   CCM_POTENCIA, CCM_COMISION)
								  Values (@N_ID,          convert(date, getdate()), @N_Codcontrato, @V_Cliente,   @V_Cup,
                                          @V_Agente,      'D',                      @N_Comision,    @N_Potencia,  @N_Decomision)
	  FETCH NEXT From Decomisiones Into  @N_Codcontrato, @V_Cliente, @V_Cup, @N_Potencia, @N_Comision, @V_Agente, @N_Decomision;
    End 	
   CLOSE Decomisiones
   DEALLOCATE Decomisiones  
  Declare Comisiones_O Cursor For 
    SELECT CAG_AGE_CODIGO, CAG_CNT_CODIGO, CNT_CLI_NIF, CNT_CUP_CODIGO, CAG_COMISION, CAG_POTENCIA, CAG_CONSUMO, (CNT_POTP1 + CNT_POTP2 + ISNULL(CNT_POTP3,0) + ISNULL(CNT_POTP4,0) + ISNULL(CNT_POTP5,0) + ISNULL(CNT_POTP6,0)) Potencia
	  FROM METDB.MET_CONTRACTCOMIAG, METDB.MET_CONTRATOS
	  WHERE CAG_TIPCOMI = 'O'
	   AND CAG_AGE_CODIGO NOT IN ('118','165','161','231')
	   AND CAG_CNT_CODIGO = CNT_CODIGO
	   AND CNT_CNS_CODIGO = 'C'
	   AND convert(date, getdate()) Between CAG_FECINI And isnull(CAG_FECFIN, convert(date, getdate()))
	   AND NOT EXISTS (Select 'X' 
	                     From METDB.MET_CALCCOMISION
						Where CCM_CNT_CODIGO = CNT_CODIGO
						  And DATEDIFF(day, CCM_FECPAGO, GETDATE()) < 365
						  And CCM_NUMFACTURA IS NULL)
  Open Comisiones_O
  FETCH NEXT From Comisiones_O Into @V_Agente, @N_Codcontrato, @V_Cliente, @V_Cup, @N_Comision,  @N_MargPot, @N_Consumo, @N_Potencia;
  While @@FETCH_STATUS = 0
    Begin
	  SELECT @N_ID = NEXT VALUE FOR METDB.CCM_SQ;
      Insert into METDB.MET_CALCCOMISION (CCM_ID,         CCM_FECPAGO,              CCM_CNT_CODIGO, CCM_CLI_NIF,  CCM_CUP_CODIGO, 
		                                  CCM_AGE_CODIGO, CCM_TIPCOMI,              CCM_COMIUNIT,   CCM_POTENCIA, CCM_MARGPOT,
										  CCM_CONSUMO,    CCM_COMISION)
								  Values (@N_ID,          convert(date, getdate()), @N_Codcontrato, @V_Cliente,   @V_Cup,
                                          @V_Agente,      'O',                      @N_Comision,    @N_Potencia,  @N_MargPot,
										  @N_Consumo,     (@N_Comision * @N_Consumo) + (Isnull(@N_MargPot,0) * @N_Potencia/1000))   
     FETCH NEXT From Comisiones_O Into @V_Agente, @N_Codcontrato, @V_Cliente, @V_Cup, @N_Comision,  @N_MargPot, @N_Consumo, @N_Potencia; 
	End
  Close Comisiones_O
  DEALLOCATE Comisiones_O
  Declare Decomision_O Cursor For 
      SELECT CAG_AGE_CODIGO, CAG_CNT_CODIGO, CNT_CLI_NIF, CNT_CUP_CODIGO, CAG_COMISION, CAG_POTENCIA, CAG_CONSUMO, (CNT_POTP1 + CNT_POTP2 + CNT_POTP3 + CNT_POTP4 + CNT_POTP5 + CNT_POTP6) Potencia, CNT_FECALT  
	    FROM METDB.MET_CONTRACTCOMIAG, METDB.MET_CONTRATOS C1
       WHERE CAG_TIPCOMI = 'O'
         AND CAG_AGE_CODIGO NOT IN ('118','165','161','231')
	     AND CAG_CNT_CODIGO = CNT_CODIGO
	     AND CNT_CNS_CODIGO = 'B'
	     AND CNT_FECVEN  between DATEFROMPARTS(YEAR(DATEADD(MONTH, -2, getdate())),MONTH(DATEADD(MONTH, -2, getdate())),1) And EOMONTH(DATEADD(MONTH, -2, getdate()))
		 AND NOT EXISTS (SELECT 'X' FROM METDB.MET_CALCCOMISION WHERE CCM_CNT_CODIGO = CNT_CODIGO AND CCM_COMISION < 0)
		 AND NOT EXISTS (SELECT 'X' FROM METDB.MET_CONTRATOS C2 WHERE C2.CNT_CUP_CODIGO = C1.CNT_CUP_CODIGO AND CNT_CNS_CODIGO IN ('C','T'))
  Open Decomision_O
  FETCH NEXT FROM Decomision_O Into @V_Agente, @N_Codcontrato, @V_Cliente, @V_Cup, @N_Comision,  @N_MargPot, @N_Consumo, @N_Potencia, @D_Fecalt
  While @@FETCH_STATUS = 0
    Begin
	  Set @N_Consumo_Fact = (Select isnull(SUM(fpw_totcons),0)
	                           From METDB.MET_FACTPOWER
	                          Where FPW_CNT_CODIGO = @N_Codcontrato
	                            And FPW_FECFACT >= @D_Fecalt)
      If @N_Consumo > @N_Consumo_Fact And @N_Consumo_Fact != 0 
	     Begin 
		    SELECT @N_ID = NEXT VALUE FOR METDB.CCM_SQ;
			Set @N_Comision_D = (@N_Consumo_Fact - @N_Consumo) * (@N_Comision)
            Insert into METDB.MET_CALCCOMISION (CCM_ID,         CCM_FECPAGO,              CCM_CNT_CODIGO, CCM_CLI_NIF,  CCM_CUP_CODIGO, 
		                                        CCM_AGE_CODIGO, CCM_TIPCOMI,              CCM_COMIUNIT,   CCM_POTENCIA, CCM_MARGPOT,
										        CCM_CONSUMO,    CCM_COMISION)
								        Values (@N_ID,          convert(date, getdate()), @N_Codcontrato, @V_Cliente,   @V_Cup,
                                                @V_Agente,      'D',                      @N_Comision,    @N_Potencia,  @N_MargPot,
										        @N_Consumo_Fact - @N_Consumo, @N_Comision_D)   
		 End 
	  FETCH NEXT FROM Decomision_O Into @V_Agente, @N_Codcontrato, @V_Cliente, @V_Cup, @N_Comision,  @N_MargPot, @N_Consumo, @N_Potencia, @D_Fecalt  
	End
  Close Decomision_O
  Deallocate Decomision_O
END