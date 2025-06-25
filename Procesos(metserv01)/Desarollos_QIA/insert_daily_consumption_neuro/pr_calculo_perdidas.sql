/****** Object:  StoredProcedure [METDB].[PR_CALCULO_PERDIDAS]    Script Date: 23/06/2025 12:28:13 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [METDB].[PR_CALCULO_PERDIDAS] AS
DECLARE
   @V_Tarifa Varchar(10),
   @V_Cup Varchar(24),
   @V_Proced Varchar(20),
   @V_TIpo Varchar(1),
   @N_Hora Numeric(2),
   @D_Fecha Date,
   @N_Perdidas Numeric(15,12),
   @V_Provincia Varchar(2);



    Declare Perdidas Cursor For
       Select CSP_CUP_CODIGO, CSP_FECHA, CNT_TAR_CODIGO, CSP_PROCED, CSP_TIPO, CSP_HORA, CUP_PRV_CODIGO
	     from METDB.MET_CONSUPOWER, METDB.MET_CONTRATOS, METDB.MET_CUPS
		Where CSP_LECTURASPERD Is Null
		  And CSP_CUP_CODIGO = CUP_CODIGO
		  And CUP_CODIGO = CNT_CUP_CODIGO 
		  AND CSP_FECHA BETWEEN CNT_FECALT AND ISNULL(CNT_FECVEN, CSP_FECHA)
	       


BEGIN
 Open Perdidas
  FETCH NEXT FROM Perdidas INTO @V_Cup, @D_Fecha, @V_Tarifa,@V_Proced, @V_Tipo, @N_Hora,@V_Provincia

	WHILE @@FETCH_STATUS = 0  
    Begin
	 BEGIN TRANSACTION;  
	   Set @N_Perdidas = (Select 1+PFL_REALLOSS
	                     From METDB.MET_PROFILES
                         Where PFL_TAR_CODIGO = @V_Tarifa
						   And PFL_DATE = @D_Fecha
						   And PFL_HORA = @N_Hora - 1
						   And PFL_ZNS_ID = CASE WHEN @V_Provincia IN ('35','38') THEN 'Canarias' 
	                                           WHEN @V_Provincia = '07' Then 'Baleares'
			                                   WHEN @V_Provincia = '51' Then 'Ceuta'
			                                   WHEN @V_Provincia = '52' Then 'Melilla'
			                                   Else  'Peninsula' END );
       Update METDB.MET_CONSUPOWER
	      Set CSP_LECTURASPERD = CSP_LECTURAS * @N_Perdidas
		Where CSP_CUP_CODIGO = @V_Cup
		  And CSP_FECHA = @D_Fecha
		  And CSP_HORA = @N_Hora
		  And CSP_TIPO = @V_Tipo
		  And CSP_PROCED = @V_Proced
        COMMIT TRANSACTION; 
	   FETCH NEXT FROM Perdidas INTO @V_Cup, @D_Fecha, @V_Tarifa,@V_Proced, @V_Tipo, @N_Hora, @V_Provincia
	End 	 
  CLOSE Perdidas
  DEALLOCATE Perdidas;	 
  
End
