/****** Object:  StoredProcedure [METDB].[PR_LIMPIAR_CONSUMOS_POWER]    Script Date: 25/06/2025 12:17:13 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [METDB].[PR_LIMPIAR_CONSUMOS_POWER] AS
DECLARE
  @V_Cup Varchar(30),
  @D_Fecha Date,
  @V_Tipo Varchar(5),
  @N_Hora int,
  @V_Existe Varchar(1);

  Declare Consumos_CCH Cursor For 
   Select CSP_CUP_CODIGO, csp_fecha, CSP_TIPO, CSP_HORA
     From metdb.MET_consupower
    Group by CSP_CUP_CODIGO, csp_fecha, CSP_TIPO, CSP_HORA
    Having count(*) > 1        
BEGIN
   Open Consumos_CCH
	FETCH NEXT FROM Consumos_CCH Into @V_Cup, @D_Fecha, @V_Tipo, @N_Hora 
	WHILE @@FETCH_STATUS = 0  
	 Begin
	    Set @V_Existe = (Select 'S' 
		                   From METDB.MET_CONSUPOWER 
						  Where CSP_CUP_CODIGO = @V_Cup
		                    And CSP_FECHA = @D_Fecha
		                    And CSP_TIPO = @V_Tipo
		                    And CSP_HORA = @N_Hora 
		                    And CSP_PROCED = 'CCH')
       If @V_Existe = 'S' 	               
	    Begin 
		   BEGIN TRANSACTION;  
			Delete METDB.MET_CONSUPOWER
			 Where CSP_CUP_CODIGO = @V_Cup
			   And CSP_FECHA = @D_Fecha
			   And CSP_TIPO = @V_Tipo
			   And CSP_HORA = @N_Hora 
			   And CSP_PROCED != 'CCH'
		   Commit TRANSACTION;  
         End
       Else 
	     Begin
           BEGIN TRANSACTION;  
      	    Delete METDB.MET_CONSUPOWER
		     Where CSP_CUP_CODIGO = @V_Cup
		       And CSP_FECHA = @D_Fecha
		       And CSP_TIPO = @V_Tipo
		       And CSP_HORA = @N_Hora 
		       And CSP_PROCED != 'Prediccion'
           Commit TRANSACTION;  
   	     End   
	    FETCH NEXT FROM Consumos_CCH Into @V_Cup, @D_Fecha, @V_Tipo, @N_Hora  
	 End
	Close Consumos_CCH
    Deallocate Consumos_CCH
END
