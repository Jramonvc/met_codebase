USE [SigeMET]
GO
/****** Object:  StoredProcedure [dbo].[MET_CARGPRECIOS]    Script Date: 25/06/2025 13:12:59 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[MET_CARGPRECIOS] AS
DECLARE
 @D_Fecha date,
 @D_Mes   Date,
 @V_Indice varchar(30),
 @N_Precio float,
 @N_MIBGAS bigint,
 @N_OMIE   bigint,
 @N_TTFD   bigint,
 @N_TTFM   bigint;
 
  Declare Precios Cursor For
    Select FECHA, INDICE, PRECIO/1000
     From  sigemet.dbo.met_precios
	Where INDICE != 'TTFM'
    ORDER BY INDICE

  Declare Precios_TTFM Cursor For
     Select DATEADD(MONTH,-1,DATEADD(DAY, 1, EOMONTH(Fecha, -1))), INDICE, PRECIO/1000, DIA
       From  sigemet.dbo.met_precios, SIGEMET.DBO.VW_DIASANYO
 	  Where INDICE = 'TTFM'
	    AND DATEADD(MONTH,-1,DATEADD(DAY, 1, EOMONTH(Fecha, -1))) = DATEADD(DAY, 1, EOMONTH(DIA, -1))
	  group by DATEADD(MONTH,-1,DATEADD(DAY, 1, EOMONTH(Fecha, -1))), INDICE, PRECIO, DIA

BEGIN
   Set @N_MIBGAS = (Select max(IdPrecioOperadorGas) from sigemet.dbo.PrecioOperadorGas)
   Set @N_OMIE   = (Select max(IdPrecioOmieGas) from sigemet.dbo.PrecioOmieGas)
   Set @N_TTFD   = (Select max(IdPrecioTtfSpamHeren) from sigemet.dbo.PrecioTtfSpamHeren)
   Set @N_TTFM   = (Select max(IdPrecioTtfMadamHeren) from sigemet.dbo.PrecioTtfMadamHeren)
   
   OPEN Precios;
   FETCH NEXT FROM Precios INTO @D_Fecha, @V_Indice, @N_Precio;
   WHILE @@FETCH_STATUS = 0
    BEGIN
        If @V_Indice = 'MIBGAS' 
		   Begin
		      If NOT EXISTS(Select 'x' From SigeMET.dbo.PrecioOperadorGas Where FechaOperadorGas = @D_Fecha)  
			     Begin
				   Set @N_MIBGAS = @N_MIBGAS + 1;
				   SET IDENTITY_INSERT sigemet.dbo.PrecioOperadorGas ON
				   Insert Into SigeMET.dbo.PrecioOperadorGas (IdPrecioOperadorGas, Entorno, FechaOperadorGas, PrecioOperadorGas)
				                                      Values (@N_MIBGAS,           'G2',    @D_Fecha,         @N_Precio)
				   SET IDENTITY_INSERT sigemet.dbo.PrecioOperadorGas OFF
				 End
			  Else
			     Begin
				   Update SigeMET.dbo.PrecioOperadorGas
				      Set PrecioOperadorGas = @N_Precio
                    Where FechaOperadorGas = @D_Fecha 
				 End
		   End
		Else
		  Begin
		    If @V_Indice = 'MIBGDA'
			  Begin
			    If NOT EXISTS(Select 'x' From SigeMET.dbo.PrecioOmieGas Where FechaOmieGas = @D_Fecha)    
				   Begin
				    Set @N_OMIE = @N_OMIE + 1
					SET IDENTITY_INSERT sigemet.dbo.PrecioOmieGas ON
					Insert Into Sigemet.dbo.PrecioOmieGas (IdPrecioOmieGas, Entorno, FechaOmieGas, PrecioOmieGas)
					                               Values (@N_OMIE,         'G2',    @D_Fecha,     @N_Precio)
                    SET IDENTITY_INSERT sigemet.dbo.PrecioOmieGas OFF
                   End
			    Else
				  Begin
				    Update SigeMET.dbo.PrecioOmieGas
					   Set PrecioOmieGas = @N_Precio
                     Where FechaOmieGas = @D_Fecha 
				  End
			  End
            Else
			   Begin
			     If @V_Indice = 'TTFD'
				   Begin
				     If NOT EXISTS(Select 'x' From sigemet.dbo.PrecioTtfSpamHeren Where FechaTtfSpamHeren = @D_Fecha)
					     Begin
						   Set @N_TTFD = @N_TTFD + 1
						   SET IDENTITY_INSERT sigemet.dbo.PrecioTtfSpamHeren ON
						   Insert Into Sigemet.dbo.PrecioTtfSpamHeren (IdPrecioTtfSpamHeren, Entorno, FechaTtfSpamHeren, PrecioTtfSpamHeren)
						                                       Values (@N_TTFD,              'G2',    @D_Fecha,          @N_Precio)
						   SET IDENTITY_INSERT sigemet.dbo.PrecioTtfSpamHeren OFF
						 End
					 Else
					     Begin
						   Update SigeMET.dbo.PrecioTtfSpamHeren
						      Set PrecioTtfSpamHeren = @N_Precio
                            Where FechaTtfSpamHeren = @D_Fecha
						 End
				   End   
			   End
          End     
       FETCH NEXT FROM Precios INTO @D_Fecha, @V_Indice, @N_Precio;
    End
	CLOSE Precios;
    DEALLOCATE Precios;
	OPEN Precios_TTFM;
    FETCH NEXT FROM Precios_TTFM INTO @D_Mes, @V_Indice, @N_Precio,@D_Fecha;
	WHILE @@FETCH_STATUS = 0
	 Begin
	   If NOT EXISTS(Select 'x' From sigemet.dbo.PrecioTtfMadamHeren Where FechaTtfMadamHeren = @D_Fecha) 
		  Begin
		   Set @N_TTFM = @N_TTFM + 1
		   SET IDENTITY_INSERT sigemet.dbo.PrecioTtfMadamHeren ON 
		     Insert Into Sigemet.dbo.PrecioTtfMadamHeren (IdPrecioTtfMadamHeren, Entorno, FechaTtfMadamHeren, PrecioTtfMadamHeren)
			                                      Values (@N_TTFM,               'G2',    @D_Fecha,           @N_Precio)
           SET IDENTITY_INSERT sigemet.dbo.PrecioTtfMadamHeren OFF
		  End
		Else  
		  Begin
		    Update SigeMET.dbo.PrecioTtfMadamHeren 
			   Set PrecioTtfMadamHeren = @N_Precio
             where FechaTtfMadamHeren =  @D_Fecha
	      End
	   FETCH NEXT FROM Precios_TTFM INTO @D_Mes, @V_Indice, @N_Precio,@D_Fecha;
	 End		
	Delete sigemet.dbo.met_precios
    CLOSE Precios_TTFM;
    DEALLOCATE Precios_TTFM;

End	
