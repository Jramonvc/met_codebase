/****** Object:  StoredProcedure [METDB].[CARGA_CLIENTES]    Script Date: 23/06/2025 12:25:09 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [METDB].[CARGA_CLIENTES] AS
 Declare
  @V_TipCol float,
  @V_Nif varchar(15),
  @V_Error Varchar(100),
  @V_ViaCod Varchar(6),
  @V_NIF_Aux nVarchar(15),
  @V_NOMBRE nvarchar(100),    
  @V_APELLIDO1 nvarchar(100), 
  @V_APELLIDO2 nvarchar(100),
  @V_RAZSOC nvarchar(100),    
  @V_CODEXTNAV nvarchar(20), 
  @V_CIUDAD nvarchar(100),    
  @V_NUMERO nvarchar(100),
  @V_ACLARADOR nvarchar(100), 
  @V_CODPOSTAL nvarchar(5),  
  @V_CALLE nvarchar(100),     
  @V_VIA nvarchar(100),
  @V_SEGMENTO nvarchar(2),  
  @V_IBAN nvarchar(24),      
  @V_DIAPAGO int,   
  @V_EMAIL nvarchar(300),     
  @V_MOVIL nvarchar(60),     
  @V_TELEFONO nvarchar(60),   
  @V_FPGCOD nvarchar(6), 
  @V_LETRA nvarchar(1), 
  @V_PRVCOD nvarchar(2),
  @V_Existe nvarchar(1);
 
Begin
   Declare Clientes Cursor For 
     SELECT NIF,       NOMBRE,    APELLIDO1, APELLIDO2,
            RAZSOC,    CODEXTNAV, CIUDAD,    NUMERO,
            ACLARADOR, RIGHT('0' + CODPOSTAL,5) CODPOSTAL, CALLE,     VIA,
            SUBSTRING(CATEGORIA,1,2) SEGMENTO,  IBAN,      DIAPAGO,   
            EMAIL,     MOVIL,     TELEFONO,  FPG_CODIGO, SUBSTRING(NIF,1,1) LETRA, PRV_CODIGO
       From METDB.TMP_CLIENTES, METDB.MET_FORMPAGS, METDB.MET_PROVINCIAS
	  Where FORMPAGO = FPG_DENOM
		And SUBSTRING(RIGHT('0' + CODPOSTAL,5),1,2) = PRV_CODIGO 
    Open Clientes
	FETCH NEXT FROM CLIENTES INTO @V_NIF,@V_NOMBRE, @V_APELLIDO1, @V_APELLIDO2, 
								  @V_RAZSOC, @V_CODEXTNAV, @V_CIUDAD, @V_NUMERO,
                                  @V_ACLARADOR, @V_CODPOSTAL, @V_CALLE, @V_VIA, 
								  @V_SEGMENTO, @V_IBAN, @V_DIAPAGO, 
								  @V_EMAIL, @V_MOVIL, @V_TELEFONO, @V_FPGCOD, @V_LETRA, @V_PRVCOD;

	WHILE @@FETCH_STATUS = 0  
    Begin
	  SET @V_TipCol = (Select COL_CODIGO
		                From METDB.MET_COLECTIVOS
	                   Where COL_LETRA = @V_LETRA);
      If @V_TipCOl Is Null 
        Begin
		  exec METDB.ver_nif @V_NIF,@V_NIF_Aux output,@V_Error output
		  If @V_Error != '0' 
		       set @V_TipCol = '21'; 
		  Else
		       set @V_TipCol = '1'; 
        End 		
		
	   Set @V_Existe = (Select 'S' FROM METDB.MET_CLIENTES WHERE CLI_NIF = @V_Nif);
		If @V_Existe = 'S' 
		 Begin
		     Update METDB.MET_CLIENTES
                Set CLI_COL_CODIGO = @V_TipCol,           
                    CLI_RAZSOC = @V_RAZSOC,       
                    CLI_NOMBRE = @V_NOMBRE,
                    CLI_APELLIDO1 = @V_APELLIDO1,   
                    CLI_APELLIDO2 = @V_APELLIDO2,   
                    CLI_CCL_CODIGO = ISNULL(@V_SEGMENTO,'DO'),
                    CLI_CODNAVI = @V_CODEXTNAV,     
                    CLI_FPG_CODIGO = @V_FPGCOD,   
                    CLI_IBAN = @V_IBAN,
                    CLI_DIAPAG = @V_DIAPAGO,
                    CLI_TELEF = @V_TELEFONO,       
                    CLI_MOVIL = @V_MOVIL,       
                    CLI_EMAIL = @V_EMAIL        
              Where CLI_NIF = @V_Nif;
	      End		   
		Else
		  Begin
             INSERT INTO METDB.MET_CLIENTES (CLI_COL_CODIGO,  CLI_NIF,         CLI_RAZSOC,       CLI_NOMBRE,
                                             CLI_APELLIDO1,   CLI_APELLIDO2,   CLI_PAI_CODIGO,   CLI_CCL_CODIGO,
                                             CLI_CODNAVI,     CLI_FPG_CODIGO,  CLI_IBAN,         CLI_DIAPAG,
                                             CLI_TELEF,       CLI_MOVIL,       CLI_EMAIL,        CLI_CONTACTO)
	   	   			               VALUES   (@V_TipCol,       @V_Nif,          @V_RAZSOC,        @V_NOMBRE,
                                             @V_APELLIDO1,    @V_APELLIDO2,    'ES',             ISNULL(@V_SEGMENTO,'DO'),
                                             @V_CODEXTNAV,    @V_FPGCOD,       @V_IBAN,          @V_DIAPAGO,
                                             @V_TELEFONO,     @V_MOVIL,        @V_EMAIL,         Null);       		
		  End
		
		Set @V_ViaCOD = (Select VIA_CODIGO
	                     From METDB.MET_TIPVIAS
                         Where VIA_DENOM = @V_VIA);
	    If @v_ViaCod Is Null 
	          set @v_ViaCod = '00'; 					 
	    Set @V_Existe = (Select 'S' 
                           From METDB.MET_DIRECCIONES
                          Where DIR_TPD_CODIGO = 'FC'
                            And DIR_CODIGO = @V_Nif
                            And DIR_SUJETO = 'C');						 
	    If @V_Existe = 'S' 
		  Begin
	         Update METDB.MET_DIRECCIONES
               Set DIR_VIA_CODIGO = @v_ViaCod,  
                   DIR_UBICAC = IsNull(@V_CALLE,' ')+' '+ISNULL(@V_NUMERO,''),   
                   DIR_ACLARADOR = @V_ACLARADOR,
                   DIR_POBLAC = @V_CIUDAD,      
                   DIR_CODPOS = @V_CODPOSTAL,   
                   DIR_PRV_CODIGO = @V_PRVCOD,
                   DIR_TELEFONO =  @V_TELEFONO
             Where DIR_TPD_CODIGO = 'FC'
               And DIR_CODIGO = @V_Nif
               And DIR_SUJETO = 'C';  
          End 
        Else	  

		     Insert Into METDB.MET_DIRECCIONES (DIR_TPD_CODIGO,  DIR_CODIGO,   DIR_SUJETO,
                                                DIR_VIA_CODIGO,  DIR_UBICAC,   DIR_ACLARADOR,
                                                DIR_POBLAC,      DIR_CODPOS,   DIR_PRV_CODIGO,
                                                DIR_PAI_CODIGO,  DIR_TITULAR,  DIR_TELEFONO,
							    		        DIR_CLI_NIF,     DIR_USERNAME, DIR_FECSYS)
							             Values('FC',            @V_Nif,      'C',
							                    @v_ViaCod,       ISNULL(@V_CALLE,' ')+' '+ISNULL(@V_NUMERO,''), @V_ACLARADOR,
									            @V_CIUDAD,       @V_CODPOSTAL, @V_PRVCOD,
									            'ES',            NULL,         @V_TELEFONO,
									            @V_Nif,         'MET',         getdate());
		
	  FETCH NEXT FROM CLIENTES INTO @V_NIF,@V_NOMBRE, @V_APELLIDO1, @V_APELLIDO2, 
								    @V_RAZSOC, @V_CODEXTNAV, @V_CIUDAD, @V_NUMERO,
                                    @V_ACLARADOR, @V_CODPOSTAL, @V_CALLE, @V_VIA, 
								    @V_SEGMENTO, @V_IBAN, @V_DIAPAGO, 
								    @V_EMAIL, @V_MOVIL, @V_TELEFONO, @V_FPGCOD, @V_LETRA, @V_PRVCOD;								
					
    End	
	CLOSE CLIENTES
    DEALLOCATE CLIENTES;
	EXEC METDB.CARGA_CUPS;
	EXEC METDB.CARGA_CONTRATOS;
	EXEC METDB.MET_PRECFIJO;
    EXEC METDB.PR_CARGA_TERMVAR;
    EXEC METDB.PR_FACTPOWER;
	DELETE metdb.TMP_CONTRATOS;
	DELETE METDB.TMP_CLIENTES;
	DELETE METDB.TMP_CUPS;
	DELETE METDB.TMP_CONTTARIF;
	DELETE METDB.TMP_PREGTAR;
	DELETE METDB.TMP_TERMVARS;
	DELETE METDB.TMP_TERMVARC;
	DELETE METDB.TMP_CONTPOTENCIA;
	DELETE METDB.TMP_FACTPOWER;
END
