/****** Object:  StoredProcedure [METDB].[PR_CONTPOWER]    Script Date: 24/06/2025 12:01:43 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [METDB].[PR_CONTPOWER] AS
DECLARE  
 @V_CUP VARCHAR(30),
 @V_COD_CNAE VARCHAR(10),
 @V_TARATR VARCHAR(10),
 @V_TARIFA VARCHAR(100),
 @N_POTP1 FLOAT,
 @N_POTP2 FLOAT,
 @N_POTP3 FLOAT,
 @N_POTP4 FLOAT,
 @N_POTP5 FLOAT,
 @N_POTP6 FLOAT,
 @V_ESTADO VARCHAR(100),
 @V_CODIGO VARCHAR(30),
 @V_DIRPDS VARCHAR(300),
 @V_POBLACPDS VARCHAR(100),
 @V_ACLRPDS VARCHAR(200),
 @V_CPPDS VARCHAR(5),
 @D_FECINI DATE,
 @D_FECFIN DATE,
 @N_PLAPAGO INT,
 @N_DIAPAG INT,
 @V_NOMCLI VARCHAR(100),
 @V_APELLIDO1 VARCHAR(100),
 @V_APELLIDO2 VARCHAR(100),
 @V_RAZSOCIAL VARCHAR(100),
 @V_EMAIL VARCHAR(300),
 @V_NIF VARCHAR(40),
 @V_TELCLI VARCHAR(100),
 @V_EMAIL2 VARCHAR(300),
 @V_CONTACTO VARCHAR(200),
 @V_ACLRCLI VARCHAR(200),
 @V_DIRCLI VARCHAR(300),
 @V_CPCLI VARCHAR(5),
 @V_POBLACLI VARCHAR(100),
 @D_FECFIR DATE,
 @V_AGENTE VARCHAR(100),
 @V_FORMPAG VARCHAR(100),
 @V_IBAN VARCHAR(30),
 @N_GTFCOD int,
 @V_Tipcli VARCHAR(2),
 @V_Letra VARCHAR(1),
 @V_TipCol float,
 @V_NIF_Aux Varchar(15),
 @V_Error Varchar(100),
 @V_FPGCOD Varchar(6),
 @V_TipVia Varchar(30),
 @V_ViaCOD Varchar(6),
 @V_CODAGE int,  
 @V_CNAE int,
 @V_SUBESTADO int,
 @N_Consumanual float,
 @N_Consestimado Float,
 @V_Distribuidora Varchar(10),
 @N_FEECT Numeric(6,3),
 @D_FECCRE Date,
 @V_ModFecha Varchar(1);
 Declare TarPower Cursor For 
     Select Upper(TARIFA), TARATR 
       From METDB.TMP_CONTRATNEURO
	  where TARATR IS NOT NULL
	  Group by TARATR,TARIFA
	 Except
     Select GTF_DENOM, TAR_CODERP
       From METDB.MET_GRTARIF, METDB.MET_TARIFAS
      Where GTF_TAR_CODIGO = TAR_CODIGO;
 
 Declare Cliente Cursor For
     SELECT PLAPAGO,DIAPAG,NOMCLI,APELLIDO1,APELLIDO2,RAZSOCIAL,EMAIL,
            NIF,TELCLI,CONTACTO,ACLRCLI, Upper(VIACLI), DIRCLI, RIGHT(CPCLI, 5) CODPOSTAL,
            POBLACLI, FORMPAG,IBAN,CASE charindex('VSS',tarifa, 1) When 0 Then 'IN' ELSE 'DO' END SEGMENTO,
			SUBSTRING(NIF,1,1) LETRA
       FROM METDB.TMP_CONTRATNEURO
	  Where NIF is not null
      GROUP BY PLAPAGO,DIAPAG,NOMCLI,APELLIDO1,APELLIDO2,RAZSOCIAL, VIACLI, CPCLI,
               EMAIL,NIF,TELCLI,CONTACTO,ACLRCLI,DIRCLI,POBLACLI, FORMPAG,IBAN,TARIFA
 Declare Agente Cursor For
        SELECT AGENTE
          FROM METDB.TMP_CONTRATNEURO
		  where AGENTE is not null
         GROUP BY AGENTE
        EXCEPT
       SELECT AGE_DENOM 
         FROM METDB.MET_AGENTES 
 Declare ContPower Cursor For 
   SELECT CUPS, NIF, GTF_CODIGO, GTF_TAR_CODIGO, POTP1,POTP2,POTP3,POTP4,POTP5,POTP6,
          CASE SUBESTADO 
		     WHEN '15' Then 'A'
			 ELSE
	      case upper(estado)
		        when 'ACTIVABLE' Then 'T'
				when 'ACTIVO' Then 'C'
				when 'CANCELADO' Then 'A'
				when 'FINALIZADO' Then 'B'
				when 'PENDIENTE' Then 'P' END END ESTADO,
           CODIGO,DIRPDS,CPPDS, Upper(VIAPDS), ACLRPDS, POBLACPDS,
		   FECINI,FECFIN,PLAPAGO,DIAPAG,FECFIR,FORMPAG,IBAN, AGE_CODIGO, CNA_ID, CONSANUAL, CASE SUBESTADO WHEN '24' THEN NULL ELSE SUBESTADO END SUBESTADO, CONSESTIMADO, DISTRIBUIDORA,
		   MARGEN, FECCRE, CASE WHEN CHARINDEX('VSS', GTF_DENOM, 1) != 0  THEN 'S' ELSE 'N' END
      FROM METDB.TMP_CONTRATNEURO LEFT JOIN METDB.MET_AGENTES ON AGENTE = AGE_DENOM, METDB.MET_CNAE, METDB.MET_GRTARIF
	 WHERE CNAE = CNA_CODIGO 
	   AND TARATR = GTF_TAR_CODIGO
	   AND UPPER(TARIFA) = GTF_DENOM 
	   --  AND GTF_DENOM != 'INDEFINIDO'
	   
 BEGIN
    Open TarPower
    FETCH NEXT From TarPower Into  @V_TARIFA, @V_TARATR;
    While @@Fetch_Status = 0 
      Begin 
	    If NOT EXISTS(Select 'x' From METDB.MET_TARIFAS Where TAR_CODIGO = @V_TARATR)  
		 
	        Insert Into METDB.MET_TARIFAS(TAR_CODIGO, TAR_DENOM, TAR_TIPPEAJE, TAR_CODERP, TAR_GRUPO)
		                           VALUES(@V_TARATR,  @V_TARATR, 'P',          @V_TARATR,  'P');
	   
	    SELECT @N_GTFCOD = NEXT VALUE FOR RISK_MGMT_SPAIN.METDB.GTF_SQ;
	    INSERT INTO METDB.MET_GRTARIF (GTF_CODIGO, GTF_DENOM, GTF_TAR_CODIGO)
                               VALUES (@N_GTFCOD, UPPER(@V_TARIFA), @V_TARATR);		
        	
	   FETCH NEXT From TarPower Into @V_TARIFA, @V_TARATR;   
      End
    CLOSE TarPower;
    DEALLOCATE TarPower; 
    
    Open Cliente
    FETCH NEXT From Cliente Into @N_PLAPAGO, @N_DIAPAG, @V_NOMCLI,    @V_APELLIDO1, @V_APELLIDO2, @V_RAZSOCIAL, 
								 @V_EMAIL,   @V_NIF,    @V_TELCLI,    @V_CONTACTO,  @V_ACLRCLI,   @V_TIPVIA,  
								 @V_DIRCLI,  @V_CPCLI,  @V_POBLACLI,  @V_FORMPAG,   @V_IBAN,      @V_Tipcli,  
								 @V_Letra  	
	While @@Fetch_Status = 0 
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
		  Set @V_FPGCOD = (Select FPG_CODIGO
                               From METDB.MET_FORMPAGS
                              Where FPG_DENOM LIKE Case @V_FORMPAG When 'Transferencia' Then 'TRANSFERENCIA' ELSE 'DOMICILIADO' END + '%'
                                And FPG_DCOBRO = ISNULL(@N_PLAPAGO,0)
								And FPG_FECLECT = 'N')	
	    If NOT Exists(Select 'X' From METDB.MET_CLIENTES WHERE  CLI_NIF = @V_NIF) 
		 Begin 
		    If @V_FPGCOD Is Not Null 	
		          INSERT INTO METDB.MET_CLIENTES (CLI_COL_CODIGO,  CLI_NIF,         CLI_RAZSOC,       CLI_NOMBRE,
                                                  CLI_APELLIDO1,   CLI_APELLIDO2,   CLI_PAI_CODIGO,   CLI_CCL_CODIGO,
                                                  CLI_CODNAVI,     CLI_FPG_CODIGO,  CLI_IBAN,         CLI_DIAPAG,
                                                  CLI_TELEF,       CLI_MOVIL,       CLI_EMAIL,        CLI_CONTACTO)
	   	   			                    VALUES   (@V_TipCol,       @V_Nif,          @V_RAZSOCIAL,     @V_NOMCLI,
                                                  @V_APELLIDO1,    @V_APELLIDO2,    'ES',             @V_Tipcli,
                                                  Null,            @V_FPGCOD,       @V_IBAN,          @N_DIAPAG,
                                                  @V_TELCLI,       Null,            @V_EMAIL,         @V_CONTACTO);
         End
		Else
		 Begin
		   Update METDB.MET_CLIENTES 
		     SET CLI_COL_CODIGO = @V_TipCol,           
                 CLI_RAZSOC = @V_RAZSOCIAL,       
                 CLI_NOMBRE = @V_NOMCLI,
                 CLI_APELLIDO1 = @V_APELLIDO1,   
                 CLI_APELLIDO2 = @V_APELLIDO2,   
                 CLI_CCL_CODIGO = @V_Tipcli,
                 CLI_FPG_CODIGO = @V_FPGCOD,   
                 CLI_IBAN = @V_IBAN,
                 CLI_DIAPAG = @N_DIAPAG,
                 CLI_TELEF = @V_TELCLI,      
                 CLI_EMAIL = @V_EMAIL  
			WHERE CLI_NIF = @V_NIF
		 End
       Set @V_ViaCOD = (Select VIA_CODIGO
	                      From METDB.MET_TIPVIAS
                         Where VIA_DENOM = @V_TipVia );
	   If @v_ViaCod Is Null 
	     Begin
	       set @v_ViaCod = '00';
           If Not Exists (Select 'X' 
			   	            From METDB.MET_DIRECCIONES
                           Where DIR_TPD_CODIGO = 'FC'
                             And DIR_CODIGO = @V_Nif
                             And DIR_SUJETO = 'C')
                 Begin
                    Insert Into METDB.MET_DIRECCIONES (DIR_TPD_CODIGO,  DIR_CODIGO,   DIR_SUJETO,
                                                       DIR_VIA_CODIGO,  DIR_UBICAC,   DIR_ACLARADOR,
                                                       DIR_POBLAC,      DIR_CODPOS,   DIR_PRV_CODIGO,
                                                       DIR_PAI_CODIGO,  DIR_TITULAR,  DIR_TELEFONO,
				        		                       DIR_CLI_NIF,     DIR_USERNAME, DIR_FECSYS)
					                            Values('FC',            @V_Nif,      'C',
					                                    @v_ViaCod,       @V_DIRCLI,    @V_ACLRCLI,
							                            @V_POBLACLI,     @V_CPCLI,     substring(@V_CPCLI,1,2),
							                           'ES',            NULL,         @V_TELCLI,
							                           @V_Nif,         'MET',         getdate());								  
			     End
           Else
		    Update METDB.MET_DIRECCIONES
               Set DIR_VIA_CODIGO = @v_ViaCod,  
                   DIR_UBICAC = @V_DIRCLI,   
                   DIR_ACLARADOR = @V_ACLRCLI,
                   DIR_POBLAC = @V_POBLACLI,      
                   DIR_CODPOS = @V_CPCLI,   
                   DIR_PRV_CODIGO = substring(@V_CPCLI,1,2),
                   DIR_TELEFONO =  @V_TELCLI
             Where DIR_TPD_CODIGO = 'FC'
               And DIR_CODIGO = @V_Nif
               And DIR_SUJETO = 'C'; 
         End
	     FETCH NEXT From Cliente Into @N_PLAPAGO, @N_DIAPAG, @V_NOMCLI,    @V_APELLIDO1, @V_APELLIDO2, @V_RAZSOCIAL, 
		 						      @V_EMAIL,   @V_NIF,    @V_TELCLI,    @V_CONTACTO,  @V_ACLRCLI,   @V_TIPVIA,  
								      @V_DIRCLI,  @V_CPCLI,  @V_POBLACLI,  @V_FORMPAG,   @V_IBAN,      @V_Tipcli,  
								      @V_Letra   	
	  End
    CLOSE Cliente;
    DEALLOCATE Cliente;	
	
	Open Agente
	FETCH NEXT FROM Agente INTO @V_Agente;
	WHILE @@FETCH_STATUS = 0  
    Begin
	   SELECT @V_CODAGE = NEXT VALUE FOR METDB.AGE_SQ;
		  INSERT INTO METDB.MET_AGENTES(AGE_CODIGO, AGE_DENOM)
                                 VALUES (@V_CODAGE, UPPER(@V_AGENTE));
	   FETCH NEXT FROM Agente INTO @V_Agente;	 
	End 	 
	CLOSE Agente;
    DEALLOCATE Agente;	 

    Open ContPower


    FETCH NEXT From ContPower Into @V_CUP,       @V_NIF,     @N_GTFCOD,    @V_TARIFA,  @N_POTP1,   @N_POTP2,  @N_POTP3,  
	                               @N_POTP4,     @N_POTP5,   @N_POTP6,     @V_ESTADO,  @V_CODIGO,  @V_DIRPDS, @V_CPPDS,   
								   @V_TIPVIA,    @V_ACLRPDS, @V_POBLACPDS, @D_FECINI,  @D_FECFIN,  @N_PLAPAGO, @N_DIAPAG, @D_FECFIR,    
								   @V_FORMPAG,   @V_IBAN,    @V_CODAGE,    @V_CNAE,    @N_Consumanual, @V_SUBESTADO, @N_Consestimado, @V_Distribuidora,
								   @N_FEECT,     @D_FECCRE,  @V_ModFecha
	While @@Fetch_Status = 0 
      Begin
	    Set @V_ViaCOD = (Select VIA_CODIGO
	                       From METDB.MET_TIPVIAS
                          Where VIA_DENOM = @V_TIPVIA);
	    If @v_ViaCod Is Null 
	         set @v_ViaCod = '00'; 
	    If Not Exists(Select 'X' From METDB.MET_CUPS WHERE CUP_CODIGO = @V_CUP)
           Begin
		      
			   INSERT INTO METDB.MET_CUPS (CUP_CODIGO, CUP_VIA_CODIGO, CUP_UBICAC,     
                                           CUP_NUMVIA, CUP_ACLARADOR,  CUP_POBLAC,     
                                           CUP_CODPOS, CUP_PRV_CODIGO, CUP_PAI_CODIGO,
                                           CUP_QD,     CUP_DIS_CODIGO)		 
	                               VALUES (@V_CUP,     @V_ViaCOD,      @V_DIRPDS,   
                                           Null,       Null,           @V_POBLACPDS,      
	   					                   @V_CPPDS,   substring(@V_CPPDS,1,2),       'ES',
                                           @N_Consumanual, @V_Distribuidora); 		 
           End	
		Else
		   Begin
		     Update METDB.MET_CUPS
			    Set CUP_QD =  @N_Consumanual,
				    CUP_VIA_CODIGO = @V_ViaCOD,
					CUP_ACLARADOR = @V_ACLRPDS,
					CUP_POBLAC = @V_POBLACPDS,
					CUP_UBICAC = @V_DIRPDS,
					CUP_DIS_CODIGO = @V_Distribuidora
			  Where CUP_CODIGO = @V_CUP;
		   End
        Set @V_FPGCOD = (Select TOP (1) FPG_CODIGO
                               From METDB.MET_FORMPAGS
                              Where FPG_DENOM LIKE Case @V_FORMPAG When 'Transferencia' Then 'TRANSFERENCIA' ELSE 'DOMICILIADO' END + '%'
                                And FPG_DCOBRO = ISNULL(@N_PLAPAGO,0)) 
		If Not Exists(Select 'X' From METDB.MET_CONTRATOS WHERE CNT_CODIGO = @V_CODIGO)
            Begin
			   Insert Into METDB.MET_CONTRATOS (CNT_CODIGO,     CNT_CLI_NIF,    CNT_CUP_CODIGO,
								  	            CNT_FECALT,     CNT_FECBAJ,     CNT_FECVEN,
									            CNT_TAR_CODIGO, CNT_GTF_CODIGO, CNT_CAUDAL,
									            CNT_CNS_CODIGO, CNT_FPG_CODIGO, CNT_IBAN,
									            CNT_DIAPAG,     CNT_AGE_CODIGO, CNT_CNA_ID,
										        CNT_POTP1,      CNT_POTP2,      CNT_POTP3,
										        CNT_POTP4,      CNT_POTP5,      CNT_POTP6,
												CNT_CSS_ID,     CNT_CONSESTIMADO, CNT_FEECT,
											    CNT_FECCONTR)		
								         Values(@V_CODIGO,      @V_NIF,         @V_CUP,
									            @D_FECINI,      Null,           @D_FECFIN,
									            @V_Tarifa,      @N_GTFCOD,      @N_Consumanual,
									            @V_ESTADO,      @V_FPGCOD,      @V_IBAN,
									            @N_DIAPAG,      @V_CODAGE,      @V_CNAE,
										        @N_POTP1,       @N_POTP2,       @N_POTP3,  
										        @N_POTP4,       @N_POTP5,       @N_POTP6,
												@V_SUBESTADO,   @N_Consestimado, @N_FEECT,
												@D_FECCRE);
            End			
          Else
		    Begin
			  Update METDB.MET_CONTRATOS 
			     Set CNT_CNS_CODIGO = @V_ESTADO,
				     CNT_FECALT = CASE WHEN @D_FECINI IS Null And CNT_FECALT Is Not Null Then CNT_FECALT ELSE  @D_FECINI END,
					 CNT_FECVEN = CASE @V_ModFecha When 'S' Then @D_FECFIN ELSE CNT_FECVEN END,
					 CNT_FECBAJ = CASE @V_ESTADO When 'B' Then  @D_FECFIN else CNT_FECBAJ END,
					 CNT_TAR_CODIGO = @V_TARIFA,
					 CNT_GTF_CODIGO = @N_GTFCOD,
					 CNT_POTP1      = @N_POTP1, 
					 CNT_AGE_CODIGO = @V_CODAGE,
					 CNT_POTP2 =@N_POTP2,      
					 CNT_POTP3 = @N_POTP3,
					 CNT_POTP4 = @N_POTP4,      
					 CNT_POTP5 = @N_POTP5,      
					 CNT_POTP6 = @N_POTP6,
					 CNT_CAUDAL = @N_Consumanual,
					 CNT_CSS_ID = @V_SUBESTADO,
					 CNT_CONSESTIMADO = @N_Consestimado,
					 CNT_CUP_CODIGO = @V_CUP,
					 CNT_CLI_NIF = @V_NIF,
					 CNT_IBAN = @V_IBAN,
					 CNT_CNA_ID = @V_CNAE,
					 CNT_DIAPAG = @N_DIAPAG,
					 CNT_FPG_CODIGO = @V_FPGCOD,
					 CNT_FEECT = ISNULL(@N_FEECT, CNT_FEECT),
					 CNT_FECCONTR = @D_FECCRE
               Where CNT_CODIGO = @V_CODIGO;
			End
	FETCH NEXT From ContPower Into @V_CUP,       @V_NIF,     @N_GTFCOD,    @V_TARIFA,  @N_POTP1,   @N_POTP2,  @N_POTP3,  
	                               @N_POTP4,     @N_POTP5,   @N_POTP6,     @V_ESTADO,  @V_CODIGO,  @V_DIRPDS, @V_CPPDS,   
								   @V_TIPVIA,    @V_ACLRPDS, @V_POBLACPDS, @D_FECINI,  @D_FECFIN,  @N_PLAPAGO, @N_DIAPAG, @D_FECFIR,
								   @V_FORMPAG,   @V_IBAN,    @V_CODAGE,    @V_CNAE,    @N_Consumanual, @V_SUBESTADO, @N_Consestimado, @V_Distribuidora,
								   @N_FEECT,    @D_FECCRE,   @V_ModFecha
      End;	  
	CLOSE ContPower;
    DEALLOCATE ContPower;  
	delete METDB.TMP_CONTRATNEURO;
	exec METDB.PR_CARGA_POWERPRIC;
	exec metdb.PR_FACTPOWER;
	END;
