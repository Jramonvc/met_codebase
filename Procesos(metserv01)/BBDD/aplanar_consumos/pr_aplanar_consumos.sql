/****** Object:  StoredProcedure [METDB].[PR_APLANAR_CONSUMOS]    Script Date: 23/06/2025 12:17:23 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO













 ALTER PROCEDURE [METDB].[PR_APLANAR_CONSUMOS] AS 
 DECLARE 
   @V_Numfact Varchar(26),
   @V_Cup Varchar(20),
   @V_Numfactorig Varchar(26),
   @V_Seudofact Varchar(26),
   @D_Fecini  Date,
   @D_Fecfin  Date,
   @V_Rangpres Varchar(2),
   @V_Metodo Varchar(1),
   @V_Telemedida Varchar(1),
   @V_Peaje Varchar(2),
   @V_Tipo Varchar(60),
   @N_ConsLab float,
   @N_ConsFind float;
   
   
   Declare Facturas Cursor For
     Select FCB_NUMFACT, FCB_CUP, FCB_SEUDOFACT
       From metdb.MET_FACTCABDIST B1, METDB.met_factlectdist c
      where B1.FCB_ESTADO = 'N'
	    And B1.FCB_CLASFACT != 'A' 
        And FCB_TIPFACT != CASE B1.FCB_EMPEMI WHEN '0234' Then '99' else '11' END  
		And B1.FCB_NUMFACT = C.FLD_FCB_NUMFACT 
		And B1.FCB_TIPFACT = C.FLD_FCB_TIPFACT 
        And B1.FCB_CLASFACT = C.FLD_FCB_CLASFACT
        And B1.FCB_CUP = C.FLD_FCB_CUP  
        And B1.FCB_SEUDOFACT = C.FLD_FCB_SEUDOFACT  
        And NOT EXISTS (Select 'X'
                          From METDB.MET_FACTCABDIST  B2
                         Where B2.FCB_CUP = b1.FCB_CUP
				           And B2.FCB_NUMFACTORIG = CASE WHEN b1.FCB_SEUDOFACT = '0' THEN b1.FCB_NUMFACT ELSE b1.FCB_SEUDOFACT END
						   AND (B2.FCB_CLASFACT != 'B' OR (B2.FCB_CLASFACT = 'B' And B2.FCB_METFACT = 1)))
	   	And Not Exists (Select 'X'
                               From risk_mgmt_spain.METDB.MET_FACTCABDIST B3, risk_mgmt_spain.METDB.MET_FACTLECTDIST C2
                              Where B3.FCB_CUP = B1.FCB_CUP
                                And C2.FLD_FECLECACTCAL= C.FLD_FECLECACTCAL
                                And B3.FCB_NUMFACT = C2.FLD_FCB_NUMFACT
                                And B3.FCB_TIPFACT  = C2.FLD_FCB_TIPFACT
                                And B3.FCB_CLASFACT = C2.FLD_FCB_CLASFACT   
                                And B3.FCB_CUP  = C2.FLD_FCB_CUP  
                                And B3.FCB_SEUDOFACT = C2.FLD_FCB_SEUDOFACT 
                                And cast(B3.FCB_CODPROD as int) < cast(B1.FCB_CODPROD as int))
	Group By FCB_NUMFACT, FCB_CUP, FCB_SEUDOFACT;
 BEGIN

   Declare Abonos Cursor For
     Select FCB_CUP, FCB_NUMFACTORIG, FCB_NUMFACT, FCB_SEUDOFACT
       From metdb.MET_FACTCABDIST B1
      where  FCB_ESTADO = 'N'
	    AND FCB_NUMFACTORIG != '0'
		--And FCB_CLASFACT != 'B'
		AND (FCB_CLASFACT != 'B' OR (FCB_CLASFACT = 'B' And FCB_METFACT = 1));
   Open Abonos
   Fetch Next From abonos Into  @V_Cup, @V_Numfactorig, @V_Numfact, @V_Seudofact;

      WHILE @@FETCH_STATUS = 0
        Begin   
	      Delete METDB.MET_CONSUMDIARIO
		   Where CSD_CUP_CODIGO = @V_Cup 
			 And CASE WHEN CSD_SEUDOFACT = '0' THEN CSD_NUMFACT ELSE CSD_SEUDOFACT END = @V_Numfactorig;
         /* Update METDB.MET_FACTCABDIST
	         Set FCB_ESTADO = 'P'
	   	   Where FCB_CUP = @V_Cup
		     And FCB_NUMFACT  = @V_Numfact
		     And FCB_SEUDOFACT = @V_Seudofact
			 And FCB_ESTADO = 'N'*/
	      Fetch Next From Abonos Into @V_Cup, @V_Numfactorig, @V_Numfact, @V_Seudofact;      
	    End
  
     CLOSE Abonos;
     DEALLOCATE Abonos;
   Open Facturas 
   Fetch Next From Facturas Into @V_Numfact, @V_Cup, @V_Seudofact;
   WHILE @@FETCH_STATUS = 0
     Begin
	    Declare DetLect Cursor For 
	   SELECT FECANT, FECACT, RANGPRES, METODO, TELEMEDIDA, PEAJE, 
              CASE 
			    WHEN TELEMEDIDA='N' AND RANGPRES IN ('3','4','6','03','04','06') THEN 'Tipo 1 (P>4 bar)'
                WHEN TELEMEDIDA='N' AND RANGPRES IN ('1','01') AND PEAJE NOT IN ('R1','R2','R3') THEN 'Tipo 1 (P<=4 bar)'
                WHEN TELEMEDIDA='N' AND RANGPRES IN ('1','01') AND PEAJE IN ('R1','R2','R3') THEN 'Tipo 2'
                WHEN  RANGPRES IN ('2','02')  THEN 'GNL'
                WHEN TELEMEDIDA='S' THEN 'TM'
                ELSE 'Otro' 
			  END,
              CASE 
                WHEN datediff(day,FECANT,FECACT) IN (-1,0,1) THEN  CONSUMO_KWH
                WHEN datediff(day,FECANT,FECACT) NOT IN (-1,0,1) AND (RANGPRES not in ('3','4','6','03','04','06') OR (RANGPRES IS NULL AND SUBSTRING(PEAJE,1,1)='3')) then (CONSUMO_KWH)/datediff(day,FECANT,FECACT)
                WHEN datediff(day,FECANT,FECACT) NOT IN (-1,0,1) AND (RANGPRES  in ('3','4','6','03','04','06') OR (RANGPRES IS NULL AND SUBSTRING(PEAJE,1,1) !='3'))
				     and LABORABLES>0 and FINDE>0 then  (CONSUMO_KWH)*0.85/LABORABLES
                WHEN datediff(day,FECANT,FECACT) NOT IN (-1,0,1) AND (RANGPRES  in ('3','4','6','03','04','06') OR (RANGPRES IS NULL AND SUBSTRING(PEAJE,1,1) !='3'))
				     and (LABORABLES=0 or FINDE=0) then (CONSUMO_KWH)/datediff(day,FECANT,FECACT)
                ELSE 0
                END,
              CASE 
                WHEN datediff(day,FECANT,FECACT) IN (-1,0,1) THEN  CONSUMO_KWH
                WHEN datediff(day,FECANT,FECACT) NOT IN (-1,0,1) AND (RANGPRES not in ('3','4','6','03','04','06') OR (RANGPRES IS NULL AND SUBSTRING(PEAJE,1,1)='3')) then (CONSUMO_KWH)/datediff(day,FECANT,FECACT)
                WHEN datediff(day,FECANT,FECACT) NOT IN (-1,0,1) AND (RANGPRES  in ('3','4','6','03','04','06') OR (RANGPRES IS NULL AND SUBSTRING(PEAJE,1,1) !='3'))
				     and FINDE>0 and LABORABLES>0 then  (CONSUMO_KWH)*0.15/FINDE
                WHEN datediff(day,FECANT,FECACT) NOT IN (-1,0,1) AND (RANGPRES  in ('3','4','6','03','04','06') OR (RANGPRES IS NULL AND SUBSTRING(PEAJE,1,1) !='3'))
				     and (LABORABLES=0 or FINDE=0) then (CONSUMO_KWH)/datediff(day,FECANT,FECACT)
                ELSE  0
              END 
         FROM (SELECT CUPS, FECANT, FECACT, RANGPRES, METODO, TELEMEDIDA, PEAJE, CONSUMO_KWH, NUMFACT, SEUDOFACT, FLOOR((DATEDIFF(DAY, fecant, fecact))/7)+ CASE --cálculo del número de domingos
                                                            WHEN 1 != DATEPART(WEEKDAY, fecant) and (DATEDIFF(DAY, fecant, fecact))%7 >= 7+(1-DATEPART(WEEKDAY, fecant)) THEN 1
                                                             ELSE 0 END + FLOOR((DATEDIFF(DAY, fecant, fecact))/7)+ CASE -- calculo del número de sábados
                                                             WHEN 7 != DATEPART(WEEKDAY, fecant) and (DATEDIFF(DAY, fecant, fecact))%7 >= (7-DATEPART(WEEKDAY, fecant)) THEN 1
                                                             ELSE 0 END FINDE, (datediff(day,fecant,fecact)- (
                                                             FLOOR((DATEDIFF(DAY, fecant, fecact))/7)+ CASE --cálculo del número de domingos
                                                             WHEN 1 != DATEPART(WEEKDAY, fecant) and (DATEDIFF(DAY, fecant, fecact))%7 >= 7+(1-DATEPART(WEEKDAY, fecant)) THEN 1
                                                             ELSE 0 END + FLOOR((DATEDIFF(DAY, fecant, fecact))/7)+ CASE -- calculo del número de sábados
                                                             WHEN 7 != DATEPART(WEEKDAY, fecant) and (DATEDIFF(DAY, fecant, fecact))%7 >= (7-DATEPART(WEEKDAY, fecant)) THEN 1
                                                             ELSE 0 END)) LABORABLES
                 From METDB.VW_CONSUMOS_LECT B
			    Where CUPS = @V_Cup
				  And NUMFACT = @V_Numfact
				  And SEUDOFACT = @V_Seudofact) Base;
        Open DetLect
		Fetch Next From DetLect Into @D_Fecini, @D_Fecfin, @V_Rangpres, @V_Metodo, @V_Telemedida, @V_Peaje, @V_Tipo, @N_ConsLab, @N_ConsFind
	BEGIN TRANSACTION;  
		WHILE @@FETCH_STATUS = 0  
		  Begin
               Insert  into METDB.MET_CONSUMDIARIO (CSD_CUP_CODIGO,CSD_NUMFACT,CSD_SEUDOFACT,CSD_DIA, CSD_RANGPRES,
	                                                CSD_METFACT,CSD_TELEMEDIDA,CSD_TIPPEAJE,CSD_TIPO,CSD_CONSUMO,
													CSD_CNT_CODIGO)
                                             select @V_Cup, @V_Numfact, @V_Seudofact, DIA, @V_Rangpres, @V_Metodo, @V_Telemedida, @V_Peaje, @V_Tipo, 
					                               case numdia
							                         when '7' Then @N_ConsFind
							                         when '1' Then @N_ConsFind
							                       else @N_ConsLab
							                  end, METDB.FT_CONTRATO_SIGE(NULL, @V_Cup, DIA, DIA)
					                           from metdb.VW_DIASANYO
					                          where dia between case  When datediff(day,@D_Fecini, @D_Fecfin) <= 1 Then @D_Fecfin else dateadd(day,1,@D_Fecini)  end And  @D_Fecfin 
               Fetch Next From DetLect Into @D_Fecini, @D_Fecfin, @V_Rangpres, @V_Metodo, @V_Telemedida, @V_Peaje, @V_Tipo, @N_ConsLab, @N_ConsFind
          End  
       CLOSE DetLect;
       DEALLOCATE DetLect;	
	   Update METDB.MET_FACTCABDIST
	      Set FCB_ESTADO = 'P'
		Where FCB_CUP = @V_Cup
		  And FCB_NUMFACT = @V_Numfact
		  And FCB_SEUDOFACT = @V_Seudofact;
     COMMIT TRANSACTION; 
	   Fetch Next From Facturas Into @V_Numfact, @V_Cup, @V_Seudofact;   
     End 	 
   CLOSE Facturas 
   DEALLOCATE Facturas ;	
 END;


