import pandas as pd
from sqlalchemy import create_engine
# Configurar las conexiones a las bases de datos
source_db_url = 'mssql+pyodbc://sa:Aq-1d.BlkY@82.194.94.135/SIGEMET?driver=ODBC+Driver+17+for+SQL+Server'
destination_db_url = 'mssql+pyodbc://sqluser:cwD9KVms4Qdv4mLy@met-esp-prod.database.windows.net/Risk_MGMT_Spain?driver=ODBC+Driver+17+for+SQL+Server'

#conn_str_src = ('DRIVER={SQL Server};SERVER=82.194.94.135;DATABASE=SIGEMET;UID=sa;PWD=Aq-1d.BlkY;')
# Reemplaza 'server2', 'database2', 'username2', 'password2' con los detalles de la segunda base de datos
#conn_str_dest = ('DRIVER={SQL Server};SERVER=met-esp-prod.database.windows.net;DATABASE=Risk_MGMT_Spain;UID=sqluser;PWD=cwD9KVms4Qdv4mLy')

# Conectar a ambas bases de datos
#conn_src = pyodbc.connect(conn_str_src)
#conn_dest = pyodbc.connect(conn_str_dest)

# Crear cursores para ambas conexiones
#cursor_src = conn_src.cursor()
#cursor_dest = conn_dest.cursor()

try:
    # Ejecutar una consulta para seleccionar los datos que deseas transferir
       #cursor_src.execute(
       sql_query = """select UPPER(identidad) NIF, UPPER(nombre) NOMBRE, UPPER(apellido1) APELLIDO1, UPPER(apellido2) APELLIDO2, UPPER(RazonSocial) RAZSOC,
                                 CodigoExternoNAV CODEXTNAV, upper(textociudad) ciudad, numero, aclarador, c.codpostal,
                                 upper(NombreCalle) calle,  UPPER(TextoVia) VIA, s.TextoCategoria CATEGORIA, IBAN, DiaPago, textotipocobro FORMPAGO,
		                        (select valor 
		                           from sigemet.dbo.ClienteContacto T 
		                          where T.IDCLIENTE = C.IDCLIENTE
		                            and pordefecto = 1
			                        and tipocontacto = 'E') EMAIL,
                                (select valor 
		                           from sigemet.dbo.ClienteContacto T 
		                          where T.IDCLIENTE = C.IDCLIENTE
		                            and pordefecto = 1
			                        and tipocontacto = 'M') MOVIL,
                                (select valor 
		                           from sigemet.dbo.ClienteContacto T 
		                          where T.IDCLIENTE = C.IDCLIENTE
		                            and pordefecto = 1
			                        and tipocontacto = 'T') TELEFONO
                            from SigeMET.dbo.cliente c,sigemet.dbo.ciudad i, sigemet.dbo.callejero l, sigemet.dbo.CallejeroTipoVia t,
                                 sigemet.dbo.ClienteCategoria S, sigemet.dbo.ClientePago P, sigemet.dbo.tipocobro f
                            where c.idciudad = i.idciudad
                              and c.idcallejero = l.idcallejero
                              and l.IdCallejeroTipoVia = t.IdCallejeroTipoVia
                              and isnull(c.IdClienteCategoria,'2') = s.IdClienteCategoria 
                              and c.IdCliente = p.IdCliente
                              and PorDefecto = 1
                              and p.IdTipoCobro = f.IdTipoCobro;"""

       engine_source = create_engine(source_db_url, fast_executemany=True)
       engine_destination = create_engine(destination_db_url, fast_executemany=True)
       df = pd.read_sql(sql_query, engine_source)
    # Escribir los datos en la tabla de la base de datos de destino
       df.to_sql('TMP_CLIENTES', engine_destination, schema='METDB',if_exists='append', index=False)
       
       sql_query = """SELECT CODIGOCUPS CUPS, upper(TextoCiudad) ciudad, numero, Aclarador, c.CodPostal, upper(NombreCalle) calle, upper(TextoVia)via, isnull(capacidad,0)capacidad, D.CodMinetur distribuidora
	   FROM SIGEMET.DBO.CUPS C
	        left join (select idcups, sum(capacidad) capacidad
                         from sigemet.dbo.CUPSCapacidad
	                     where convert(date,getdate()) between CUPSCapacidad.FechaDesde and isnull(CUPSCapacidad.FechaHasta,convert(date,getdate())) group by idcups) q on c.idcups = q.idcups,
            sigemet.dbo.ciudad i, sigemet.dbo.callejero l, sigemet.dbo.CallejeroTipoVia t, SIGEMET.dbo.Distribuidora D
	   where c.IdCiudad = i.IdCiudad
	     and c.idcallejero = l.IdCallejero
		 and l.IdCallejeroTipoVia = t.IdCallejeroTipoVia
         And C.IdDistribuidora = D.IdDistribuidora;"""
       df = pd.read_sql(sql_query, engine_source)
    # Escribir los datos en la tabla de la base de datos de destino
       df.to_sql('TMP_CUPS', engine_destination, schema='METDB',if_exists='append', index=False)
       
       sql_query = """
          Select c.entorno+convert(varchar,C.CodigoContrato) Contrato,Identidad, CodigoCUPS, convert(date,c.fechaalta) fecalta, convert(date,fechabaja) fecbaja, 
                 convert(date,fechavto) fechvto, TextoSituacion ESTADO, IBAN,DiaPago, UPPER(textotipocobro) FORMAPAGO,
	             CodigoCNAE, CodigoAgrupacion, 
	             case when isnull(A.idagentenivelanterior, '1') in ('1','2','5') Then NombreAgente else (select Nombreagente from SIGEMET.DBO.AGENTE AG2 WHERE AG2.IDAGENTE = a.IdAgenteNivelAnterior and AG2.Entorno = A.Entorno) end AGENTE, 
	             R.TextoTarifa TARIFA,g.TextoTarifaGrupo TARGRUP, o.Caudal caudal,
	             TextoPresion PRESION, 
	             CASE IsTelemedido When '1' Then 'S' else 'N' end Telemedido, convert(date,fechacontrato) feccontr, ConsumoEstimado CONSESTIMADO, ISNULL(FEE*1000,EnergiaFEEIndexado) FEECANAL, round(PotenciaFee * 365,2) POTFEE
            From SIGEMET.DBO.Contrato C 
                 left join sigemet.dbo.agente a on  a.IDagente = C.IdAgente and a.entorno = CASE WHEN c.entorno = 'E1' THEN 'G1' ELSE 'G2' END 
	             left join sigemet.dbo.presion Presion on c.IdPresion = presion.IdPresion
	             left join sigemet.dbo.ContratoPrecio CP on c.IdContrato = cp.IdContrato and cp.Entorno = CASE WHEN c.entorno = 'E1' THEN 'G1' ELSE 'G2' END ,
                 SIGEMET.DBO.CLIENTE L, sigemet.dbo.cups p, 
                 sigemet.dbo.ContratoTarifa T, sigemet.dbo.CNAE N, sigemet.dbo.ContratoSituacion S, 
	             SIGEMET.DBO.ClientePago	F, SIGEMET.DBO.TipoCobro I, SIGEMET.DBO.TARIFA R, SIGEMET.DBO.TarifaGrupo G, sigemet.dbo.ContratoPotencia o
           Where c.IdCliente = l.IdCliente
             And c.idcups = p.idcups
             And C.CODIGOCONTRATO = T.CodigoContrato
             And C.Entorno = case T.Entorno when 'G1' THEN 'E1' ELSE 'E2' END
             And ((select max(fechadesde) from SIGEMET.DBO.ContratoTarifa T2 WHERE T2.CodigoContrato = C.CodigoContrato) between t.fechadesde and isnull(t.fechahasta,convert(date,getdate()))
                 or t.fechadesde > convert(date,getdate()))
             And C.IdCNAE = N.IdCNAE
             And C.IdContratoSituacion = S.IdContratoSituacion
             And C.IdClientePago = F.IdClientePago
             And C.IdCliente = F.IdCliente
             And F.IdTipoCobro = I.IdTipoCobro
             And T.IdTarifa = R.IdTarifa
             And t.IdTarifa = g.IdTarifa
             And t.IdTarifaGrupo = g.IdTarifaGrupo
             And c.IdContrato = o.IdContrato
           GROUP BY c.entorno,C.CodigoContrato,Identidad, CodigoCUPS, C.fechaalta, fechabaja, 
                    fechavto, TextoSituacion, IBAN,DiaPago, UPPER(textotipocobro),
                    CodigoCNAE, CodigoAgrupacion, NombreAgente, R.TextoTarifa,g.TextoTarifaGrupo, o.Caudal,
                    TextoPresion, ISTELEMEDIDO, fechacontrato, ConsumoEstimado, A.IdAgenteNivelAnterior, A.Entorno, ISNULL(FEE*1000,EnergiaFEEIndexado), PotenciaFee"""
       df = pd.read_sql(sql_query, engine_source)
    # Escribir los datos en la tabla de la base de datos de destino
       df.to_sql('TMP_CONTRATOS', engine_destination, schema='METDB',if_exists='append', index=False) 
       sql_query = """Select case c.Entorno when 'G2' THEN 'E2' ELSE 'E1'END + CONVERT(VARCHAR, CODIGOCONTRATO) CONTRATO,  convert(date,FechaDesde) FECINI, 
                             convert(date,FechaHasta) FECFIN, tp.TextoTarifaPeaje TARIFA, G.TextoTarifaGrupo GRUPTAR, IdContratoTarifa
                        from SigeMET.dbo.ContratoTarifa C, SigeMET.dbo.Tarifa T, SigeMET.dbo.TarifaGrupo G, sigemet.dbo.TarifaPeaje TP
                       Where C.IdTarifa = T.IdTarifa
                         And C.IdTarifaGrupo = G.IdTarifaGrupo
						 and t.IdTarifaPeaje = tp.IdTarifaPeaje"""
       df = pd.read_sql(sql_query, engine_source)
    # Escribir los datos en la tabla de la base de datos de destino
       df.to_sql('TMP_CONTTARIF', engine_destination, schema='METDB',if_exists='append', index=False) 

       sql_query = """Select T.TextoTarifa Tarifa, G.TextoTarifaGrupo GRUPO, convert(date,fechainicio) FECINI, convert(date,fechafinal) FECFIN, PotenciaPrecio TERFIJO, EnergiaPrecio TERENERG
                        From SigeMET.dbo.TarifaPrecio P, SigeMET.dbo.Tarifa T, SigeMET.dbo.TarifaGrupo G
                       Where P.IdTarifa = T.IdTarifa
                         And P.IdTarifa = G.IdTarifa
	                     And P.IdTarifaGrupo = G.IdTarifaGrupo"""
       df = pd.read_sql(sql_query, engine_source)
    # Escribir los datos en la tabla de la base de datos de destino
       df.to_sql('TMP_PREGTAR', engine_destination, schema='METDB',if_exists='append', index=False)

       sql_query = """Select entorno+convert(varchar,CodigoContrato) As CodigoContrato,
                        T2.Loc.value('../../FechaDesde[1]', 'date') AS FechaDesde,
                        T2.Loc.value('../../FechaHasta[1]', 'date') AS FechaHasta,
                        case T2.Loc.value('../../Formula[1]', 'nvarchar(2)')
                        when 8 then 'BRENT 603 CAMBIO MES'
                        when 11 then 'MEZCLA FORMULAS'
                        when 12 then 'PRECIOS ESCALONADO'
                        when 15 then 'TTF Herem'
                        when 2 then 'MIBGAS'
                        when 3 then 'TTF DAM'
                        when 4 then 'TTF MADAM'
                        when 5 then 'TTF DAM 303'
                        when 14 then 'TTF SPAM'
                        when 6 then 'BRENT 603 TIPO CAMBIO 303'
                        when 7 then 'BRENT 303 TIPO CAMBIO 303'
                        when 9 then 'BRENT 303 TIPO CAMBIO MES'
                        when 10 then 'OMIE'
                        else T2.Loc.value('../../Formula[1]', 'nvarchar(2)') end AS Formula,
                        T2.Loc.value('EsFijo[1]', 'bit') AS IsPrecioFijo,
                        T2.Loc.value('Minimo[1]', 'float') AS Minimo,
                        T2.Loc.value('Maximo[1]', 'float') AS Maximo,
                        T2.Loc.value('Porcentaje[1]', 'float') AS Porcentaje,
                        T2.Loc.value('PrecioFijo[1]', 'float') AS PrecioFijo,
                        case T2.Loc.value('Formula[1]', 'nvarchar(2)')
                        when 8 then 'BRENT 603 CAMBIO MES'
                        when 11 then 'MEZCLA FORMULAS'
                        when 12 then 'PRECIOS ESCALONADO'
                        when 15 then 'TTF Herem'
                        when 2 then 'MIBGAS'
                        when 3 then 'TTF DAM'
                        when 4 then 'TTF MADAM'
                        when 5 then 'TTF DAM 303'
                        when 14 then 'TTF SPAM'
                        when 6 then 'BRENT 603 TIPO CAMBIO 303'
                        when 7 then 'BRENT 303 TIPO CAMBIO 303'
                        when 9 then 'BRENT 303 TIPO CAMBIO MES'
                        when 10 then 'OMIE'
                        when 0 then ''
                        else T2.Loc.value('Formula[1]', 'nvarchar(2)') end AS SubFormula,
                        T2.Loc.value('Spread[1]', 'float') as Spread,
                        T2.Loc.value('VariableA[1]', 'float') AS VariableA,
                        T2.Loc.value('VariableB[1]', 'float') AS VariableB,
                        T2.Loc.value('VariableC[1]', 'float') AS VariableC,
                        T2.Loc.value('TienePenalizacion[1]', 'bit') as Penalizacion,
                        T2.Loc.value('TipoMedia[1]', 'float') as TipMedia
                   From sigemet.dbo.Contrato CROSS APPLY ContratoInfoXML.nodes('//FacturacionVariableGasRangoConfigDTO') as T2(Loc);"""
       df = pd.read_sql(sql_query, engine_source)
    # Escribir los datos en la tabla de la base de datos de destino
       df.to_sql('TMP_TERMVARC', engine_destination, schema='METDB',if_exists='append', index=False)  

       sql_query = """Select entorno+convert(varchar,CodigoContrato) As CodigoContrato,
                             T2.Loc.value('FechaDesde[1]', 'date') AS FechaDesde,
                             T2.Loc.value('FechaHasta[1]', 'date') AS FechaHasta,
                             case T2.Loc.value('Formula[1]', 'nvarchar(2)')
                             when 8 then 'BRENT 603 CAMBIO MES'
                             when 11 then 'MEZCLA FORMULAS'
                             when 12 then 'PRECIOS ESCALONADO'
                             when 15 then 'TTF Herem'
                             when 2 then 'MIBGAS'
                             when 3 then 'TTF DAM'
                             when 4 then 'TTF MADAM'
                             when 5 then 'TTF DAM 303'
                             when 14 then 'TTF SPAM'
                             when 6 then 'BRENT 603 TIPO CAMBIO 303'
                             when 7 then 'BRENT 303 TIPO CAMBIO 303'
                             when 9 then 'BRENT 303 TIPO CAMBIO MES'
                             when 10 then 'OMIE'
                             else T2.Loc.value('Formula[1]', 'nvarchar(2)') end AS Formula,
                             T2.Loc.value('EsFijo[1]', 'bit') AS IsPrecioFijo,
                             T2.Loc.value('Minimo[1]', 'float') AS Minimo,
                             T2.Loc.value('Maximo[1]', 'float') AS Maximo,
                             T2.Loc.value('Porcentaje[1]', 'float') AS Porcentaje,
                             T2.Loc.value('PrecioFijo[1]', 'float') AS PrecioFijo,
                             case T2.Loc.value('Formula[1]', 'nvarchar(2)')
                             when 8 then 'BRENT 603 CAMBIO MES'
                             when 11 then 'MEZCLA FORMULAS'
                             when 12 then 'PRECIOS ESCALONADO'
                             when 15 then 'TTF Herem'
                             when 2 then 'MIBGAS'
                             when 3 then 'TTF DAM'
                             when 4 then 'TTF MADAM'
                             when 5 then 'TTF DAM 303'
                             when 14 then 'TTF SPAM'
                             when 6 then 'BRENT 603 TIPO CAMBIO 303'
                             when 7 then 'BRENT 303 TIPO CAMBIO 303'
                             when 9 then 'BRENT 303 TIPO CAMBIO MES'
                             when 10 then 'OMIE'
                             when 0 then ''
                             else T2.Loc.value('Formula[1]', 'nvarchar(2)') end AS SubFormula,
                             T2.Loc.value('Spread[1]', 'float') as Spread,
                             T2.Loc.value('VariableA[1]', 'float') AS VariableA,
                             T2.Loc.value('VariableB[1]', 'float') AS VariableB,
                             T2.Loc.value('VariableC[1]', 'float') AS VariableC,
                             T2.Loc.value('TienePenalizacion[1]', 'bit') as Penalizacion,
                             T2.Loc.value('TipoMedia[1]', 'float') as TipMedia
                       From sigemet.dbo.Contrato CROSS APPLY ContratoInfoXML.nodes('//FacturacionVariableGasRangoDTO') as T2(Loc)"""
       df = pd.read_sql(sql_query, engine_source)
    # Escribir los datos en la tabla de la base de datos de destino
       df.to_sql('TMP_TERMVARS', engine_destination, schema='METDB',if_exists='append', index=False)  

       sql_query = """ Select C.Entorno + CONVERT(varchar, codigocontrato) contrato, PotenciaContratada Potencia, TextoTarifaPeriodo Periodo
                         from sigemet.dbo.ContratoPotencia P, sigemet.dbo.TarifaPeriodo T, sigemet.dbo.Contrato C
                        where p.Entorno = 'E1'
                          and P.IdTarifaPeriodo = T.IdTarifaPeriodo
	                      and p.IdContrato = C.IdContrato"""
       df = pd.read_sql(sql_query, engine_source)
       # Escribir los datos en la tabla de la base de datos de destino
       df.to_sql('TMP_CONTPOTENCIA', engine_destination, schema='METDB',if_exists='append', index=False)  

       sql_query = """Select fc.IdFacturaVentaCabecera id, tp.TextoTarifaPeaje TARIFA, FechaLecturaAnteriorXML FECDESDE, fc.FechaLecturaActualXML FECHASTA, P.CodigoCUPS CUPS, cl.Identidad NIF,
	                         FT.ImporteTotal TOTFACT, FI.TextoFacturaTipo TIPFACT, tp.TextoTarifaPeaje TARACCES, convert(xml,replace(CAST(InfoCabeceraXML AS nVARCHAR(max)),'xmlns=','xmlns:espacionombres=')).value('(//ConsumoActiva)[1]','numeric(20,4)') TOTCONS,
		                     NULL IMPIEE, PorcentajeImpuesto IVA, NULL TOTENERGIA, C.ENTORNO+ CONVERT(VARCHAR,C.CodigoContrato) CONTRATO, NULL TOTPOT, NULL TOTAQU, FT.ImporteBase TOTBASIMP, NULL TOTIMPE, ImporteImpuesto TOTIVA,
		                     FC.SerieFactura+CONVERT(VARCHAR, FC.NumeroFactura) NUMFACT, FC.FechaFactura FECFACT, NULL ENVMAIL, NULL FECMAIL, NULL RESULTMAIL, FC.FechaVencimiento FECCARGO, NULL RECTIF, NULL FECRECT, NULL NUMRECT, NULL TASMUNI
	                   From sigemet.dbo.FacturaVentaCabecera FC, SIGEMET.DBO.FacturaVentaTotal FT, sigemet.dbo.facturatipo FI, sigemet.dbo.TarifaPeaje TP, sigemet.dbo.contrato c, sigemet.dbo.cliente Cl, sigemet.dbo.CUPS P
	                  Where FC.Entorno = 'E1'
                        And FC.Fechafactura >= convert(date, DATEADD (day, -30, getdate()))
	                    And FC.IdFacturaVentaCabecera = FT.IdFacturaVentaCabecera
	                    And FC.IdFacturaTipo = FI.IdFacturaTipo
	                    And fc.IdTarifaPeajeXML = tp.IdTarifaPeaje
	                    And fc.IdContrato = c.IdContrato
	                    And c.IdCups = p.IdCups
	                    And c.IdCliente = cl.IdCliente"""
       df = pd.read_sql(sql_query, engine_source)
        # Escribir los datos en la tabla de la base de datos de destino
       df.to_sql('TMP_FACTPOWER', engine_destination, schema='METDB',if_exists='append', index=False)   
       print("Transferencia de datos exitosa.")

except Exception as e:
    print("Error durante la transferencia de datos:", e)




