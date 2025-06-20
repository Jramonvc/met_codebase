import psycopg2
import pyodbc

# Conexión a PostgreSQL
pg_conn = psycopg2.connect(
    dbname='metenergia_meten',
    user='readonly_user',
    password='PFsUaPVcfc7raR95mJy8',
    host='178.32.23.157',
    port='5432'
)
pg_cursor = pg_conn.cursor()
# Conexión a SQL Server
#ql_server_conn_str = 'mssql+pyodbc://sqluser:cwD9KVms4Qdv4mLy@met-esp-prod.database.windows.net/Risk_MGMT_Spain?driver=ODBC+Driver+17+for+SQL+Server'
sql_server_conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=met-esp-prod.database.windows.net;'
        'DATABASE=Risk_MGMT_Spain;'
        'UID=sqluser;'
        'PWD=cwD9KVms4Qdv4mLy;'
)
sql_server_conn = pyodbc.connect(sql_server_conn_str)
sql_server_cursor = sql_server_conn.cursor()

# Seleccionar datos de Clientes/Contratos
pg_cursor.execute("""   select ps.cups, c.codigo CNAE, ta.descripcion TARATR, tp.nombre TARIFA,  potencia_contratada ->>0 as POTP1, potencia_contratada ->>1 as POTP2, potencia_contratada ->>2 asPOTP3, potencia_contratada ->>3 as POTP4, 
       potencia_contratada ->>4 as POTP5, potencia_contratada ->>5 as POTP6, ecs.estado , cs.codigo, 
                            tv.codigo VIAPDS, d.calle ||' '||COALESCE(D.numero_finca,'') DIREPDS, d.ACLARADOR_FINCA ACLRPDS, M.nombre POBLACPDS, substring(D.cod_postal,1,5) CPPDS, 
                            case when (CS.fecha_inicio is null and ta.codigo = '018') and (current_date - date(cs.created_at)) > 7 then current_date else cs.fecha_inicio end  FECINI , 
                            CS.fecha_fin FECFIN,  valor_dias_meses_plazo_pago PLAPAGO, dia_pago DIAPAG,
                            c2.nombre NOMBCLI, C2.primer_apellido APELLIDO1, c2.segundo_apellido APELLIDO2, c2.razon_social RAZSOCIAL, c2.correo_electronico EMAIL, c2.identificador NIF, c2.telefono_1 TELCLI,
                            cs.nombre CONTACTO, TV2.codigo VIACLI, d2.calle ||' '||COALESCE(D2.numero_finca,'') DIRCLI, d2.aclarador_finca  ACLRCLI, M2.nombre  POBLACLI, substring(D2.cod_postal,1,5) CPCLI,
                            cs.fecha_firma FECFIR, (select case  when strpos(COALESCE(ccs.nombre, ccs.nombre_completo), 'CLOCAT') != 0 then 'CLOCAT ENERGY, S.L' 
                                                                 when strpos(COALESCE(ccs.nombre, ccs.nombre_completo), 'PIÑEIRO') != 0 then 'PIÑEIRO SUMINISTROS SLU' 
                                                                 when strpos(COALESCE(ccs.nombre, ccs.nombre_completo), 'EVOLTA') != 0 then 'EVOLTA CONSULTORES SL' 
                                                            else
                                                                 COALESCE(ccs.nombre, ccs.nombre_completo) end  
                                                      from contactos_contrato_suministro_pivote ccsp, contactos_contrato_suministro ccs  
                                                     where id_contrato_suministro  = cs.id
                                                       and id_tipo_contacto  = '7'
                                                       and id_contacto = ccs.id 
                                                      limit 1) agente,
                           mp.nombre FORMPAG, cs.iban, CONSUMO_ANUAL_CALCULADO/1000 CONSANUAL, cs.id_subestado subestado, cs.consumo_anual_estimado/1000, cd.codigo_cnmc, fee_energia #>> '{periodos_concepto, p1, valor}' AS Fee_p1,
                           CS.created_at ::date
                       from contratos_suministro cs, puntos_suministro ps, "66_cnae" c, contratos_suministro_atributos csa, "17_tarifa_atr" ta, 
                            estados_contrato_suministro ecs, direcciones d, tarifa_precios_contrato_suministro tpcs, tarifa_precios tp,
                            "12_tipo_via" tv, municipio m, clientes c2, direcciones d2, "12_tipo_via" tv2, municipio m2, modo_pago mp, codigo_distribuidoras cd 
                      where cs.ps_id = ps.id
                        and ps.id_cnae  = c.id 
                        and cs.id = csa.id_contrato_suministro 
                        and csa.id_tarifa_atr = ta.id 
                        and csa.fecha_fin  is null
                        and cs.id_estado  = ecs.id 
                        and ps.id_direcciones  = d.id 
                        and d.id_tipo_via  = tv.id 
                        and D.id_municipio  = M.id 
                        and cs.id_cliente = c2.id 
                        and c2.id_direcciones = d2.id
                        and D2.id_tipo_via  = TV2.id 
                        and D2.id_municipio = M2.id 
                        and cs.modo_pago = cast(mp.id as char)
                        and ps.id_codigo_distribuidora = cd.id  
                        and cs.id  = tpcs.id_contrato_suministro 
                        and tpcs.fecha_desde = (select max(fecha_desde) from tarifa_precios_contrato_suministro tpcs2 where tpcs2.id_contrato_suministro = cs.id)
                        and tpcs.id_tarifa_precios  = tp.id ;""")
rows = pg_cursor.fetchall()
# Insertar datos en SQL Server
sql_query = """
    INSERT INTO METDB.TMP_CONTRATNEURO (
        CUPS, CNAE, TARATR, TARIFA, POTP1, POTP2, POTP3, POTP4, POTP5, POTP6,
        ESTADO, CODIGO, VIAPDS, DIRPDS, ACLRPDS, POBLACPDS, CPPDS, FECINI, FECFIN,
        PLAPAGO, DIAPAG, NOMCLI, APELLIDO1, APELLIDO2, RAZSOCIAL, EMAIL,
        NIF, TELCLI, CONTACTO,  VIACLI, DIRCLI, ACLRCLI, POBLACLI, CPCLI,
        FECFIR, AGENTE, FORMPAG, IBAN, CONSANUAL, SUBESTADO, CONSESTIMADO, DISTRIBUIDORA, MARGEN, FECCRE
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?)
"""
for row in rows:
    sql_server_cursor.execute(sql_query, row)

# Confirmar cambios en SQL Server 
sql_server_conn.commit()

# Seleccionar datos de Facturacion 
pg_cursor.execute(""" select p.id, ct.descripcion , fecha_desde, fecha_hasta , cups , identificador, total_factura, id_tipo_factura, ta.descripcion , consumo_energia_kwh total_energiaconsumo , importe_total_iee, iva, 
                             total_energia, cs.codigo, total_potencia , total_alquileres, total_base_imponible, total_impuesto_electricidad , total_iva, codigo_prefactura, fecha_prefactura,
                             case enviada_email when true then 'S' else 'N'end ENVMAIL, fecha_envio_email, resultado_envio_email, fecha_cargo, case rectificativa when true then 'S' else 'N'end RECT, fecha_rectifica,  codigo_rectifica, 
                             case en_tasas_municipales when true then 'S' else 'N'end  TASMUNI
                        from prefacturas p left join "17_tarifa_atr" ta on p.id_tarifa_acceso  = ta.id
                             , puntos_suministro ps, clientes c, "107_codigo_tarifa" ct, contratos_suministro cs
                       where fecha_prefactura >= (CURRENT_TIMESTAMP - INTERVAL '40 days')::date
                         and p.id_punto_suministro  = ps.id 
                         and p.id_cliente  = c.id 
                         and p.id_tarifa  = ct.id 
                         and p.id_contrato_suministro  = cs.id """)
rows = pg_cursor.fetchall()
# Insertar datos en SQL Server
sql_query = """
    INSERT INTO METDB.TMP_FACTPOWER (
        ID, TARIFA, FECDESDE, FECHASTA, CUPS, NIF, TOTFACT, TIPFACT, TARACCES, TOTCONS,
        IMPIEE, IVA, TOTENERGIA, CONTRATO, TOTPOT, TOTAQU, TOTBASIMP, TOTIMPE, TOTIVA, 
        NUMFACT, FECFACT, ENVMAIL, FECMAIL, RESULTMAIL, FECCARGO, RECTIF, FECRECT, 
        NUMRECT, TASMUNI)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
for row in rows:
    sql_server_cursor.execute(sql_query, row)

# Confirmar cambios en SQL Server 
sql_server_conn.commit()

pg_cursor.execute ("""select TP.nombre, elem->>'nombre_concepto', 
                             periods.key AS periodo,
                             periods.value ->>'valor' AS valor 
                        from tarifa_precios tp, tarifa_precios_energia tpe, json_array_elements(conceptos_energia) elem,
                             json_each(elem->'periodos_concepto') periods
                       where conceptos_energia is not null
                        and tp.id = tpe.id_tarifa 
                         and  elem->>'nombre_concepto' in ('Bp','PorcPp','DESV')""")
rows = pg_cursor.fetchall()
# Insertar datos en SQL Server
sql_query = """
    INSERT INTO METDB.TMP_PRECENERG_C (
        TARIFA, PRODUCTO, PERIODO, VALOR)
   VALUES (?, ?, ?, ?)
"""
for row in rows:
    sql_server_cursor.execute(sql_query, row)

# Confirmar cambios en SQL Server 
sql_server_conn.commit()

pg_cursor.execute("""select tp.nombre, 
                            periodos_energia -> 'p1'->0 ->>'formula' as P1_Energia,
                            periodos_energia -> 'p2'->0 ->>'formula' as P2_Energia,
                            periodos_energia -> 'p3'->0 ->>'formula' as P3_Energia,
                            periodos_energia -> 'p4'->0 ->>'formula' as P4_Energia,
                            periodos_energia -> 'p5'->0 ->>'formula' as P5_Energia,
                            periodos_energia -> 'p6'->0 ->>'formula' as P6_Energia
                       from tarifa_precios_energia tpe, tarifa_precios tp 
                      where tpe.id_tarifa  = tp.id 
	                    and conceptos_energia is null
	                    and periodos_energia -> 'p1'->0 ->>'formula' not like '%AJOM%'""")
rows = pg_cursor.fetchall()
# Insertar datos en SQL Server
sql_query = """
    INSERT INTO METDB.TMP_PRECENERG (TARIFA, P1, P2, P3, P4, P5, P6)
   VALUES (?, ?, ?, ?,?,?,?)
"""
for row in rows:
    sql_server_cursor.execute(sql_query, row)

# Confirmar cambios en SQL Server y cerrar conexiones
sql_server_conn.commit()

sql_server_conn.close()
pg_conn.close()