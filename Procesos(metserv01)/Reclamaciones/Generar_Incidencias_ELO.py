import pyodbc
import csv
import datetime
# Conexión a la base de datos SQL Server
conn = pyodbc.connect('DRIVER={SQL Server};'
                      'SERVER=82.194.94.135;'
                      'DATABASE=SIGEMET;'
                      'UID=sa;'
                      'PWD=Aq-1d.BlkY;')

# Crear un cursor
cursor = conn.cursor()

# Ejecutar la consulta SQL
cursor.execute("""
  SELECT 
        '`'+CONVERT(varchar, c.idcallcenter) + '`|`' + 'METS-M022' + '`|`' + l.CodigoExternoNAV + '`|`' + v.SerieFactura + CONVERT(varchar, v.NumeroFactura) + '`|`' +
        CASE
            WHEN CONVERT(xml, REPLACE(CAST(MensajeXML AS nVARCHAR(max)), 'xmlns=', 'xmlns:espacionombres=')) .value('(//claimsubtype)[1]', 'varchar(30)') IN ('003', '005', '008', '010', '049', '036') THEN 'DIST'
            WHEN CONVERT(xml, REPLACE(CAST(MensajeXML AS nVARCHAR(max)), 'xmlns=', 'xmlns:espacionombres=')) .value('(//claimsubtype)[1]', 'varchar(30)') = '009' THEN 'DICO'
            WHEN CONVERT(xml, REPLACE(CAST(MensajeXML AS nVARCHAR(max)), 'xmlns=', 'xmlns:espacionombres=')) .value('(//claimsubtype)[1]', 'varchar(30)') IN ('011', '101', '102', '105', '109') THEN 'PAGO'
            WHEN CONVERT(xml, REPLACE(CAST(MensajeXML AS nVARCHAR(max)), 'xmlns=', 'xmlns:espacionombres=')) .value('(//claimsubtype)[1]', 'varchar(30)') IN ('110', '111') THEN 'INAN'
            WHEN CONVERT(xml, REPLACE(CAST(MensajeXML AS nVARCHAR(max)), 'xmlns=', 'xmlns:espacionombres=')) .value('(//claimsubtype)[1]', 'varchar(30)') IN ('012', '114') THEN 'ENVFACT'
            WHEN CONVERT(xml, REPLACE(CAST(MensajeXML AS nVARCHAR(max)), 'xmlns=', 'xmlns:espacionombres=')) .value('(//claimsubtype)[1]', 'varchar(30)') = '068' THEN 'DIFP'
            ELSE 'OTR'
        END + '`|`' +
        FORMAT(fecha, 'dd/MM/yyyy') + '`|`' + replace(replace(textoincidencia,CHAR(13),' '),CHAR(10),' ')+'`|`'+isnull(format(fechaCierre, 'dd/MM/yyyy'),'')+'`|`'+isnull(replace(replace(textosolucion,CHAR(13),' '),CHAR(10),' '),'')+'`|`'+
		case when LEN(oficinaModulo) = 0 Then 'greenlight' else OficinaModulo end+'`'
    FROM 
        sigemet.dbo.CallCenter C, 
        SIGEMET.DBO.CallCenterSolicitud s, 
        SIGEMET.DBO.SolicitudExternaPaso e, 
        sigemet.dbo.CallCenterFactura F, 
        sigemet.dbo.FacturaVentaCabecera V, 
        sigemet.dbo.Cliente L,
		sigemet.dbo.Usuario U
    WHERE 
        c.IdCallCenter = s.IdCallCenter
        AND (FechaCierre IS NULL OR fechacierre >= DATEADD(day, -7, GETDATE()))
        AND c.IdCallCenter = f.IdCallCenter
        AND f.IdFacturaVentaCabecera = v.IdFacturaVentaCabecera
        AND v.IdCliente = l.IdCliente
        AND S.IdSolicitud = E.IdSolicitud
        AND IdSolicitudPaso = 92060
		AND C.IdUsuario = U.IdUsuario
""")

# Recuperar los resultados de la consulta
results = cursor.fetchall()

# Especifica la ruta y el nombre del archivo de texto
archivo_txt = 'resultados.txt'

# Escribir los resultados en un archivo de texto
with open(archivo_txt, 'w', encoding='utf-8') as txtfile:
    for row in results:
        txtfile.write('|'.join([str(item) for item in row]) + '\n')

# Cerrar cursor y conexión
cursor.close()
conn.close()
import paramiko

def subir_archivo_sftp(archivo_local, servidor_sftp, puerto_sftp, usuario_sftp, contraseña_sftp, ruta_remota):
    try:
        # Establecer la conexión SFTP
        transport = paramiko.Transport((servidor_sftp, puerto_sftp))
        transport.connect(username=usuario_sftp, password=contraseña_sftp)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Subir el archivo al servidor SFTP
        #sftp.put(localpath=archivo_local, remotepath=ruta_remota)
        sftp.put(archivo_local, ruta_remota)
        print("Archivo subido exitosamente al servidor SFTP")

    except Exception as e:
        print("Error al subir archivo al servidor SFTP:", e)

    finally:
        # Cerrar la conexión SFTP
        if transport.is_active():
            transport.close()
fecha_hora_actual = datetime.datetime.now()  
subir_archivo_sftp(r'C:\Procesos\Reclamaciones\resultados.txt', 'ftp.covline.es',22, 'met', 'enqd7YaRU0kR_2HnRPKgZUpMkZsHGV', '/interface/Incidencias_'+fecha_hora_actual.strftime('%Y%m%d%H%M%S')+'.txt')