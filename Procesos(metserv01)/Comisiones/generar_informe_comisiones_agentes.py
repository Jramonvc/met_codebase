# Descripción: Script para generar informes de comisiones de agentes y enviarlos por correo electrónico.
# Autor: Eduardo Calatayud
# Fecha: 2025-01-14
# Versión: 1.0
# Python 3.8.8
# Dependencias: pandas, tqdm, xlwings, pyodbc, sqlalchemy, smtplib, email.mime, os, datetime, warnings
# Base de datos: SQL Server
# Servidor de correo: Office 365

# Importar librerías
import os
import pandas as pd
from tqdm import tqdm
import xlwings as xw
from datetime import date
from sqlalchemy import create_engine
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import warnings
warnings.filterwarnings('ignore')

# Funciones
def crear_informe(agente, fechaCalculoComision):

  # Escribe la consulta SQL de comisiones de Power
  consulta_sql_comisiones_power = """SELECT [FechaCalculoComision]
    ,[Agente]
    ,[Contrato]
    ,[Cliente]
    ,[Identidad]
    ,[CUPS]
    ,[FechaAlta]
    ,[FechaVencimiento]
    ,[TipoComision]
    ,[NumFactura]
    ,[ComisionUnitaria]
    ,[Consumo]
    ,[Potencia]
    ,[MargenPotencia]
    ,[Comision]
    FROM [METDB].[VW_COMISIONES_AGENTES_POWER]
    WHERE [FechaCalculoComision] = '""" + fechaCalculoComision + """'
    AND Agente = '""" + agente + """'
  """

  # Ejecuta la consulta y carga los datos en un DataFrame
  comisiones_power = pd.read_sql(consulta_sql_comisiones_power, engine_azure)
  #print(comisiones_power.head())

  # Escribe la consulta SQL de comisiones de Gas
  consulta_sql_comisiones_gas = """SELECT [FechaCalculoComision]
    ,[Agente]
    ,[Contrato]
    ,[Cliente]
    ,[Identidad]
    ,[CUPS]
    ,[FechaAlta]
    ,[FechaBaja]
    ,[TipoComision]
    ,[NumFactura]
    ,REPLACE([ComisionUnitaria],',','.') [ComisionUnitaria]
    ,[Consumo]
    ,[Comision]
    FROM [SigeMET].[dbo].[V_ComisionesAgentesGas]
    WHERE [FechaCalculoComision] = '""" + fechaCalculoComision + """'
    AND Agente = '""" + agente + """'
    """

  # Ejecuta la consulta y carga los datos en un DataFrame
  comisiones_gas = pd.read_sql(consulta_sql_comisiones_gas, engine_sige)
  #print(comisiones_gas.head())
  
  if not comisiones_power.empty or not comisiones_gas.empty:
    # Ruta de la plantilla y del archivo de salida
    template_path = 'C:\Procesos\Comisiones\plantilla_simple.xlsx'
    output_path = 'C:\Procesos\Comisiones\Informes No enviados\comisiones_' + agente + '_' + fechaCalculoComision + '.xlsx'
    # Crear una instancia de Excel (control explícito de la aplicación)
    app = xw.App(visible=False)  # visible=False para que Excel no aparezca en pantalla
    # Cargar la plantilla
    wb_plantilla = app.books.open(template_path)
    wb_plantilla.save(output_path)
    wb_plantilla.close()
    
    wb = app.books.open(output_path)
    
    # Escribir los dataframes en hojas específicas
    wb.sheets['Gas'].range('A4').value = comisiones_gas
    wb.sheets['Power'].range('A4').value = comisiones_power

    # Guardar el libro de trabajo
    wb.save()
    wb.close()
    # Cierra la aplicación de Excel
    app.quit()  
    print(f"Informe de comisiones de {agente} para el {fechaCalculoComision} creado.")

    return output_path
  else:
    return None
  

def enviar_correo(para, cc, asunto, cuerpo, archivo_adjunto):
    
    # Configuración del servidor SMTP
    servidor_smtp = 'smtp.office365.com'  # Dirección del servidor SMTP
    puerto_smtp = 587  # Puerto del servidor (generalmente 587 para TLS)
    correo_remitente = 'facturacion@met.com'  # Tu correo en el servidor
    contraseña = 'Ba$YVuZ4'  # Contraseña de tu cuenta en el servidor
    
    # Datos del correo
    
    asunto = asunto
    cuerpo = cuerpo
    
    # Ruta del archivo a adjuntar
    ruta_archivo = archivo_adjunto
    nombre_archivo = archivo_adjunto.split('\\')[-1]
    bcc = ['ramon.vicioso@met.com','eduardo.calatayud@met.com'] 
    # Crear el mensaje
    mensaje = MIMEMultipart()
    mensaje['From'] = correo_remitente
    mensaje['To'] = ','.join(para)
    mensaje['Cc'] = ','.join(cc)
    mensaje['Subject'] = asunto
    
    # Adjuntar el cuerpo del mensaje
    mensaje.attach(MIMEText(cuerpo, 'plain'))
    destinatarios = para + cc + bcc
    # Adjuntar el archivo
    try:
        with open(ruta_archivo, 'rb') as archivo:
            adjunto = MIMEBase('application', 'octet-stream')
            adjunto.set_payload(archivo.read())
    
        # Codificar el archivo en Base64
        encoders.encode_base64(adjunto)
        adjunto.add_header('Content-Disposition', f'attachment', filename=nombre_archivo)
    
        # Agregar el archivo adjunto al mensaje
        mensaje.attach(adjunto)
    
        # Conectarse al servidor SMTP
        servidor = smtplib.SMTP(servidor_smtp, puerto_smtp)
        servidor.starttls()  # Habilitar encriptación TLS
        servidor.login(correo_remitente, contraseña)
    
        # Enviar el correo
        servidor.sendmail(correo_remitente, destinatarios, mensaje.as_string())
        print("Correo enviado exitosamente con el archivo adjunto!")
    
    except Exception as e:
        print(f"Ocurrió un error: {e}")
    
    finally:
        servidor.quit()

def eliminar_archivo(ruta):
    try:
        os.remove(ruta)
        print(f"Archivo {ruta} eliminado exitosamente.")
    except FileNotFoundError:
        print(f"El archivo {ruta} no existe.")
    except PermissionError:
        print(f"No tienes permisos para eliminar el archivo {ruta}.")
    except Exception as e:
        print(f"Ocurrió un error al intentar eliminar el archivo: {e}")


# Fecha de cálculo de la comisión
fechaCalculoComision = date.today()
fechaCalculoComision = fechaCalculoComision.strftime('%Y-%m-%d')


# Configura la cadena de conexión
server = '82.194.94.135'  # Nombre del servidor
database = 'SigeMET'  # Nombre de la base de datos
username = 'MET'  # Usuario de SQL Server
password = 'Welcome2024$'  # Contraseña de SQL Server

# Crea la cadena de conexión
engine_sige = create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# Configura la cadena de conexión
server = 'met-esp-prod.database.windows.net'  # Nombre del servidor
database = 'Risk_MGMT_Spain'  # Nombre de la base de datos
username = 'MET'  # Usuario de SQL Server
password = 'Welcome2024$'  # Contraseña de SQL Server

# Crea la cadena de conexión
engine_azure = create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")


# Escribe la consulta SQL
consulta_sql_agentes = """
							SELECT AGE_DENOM, 
							       case CHARINDEX(';', AGE_EMAIL,1) when 0 then AGE_EMAIL else 
							       SUBSTRING(AGE_EMAIL,1, CHARINDEX(';', AGE_EMAIL,1)-1 ) end email1,  
							       case CHARINDEX(';', AGE_EMAIL,CHARINDEX(';', AGE_EMAIL,1)+1) when 0 then 
								   SUBSTRING(AGE_EMAIL,CHARINDEX(';', AGE_EMAIL,1)+ 1, CASE WHEN CHARINDEX(';', AGE_EMAIL,CHARINDEX(';', AGE_EMAIL,1)+1) = 0 THEN 300 ELSE CHARINDEX(';', AGE_EMAIL,CHARINDEX(';', AGE_EMAIL,1)+1) END -  CHARINDEX(';', AGE_EMAIL,1))
								   else 
							       SUBSTRING(AGE_EMAIL,CHARINDEX(';', AGE_EMAIL,1)+ 1, CASE WHEN CHARINDEX(';', AGE_EMAIL,CHARINDEX(';', AGE_EMAIL,1)+1) = 0 THEN 300 ELSE CHARINDEX(';', AGE_EMAIL,CHARINDEX(';', AGE_EMAIL,1)+1) END -  CHARINDEX(';', AGE_EMAIL,1))  end email2,
								   case CHARINDEX(';', AGE_EMAIL,CHARINDEX(';', AGE_EMAIL,1)+1) when 0 then null else
								   substring(age_email, CHARINDEX(';', AGE_EMAIL,CHARINDEX(';', AGE_EMAIL,1)+1) + 1,200) end email3, COM_MAIL
                              FROM METDB.MET_AGENTES LEFT JOIN METDB.MET_COMERCIALES ON COM_CODIGO = AGE_COM_CODIGO
                             WHERE AGE_EMAIL IS NOT NULL
                               AND AGE_CODIGO NOT IN (16, 23, 24, 27, 28, 36, 42, 49, 51,
                                                      53, 55, 60, 62, 64, 65, 66, 67, 69,
                                                      76, 77, 84, 88, 89, 90, 92, 101, 102,
                                                      105, 107, 110, 116)"""

# Ejecuta la consulta y carga los datos en un DataFrame
agentes = pd.read_sql(consulta_sql_agentes, engine_azure)


# Iterar sobre los agentes
for agente, agente_email1, agente_email2, agente_email3, comercial in tqdm(zip(agentes['AGE_DENOM'], agentes['email1'], agentes['email2'], agentes['email3'],agentes['COM_MAIL'])):

  # Crear el informe
  url_informe = crear_informe(agente, fechaCalculoComision)

  # Enviar el correo
  if url_informe is not None:
    if agente_email3 is not None:
        para= [agente_email1, agente_email2, agente_email3]
    elif agente_email2 is not None:
        para= [agente_email1, agente_email2]
    else:
        para= [agente_email1]
    if comercial is None:
      cc= ['proveedores@met.com']
    else:
      cc= [comercial , 'proveedores@met.com']
    asunto='Comisiones ' + agente + '- MET Energía España'
    cuerpo=f"""Buenos días,

              Adjunto la lista de comisiones de {agente} correspondiente al cálculo del día {fechaCalculoComision}.

              Por favor, no respondan a este correo. Para cualquier consulta y para el envío de facturas deben dirigirse a:
              proveedores@met.com

              Atentamente,"""
    adjunto=url_informe
    enviar_correo(para, cc, asunto, cuerpo, adjunto)

    # Eliminar el archivo enviado
    eliminar_archivo(url_informe)
    
