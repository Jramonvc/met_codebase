import paramiko
import os
from datetime import datetime, timedelta
def descargar_archivo_sftp(servidor, puerto, usuario, contraseña, ruta_remota, ruta_local):
    try:
        # Crear una instancia de cliente SFTP
        transporte = paramiko.Transport((servidor, puerto))
        transporte.connect(username=usuario, password=contraseña)
        sftp = paramiko.SFTPClient.from_transport(transporte)

        # Descargar el archivo desde el servidor SFTP a la ruta local
        sftp.get(ruta_remota, ruta_local)
        print("Archivo descargado exitosamente.")

        # Borrar el archivo remoto
            #sftp.remove(ruta_remota)
        
    except Exception as e:
        print("Error:", e)

    finally:
        # Cerrar la conexión SFTP
        if transporte.is_active():
            transporte.close()
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
fecha_hoy = datetime.today()
Fichero = 'CoefTemp_'+fecha_hoy.strftime("%Y%m%d")+'_intradiario_1.csv'
servidor = 'sftp.enagas.es'
puerto = 22
usuario = 'usuCoefTempN1Esc'
contraseña = 'usuCoefTempN1Esc.'
ruta_remota = '/coeficientesDelfos/' + Fichero 
ruta_local = Fichero

descargar_archivo_sftp(servidor, puerto, usuario, contraseña, ruta_remota, ruta_local)
subir_archivo_sftp(Fichero, 'sftpgas.sigeenergia.com',22, 'MET', 'Mete2023', '/home/MET/Coeftemp/'+Fichero)
try:
    os.remove(Fichero)
    print("El archivo se ha borrado exitosamente.")
except FileNotFoundError:
    print("El archivo no existe.")
except PermissionError:
    print("No tienes permisos para borrar este archivo.")
except Exception as e:
    print("Se produjo un error:", e)

fecha_hoy = datetime.today()
fecha_1 = fecha_hoy + timedelta(days=1)
Fichero = 'CoefTemp_'+fecha_1.strftime("%Y%m%d")+'_prevision_1.csv'
servidor = 'sftp.enagas.es'
puerto = 22
usuario = 'usuCoefTempN1Esc'
contraseña = 'usuCoefTempN1Esc.'
ruta_remota = '/coeficientesDelfos/' + Fichero 
ruta_local = Fichero
descargar_archivo_sftp(servidor, puerto, usuario, contraseña, ruta_remota, ruta_local)
subir_archivo_sftp(Fichero, 'sftpgas.sigeenergia.com',22, 'MET', 'Mete2023', '/home/MET/Coeftemp/'+Fichero)

try:
    os.remove(Fichero)
    print("El archivo se ha borrado exitosamente.")
except FileNotFoundError:
    print("El archivo no existe.")
except PermissionError:
    print("No tienes permisos para borrar este archivo.")
except Exception as e:
    print("Se produjo un error:", e)