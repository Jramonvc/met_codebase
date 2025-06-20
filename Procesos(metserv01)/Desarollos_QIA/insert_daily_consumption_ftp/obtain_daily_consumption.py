from ftplib import FTP_TLS, all_errors
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
import bz2
import shutil
import paramiko
import gzip
import zipfile
from pathlib import Path
from stat import S_ISDIR

load_dotenv("vars.env") 
carpeta_destino = os.path.join(os.getcwd(), "files")
os.makedirs(carpeta_destino, exist_ok=True)

def main():
    #print("\nDESCARGANDO CONSUMOS DE VIESGO")
    download_viesgo("03_Salida_Medida")
    download_viesgo("02_Salida_VAL")
    download_viesgo("01_Salida_FACT")

    #print("\nDESCARGANDO CONSUMOS DE E-REDES")
    download_eredes("00_Salida_CSD")
    download_eredes("02_Salida_VAL")
    download_eredes("01_Salida_FACT")

    #print("\nDESCARGANDO CONSUMOS DE ASEME")
    download_aseme("01_Salida_FACT")
    download_aseme("02_Salida_VAL")

    #print("\nDESCARGANDO CONSUMOS DE ENDESA")
    download_endesa("01_SALIDA_FACT")
    download_endesa("02_SALIDA_VAL")

    #print("\nDESCARGANDO CONSUMOS DE FENOSA")
    download_femosa("02_Salida")
    download_femosa("02_Salida/02_Salida_VAL")
    download_femosa("02_Salida/01_Salida_FACT")

    #print("\nDESCARGANDO CONSUMOS DE IBERDROLA")
    #P1/P2
    download_iberdrola("0021-1641/01_Salida_FACT")
    download_iberdrola("0032-1641/01_Salida_FACT")
    download_iberdrola("0118-1641/01_Salida_FACT")
    #P5
    download_iberdrola("0021-1641/02_Salida_VAL")
    download_iberdrola("0032-1641/02_Salida_VAL")
    download_iberdrola("0118-1641/02_Salida_VAL")
    
def get_ultimos_tres_dias():
    hoy = datetime.now()
    return [(hoy - timedelta(days=i)).strftime("%Y%m%d") for i in range(10)]   

def download_viesgo(ruta):
    print(f"\tDescargando ruta: {ruta}")
    sftp_viesgo= conectar_sftp(os.getenv("VIESGO_HOST"), int(os.getenv("VIESGO_PORT")), 
                            os.getenv("VIESGO_USER"), os.getenv("VIESGO_PASS"))
    archivos_a_descargar = obtener_archivos_validos_sftp(sftp_viesgo, ruta) 

    for archivo in archivos_a_descargar:
        ruta_local = os.path.join(carpeta_destino, "VIESGO_" + Path(archivo).name)
        if ruta_local.endswith(".md5"):  continue
        descargar_archivo_sftp(sftp_viesgo, archivo, ruta_local)
        if ruta_local.endswith(".zip"):  descomprimir_zip(ruta_local, carpeta_destino, "VIESGO_")
    cerrar_conexion_sftp(sftp_viesgo)
    
def download_eredes(ruta):
    print(f"\tDescargando ruta: {ruta}")
    sftp_eredes= conectar_sftp(os.getenv("EREDES_HOST"), int(os.getenv("EREDES_PORT")), 
                            os.getenv("EREDES_USER"), os.getenv("EREDES_PASS"))
    archivos_a_descargar = obtener_archivos_validos_sftp(sftp_eredes, ruta) 

    for archivo in archivos_a_descargar:
        ruta_local = os.path.join(carpeta_destino, "EREDES_" + Path(archivo).name)
        descargar_archivo_sftp(sftp_eredes, archivo, ruta_local)
    cerrar_conexion_sftp(sftp_eredes)


def download_iberdrola(ruta):
    print(f"\tDescargando ruta: {ruta}")
    sftp_iberdrola = conectar_sftp(os.getenv("IBERDROLA_HOST"), int(os.getenv("IBERDROLA_PORT")), 
                            os.getenv("IBERDROLA_USER"), os.getenv("IBERDROLA_PASS"))
    archivos_a_descargar = obtener_archivos_validos_sftp(sftp_iberdrola, ruta) 

    for archivo in archivos_a_descargar:
        ruta_local = os.path.join(carpeta_destino, "IBERDROLA_" + Path(archivo).name)
        descargar_archivo_sftp(sftp_iberdrola, archivo, ruta_local)
        if ruta_local.endswith(".zip"):  descomprimir_zip(ruta_local, carpeta_destino, "IBERDROLA_")
        elif ruta_local.endswith(".gz"): descomprimir_gz(ruta_local, carpeta_destino, "")
            


def download_femosa(ruta):
    print(f"\tDescargando ruta: {ruta}")
    sftp_femosa = conectar_sftp(
        os.getenv("FEMOSA_HOST"),
        int(os.getenv("FEMOSA_PORT")),
        os.getenv("FEMOSA_USER"),
        os.getenv("FEMOSA_PASS")
    )
    
    
    archivos_a_descargar = obtener_archivos_validos_sftp(sftp_femosa, ruta)

    for archivo in archivos_a_descargar:
        ruta_local = os.path.join(carpeta_destino, "FENOSA_" + Path(archivo).name)
        descargar_archivo_sftp(sftp_femosa, archivo, ruta_local)
        if ruta_local.endswith(".bz2"):  descomprimir_bz2(ruta_local, carpeta_destino, "")


    cerrar_conexion_sftp(sftp_femosa)



def download_aseme(ruta):
    print(f"\tDescargando ruta: {ruta}")
    ftps_aseme = conectar_ftps(os.getenv("ASEME_HOST"), int(os.getenv("ASEME_PORT")), 
                            os.getenv("ASEME_USER"), os.getenv("ASEME_PASS"))
    archivo_a_descargar = obtener_archivo_valido(ftps_aseme, ruta) 

    for archivo in archivo_a_descargar:
        ruta_archivo_bz2 = os.path.join(carpeta_destino, archivo)
        descargar_archivo(ftps_aseme, archivo, ruta_archivo_bz2)
        descomprimir_bz2(ruta_archivo_bz2, carpeta_destino, "ASEME_")
    cerrar_conexion(ftps_aseme)


def download_endesa(ruta):
    print(f"\tDescargando ruta: {ruta}")
    sftp_endesa = conectar_sftp(
        os.getenv("ENDESA_HOST"),
        int(os.getenv("ENDESA_PORT")),
        os.getenv("ENDESA_USER"),
        os.getenv("ENDESA_PASS")
    )

    archivos_a_descargar = obtener_archivos_validos_sftp(sftp_endesa, ruta) 

    for archivo in archivos_a_descargar:
        ruta_local = os.path.join(carpeta_destino, "ENDESA_" + Path(archivo).name)
        descargar_archivo_sftp(sftp_endesa, archivo, ruta_local)
        if ruta_local.endswith(".zip"):  descomprimir_zip(ruta_local, carpeta_destino, "ENDESA_")
        if ruta_local.endswith(".ZIP"):  descomprimir_zip(ruta_local, carpeta_destino, "ENDESA_")

def listar_carpetas_sftp(sftp, ruta):
    try:
        sftp.chdir(ruta)
        carpetas = []
        for item in sftp.listdir_attr():
            if str(item.longname).startswith('d'):  # método simple y común
                carpetas.append(item.filename)
        return carpetas
    except Exception as e:
        print(f"\t[ERROR] No se pudieron listar carpetas en {ruta}: {e}")
        return []


def conectar_sftp(host, port, user, password):
    try:
        print(f"\n\tIntentando conectar a {host}:{port} con usuario '{user}'...")
        transport = paramiko.Transport((host, port))
        transport.banner_timeout = 30  # importante
        transport.connect(username=user, password=password)
        
        sftp = paramiko.SFTPClient.from_transport(transport)
        print(f"\n\tConectado a {host} por SFTP en el puerto {port}")
        
        return sftp
    except Exception as e:
        print(f"Error de conexión SFTP: {e}")
        return None
        
def cerrar_conexion_sftp(sftp):
    try:
        if sftp:
            sftp.close()
            print("\tConexión SFTP cerrada")
    except Exception as e:
        print(f"\tError al cerrar SFTP: {e}")

def obtener_archivos_validos_sftp(sftp, directorio):
    try:
        sftp.chdir(directorio)
        archivos = sftp.listdir()

        archivos_validos = []
        fechas_validas = get_ultimos_tres_dias()

        for archivo in archivos:
            if archivo.startswith(("P1D", "P2D", "P5D", "P1", "P2", "P5", "F5", "F5D")):
                try:
                    partes = archivo.split("_")
                    fecha_str = partes[-1].split(".")[0]  # 'YYYYMMDD'
                    if fecha_str in fechas_validas:
                        archivos_validos.append(archivo)
                except Exception as e:
                    print(f"\tError procesando archivo {archivo}: {e}")
                    continue

        if archivos_validos:
            print(f"\tArchivos seleccionados: {archivos_validos}")
            return archivos_validos
        else:
            print(f"\tNo se encontraron archivos válidos en: {directorio}")
            return []

    except Exception as e:
        print(f"\tError al acceder al directorio SFTP: {e}")
        return []


def obtener_archivo_valido(ftps, directorio):
    try:
        ftps.cwd(directorio)
        archivos = []
        ftps.retrlines('NLST', archivos.append)

        archivos_validos = []
        fechas_validas = get_ultimos_tres_dias()

        for archivo in archivos:
            if archivo.startswith(("P1D", "P2D", "P5D", "P1", "P2", "P5", "F5", "F5D")):
                try:
                    fecha_str = archivo.split('_')[-1][:8]  # 'YYYYMMDD'
                    if fecha_str in fechas_validas:
                        archivos_validos.append(archivo)
                except Exception as e:
                    print(f"\tError procesando archivo {archivo}: {e}")
                    continue

        if archivos_validos:
            print(f"\tArchivos seleccionados: {archivos_validos}")
            return archivos_validos
        else:
            print("\tNo se encontraron archivos válidos.")
            return []

    except all_errors as e:
        print(f"\tError al obtener archivos en {directorio}: {e}")
        return []


def descargar_archivo_sftp(sftp, archivo_remoto, ruta_local):
    try:
        with sftp.file(archivo_remoto, 'rb') as remote_file, open(ruta_local, 'wb') as local_file:
            shutil.copyfileobj(remote_file, local_file)
        print(f"\tArchivo descargado: {archivo_remoto}")
    except Exception as e:
        print(f"\tError al descargar archivo {archivo_remoto}: {e}")

def descomprimir_zip(ruta_zip, carpeta_destino, prefijo=""):
    try:
        with zipfile.ZipFile(ruta_zip, 'r') as zip_ref:
            for archivo in zip_ref.namelist():
                nuevo_nombre = prefijo + os.path.basename(archivo)
                ruta_destino = os.path.join(carpeta_destino, nuevo_nombre)
                with zip_ref.open(archivo) as archivo_origen, open(ruta_destino, 'wb') as archivo_salida:
                    shutil.copyfileobj(archivo_origen, archivo_salida)
        os.remove(ruta_zip)
        print(f"\tArchivo descomprimido correctamente en {carpeta_destino}")
    except Exception as e:
        print(f"\tError al descomprimir archivo .zip: {e}")

def descomprimir_gz(ruta_gz, carpeta_destino, prefijo=""):
    try:
        nombre_salida = prefijo + Path(ruta_gz).stem  
        ruta_salida = os.path.join(carpeta_destino, nombre_salida)

        with gzip.open(ruta_gz, 'rb') as f_in:
            with open(ruta_salida, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        print(f"\tArchivo descomprimido: {ruta_salida}")
        os.remove(ruta_gz)
    except Exception as e:
        print(f"\tError al descomprimir archivo .gz: {e}")
        
def conectar_ftps(host, port, user, password):
    try:
        ftps = FTP_TLS()
        ftps.connect(host, port, timeout=10)
        print(f"\n\tConectado a {host} en el puerto {port}")

        # Autenticación y forzar cifrado
        ftps.login(user, password)
        ftps.prot_p()  # Habilitar cifrado de datos

        return ftps
    except all_errors as e:
        print(f"\tError de conexión FTP: {e}")
        return None

def listar_directorio(ftps, directorio="."):
    try:
        print(f"\tListado de archivos en {directorio}:")
        ftps.cwd(directorio)  # Cambiar de directorio
        ftps.retrlines('LIST')  # Listar contenido
    except all_errors as e:
        print(f"\tError al listar directorio: {e}")

def cerrar_conexion(ftps):
    try:
        if ftps:
            ftps.quit()
            print("\tConexión cerrada")
    except Exception:
        print("\tNo se pudo cerrar la conexión (quizás no se estableció)")





def descargar_archivo(ftps, nombre_remoto, nombre_local):
    try:
        with open(nombre_local, 'wb') as f:
            ftps.retrbinary(f'RETR {nombre_remoto}', f.write)
        print(f"\tArchivo descargado: {nombre_local}")
    except all_errors as e:
        print(f"\tError al descargar archivo: {e}")

def descomprimir_bz2(path_entrada, carpeta_salida, prefijo):
    nombre_base = os.path.basename(path_entrada)
    nombre_salida = prefijo + nombre_base.replace(".bz2", "")
    ruta_salida = os.path.join(carpeta_salida, nombre_salida)

    with bz2.BZ2File(path_entrada, 'rb') as file_in:
        with open(ruta_salida, 'wb') as file_out:
            shutil.copyfileobj(file_in, file_out)
    
    print(f"\tArchivo descomprimido en: {ruta_salida}")
    os.remove(path_entrada)


if __name__ == '__main__':
    main()