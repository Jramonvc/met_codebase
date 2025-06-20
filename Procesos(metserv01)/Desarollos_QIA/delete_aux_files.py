import os
import shutil

carpetas = [
    "C:/Procesos/Desarrollos_QIA/insert_daily_indicators_esios_web/Liquicomun/A1/files",
    "C:/Procesos/Desarrollos_QIA/insert_daily_indicators_esios_web/Liquicomun/A1/files_aux",
    "C:/Procesos/Desarrollos_QIA/insert_daily_indicators_esios_web/Liquicomun/A2/files",
    "C:/Procesos/Desarrollos_QIA/insert_daily_indicators_esios_web/Liquicomun/A2/files_aux",
    "C:/Procesos/Desarrollos_QIA/insert_daily_indicators_esios_web/Liquicomun/C2/files",
    "C:/Procesos/Desarrollos_QIA/insert_daily_indicators_esios_web/Liquicomun/C2/files_aux",
    "C:/Procesos/Desarrollos_QIA/insert_daily_indicators_esios_web/NecResSub/files_xml",
    "C:/Procesos/Desarrollos_QIA/insert_daily_indicators_esios_web/TotalRPdvpPrec/files_xml",
    "C:/Procesos/Desarrollos_QIA/insert_daily_indicators_esios_web/UltimoProgramaP48/hourly_process/files_xml",
]
    
def main():
    borrar_archivos_en_carpetas(carpetas)


def borrar_archivos_en_carpetas(rutas_carpetas):
    for ruta in rutas_carpetas:
        if os.path.exists(ruta) and os.path.isdir(ruta):
            for elemento in os.listdir(ruta):
                ruta_elemento = os.path.join(ruta, elemento)
                try:
                    if os.path.isfile(ruta_elemento) or os.path.islink(ruta_elemento):
                        os.remove(ruta_elemento)
                        print(f"Archivo eliminado: {ruta_elemento}")
                    elif os.path.isdir(ruta_elemento):
                        shutil.rmtree(ruta_elemento)
                        print(f"Carpeta eliminada: {ruta_elemento}")
                except Exception as e:
                    print(f"Error al eliminar {ruta_elemento}: {e}")
        else:
            print(f"La ruta no existe o no es una carpeta: {ruta}")
            
if __name__ == '__main__':
    main()