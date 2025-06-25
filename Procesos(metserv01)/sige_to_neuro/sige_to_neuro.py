from ftplib import FTP, error_perm
import os
from datetime import datetime, timedelta
import shutil

#previous day folder
def get_yesterday():
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y%m%d")

# ftp connection
def connect_ftp(host, user, passwd):
    ftp = FTP(host)
    ftp.login(user=user, passwd=passwd)
    return ftp

#check if remote folder exists
def folder_exists(ftp, folder_name):
    try:
        ftp.cwd(folder_name)
        return True
    except Exception:
        return False

#check if file is a directory (for recursive function)
def is_ftp_directory(ftp, name):
    current = ftp.pwd()
    try:
        ftp.cwd(name)
        ftp.cwd(current)
        return True
    except error_perm:
        return False

#download files to local
def download_folder_recursive(ftp, remote_folder_sige, local_folder):
    os.makedirs(local_folder, exist_ok=True)
    ftp.cwd(remote_folder_sige)

    items = ftp.nlst()
    for item in items:
        if is_ftp_directory(ftp, item):
            #recursive part
            download_folder_recursive(ftp, item, os.path.join(local_folder, item))
            ftp.cwd('..')  #go back up after recursion
        else:
            #download file
            local_path = os.path.join(local_folder, item)
            with open(local_path, 'wb') as f:
                try:
                    ftp.retrbinary(f"RETR {item}", f.write)
                    print(f"Downloaded: {os.path.join(remote_folder_sige, item)}")
                except Exception as e:
                    print(f"Failed to download {item}: {e}")


#upload files to neuro
def upload_folder_recursive(ftp, local_folder, remote_folder_neuro):
    #folder creation/checking
    try:
        ftp.mkd(remote_folder_neuro)
        print(f"Created remote folder: {remote_folder_neuro}")
    except Exception:
        pass  #folder already exists

    ftp.cwd(remote_folder_neuro)

    for item in os.listdir(local_folder):
        local_path = os.path.join(local_folder, item)
        if os.path.isdir(local_path):
            #recursive
            upload_folder_recursive(ftp, local_path, item)
            ftp.cwd("..")  #go back up after recursion
        else:
            #upload file
            with open(local_path, "rb") as f:
                try:
                    ftp.storbinary(f"STOR " + item, f)
                    print(f"Uploaded: {os.path.join(remote_folder_neuro, item)}")
                except Exception as e:
                    print(f"Failed to upload {item}: {e}")

#delete files from temporary (local) folder
def clear_folder(folder_path):
    if not os.path.exists(folder_path):
        print(f"Folder does not exist: {folder_path}")
        return

    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        try:
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
            print(f"Deleted: {item_path}")
        except Exception as e:
            print(f"Failed to delete {item_path}: {e}")
    

#main function
def transfer_files():
    sige_host = '82.194.94.192'
    sige_user = 'SigeMet'
    sige_pass = 'bP3gT0kI9zE1cS5f'

    neuro_host = 'xml.neuroenergia.com'
    neuro_user = 'c1641'
    neuro_pass = 'tlynnj63(MB'

    yesterday_folder = get_yesterday()
    remote_path_sige = f"/sige/METElectricidad/Ocsum/Import/{yesterday_folder}"
    local_temp_path = r"C:\tempor" #change when in server

    remote_path_neuro = f"/{yesterday_folder}"

    try:
        sige_ftp = connect_ftp(sige_host, sige_user, sige_pass)

        if not folder_exists(sige_ftp, remote_path_sige):
            print(f"There is no such folder: {yesterday_folder}")
            sige_ftp.quit()
            return

        os.makedirs(local_temp_path, exist_ok=True)
        download_folder_recursive(sige_ftp, remote_path_sige, local_temp_path)
        sige_ftp.quit()
        print(f"Dowload complete for folder: {yesterday_folder}")

        neuro_ftp = connect_ftp(neuro_host, neuro_user, neuro_pass)

        upload_folder_recursive(neuro_ftp, local_temp_path, remote_path_neuro)
        neuro_ftp.quit()
        print(f"Upload complete for folder: {yesterday_folder}")

        clear_folder(local_temp_path)

    except Exception as e:
        print(f"FTP connection or transfer failed: {e}")

if __name__ == "__main__":
    transfer_files()