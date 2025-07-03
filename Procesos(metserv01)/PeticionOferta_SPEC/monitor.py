# -*- coding: utf-8 -*-
"""
Created on Tue Jul  1 16:43:16 2025

@author: amin.sahabi
"""

import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess

class Watcher:
    carpeta_entrada = "entrada"

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.carpeta_entrada, recursive=False)
        self.observer.start()
        print("Monitorizando carpeta de entrada...")
        try:
            while True:
                time.sleep(5)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()

class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if event.src_path.endswith(".xlsx"):
            print(f"Nuevo archivo detectado: {event.src_path}")
            subprocess.run(["python", "automatizar_transferencia.py", event.src_path])

if __name__ == "__main__":
    Watcher().run()
