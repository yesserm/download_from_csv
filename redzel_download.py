import csv
import os
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==== CONFIGURACIÓN ====
CSV_FILE = "enlaces_descargar.csv"
CARPETA = "descargas"
MAX_HILOS = 100  # número máximo de descargas simultáneas
CHUNK_SIZE = 1024  # tamaño de cada bloque de descarga (bytes)
# =======================

def descargar_archivo(nombre, url, extension):
    """Descarga un archivo con barra de progreso."""
    try:
        archivo_salida = os.path.join(CARPETA, f"{nombre}.{extension}")

        # Verificar carpeta destino
        os.makedirs(CARPETA, exist_ok=True)

        # Petición HTTP con stream
        r = requests.get(url, stream=True)
        r.raise_for_status()

        # Tamaño total
        total_size = int(r.headers.get("content-length", 0))

        # Barra de progreso
        barra = tqdm(
            total=total_size,
            unit="B", unit_scale=True,
            desc=nombre,
            ascii=True
        )

        with open(archivo_salida, "wb") as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
                    barra.update(len(chunk))

        barra.close()
        print(f"✅ Guardado en: {archivo_salida}")

    except Exception as e:
        print(f"❌ Error descargando {nombre} desde {url}: {e}")

def leer_csv_y_descargar():
    """Lee el CSV y procesa las descargas en paralelo."""
    tareas = []
    with open(CSV_FILE, encoding="latin-1", newline="") as csvfile:
        lector = csv.DictReader(csvfile)
        with ThreadPoolExecutor(max_workers=MAX_HILOS) as executor:
            for fila in lector:
                nombre = fila["nombre"].strip()
                url = fila["enlace"].strip()
                extension = fila["extension"].strip()
                tareas.append(
                    executor.submit(descargar_archivo, nombre, url, extension)
                )

            # Espera a que todas las tareas terminen
            for _ in as_completed(tareas):
                pass

if __name__ == "__main__":
    leer_csv_y_descargar()

