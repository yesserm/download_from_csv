import csv, os, requests, time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

CSV_FILE = "enlaces_descargar.csv"
CARPETA = "descargas"
MAX_HILOS = 10
CHUNK_SIZE = 1024 * 1024  # 1 MB
REINTENTOS = 5

def descargar_archivo(nombre, url, extension):
    os.makedirs(CARPETA, exist_ok=True)
    archivo_salida = os.path.join(CARPETA, f"{nombre}.{extension}")

    # Comenzar desde el tamaño ya descargado (reanudación)
    resume_byte_pos = os.path.getsize(archivo_salida) if os.path.exists(archivo_salida) else 0

    for intento in range(REINTENTOS):
        try:
            headers = {"Range": f"bytes={resume_byte_pos}-"} if resume_byte_pos else {}
            r = requests.get(url, stream=True, headers=headers, timeout=30)
            r.raise_for_status()

            total_size = int(r.headers.get("content-length", 0)) + resume_byte_pos
            modo = "ab" if resume_byte_pos else "wb"

            barra = tqdm(total=total_size, initial=resume_byte_pos, unit="B", unit_scale=True, desc=nombre, ascii=True)

            with open(archivo_salida, modo) as f:
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        barra.update(len(chunk))

            barra.close()
            print(f"✅ Guardado en: {archivo_salida}")
            return
        except Exception as e:
            print(f"⚠️ Error descargando {nombre}: {e} (Intento {intento+1}/{REINTENTOS})")
            time.sleep(3)  # Espera antes de reintentar

    print(f"❌ Falló la descarga de {nombre} después de {REINTENTOS} intentos.")

def descargar_archivo_sin_prints(nombre, url, extension):
    archivo_salida = f"{nombre}.{extension}"
    resume_pos = os.path.getsize(archivo_salida) if os.path.exists(archivo_salida) else 0

    for intento in range(5):
        try:
            headers = {"Range": f"bytes={resume_pos}-"} if resume_pos else {}
            r = requests.get(url, stream=True, headers=headers, timeout=30)
            r.raise_for_status()

            total_size = int(r.headers.get("content-length", 0)) + resume_pos
            modo = "ab" if resume_pos else "wb"
            barra = tqdm(total=total_size, initial=resume_pos, unit="B", unit_scale=True, desc=nombre, ascii=True)

            with open(archivo_salida, modo) as f:
                for chunk in r.iter_content(1024*1024):
                    if chunk:
                        f.write(chunk)
                        barra.update(len(chunk))
            barra.close()
            break
        except Exception:
            time.sleep(3)

def leer_csv_y_descargar():
    with open(CSV_FILE, encoding="latin-1", newline="") as csvfile:
        lector = csv.DictReader(csvfile)
        with ThreadPoolExecutor(max_workers=MAX_HILOS) as executor:
            for fila in lector:
                executor.submit(descargar_archivo, fila["nombre"].strip(), fila["url"].strip(), fila["tipo_video"].strip())

if __name__ == "__main__":
    leer_csv_y_descargar()
