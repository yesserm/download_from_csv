import requests, os, time, csv, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import re
import chardet

# Configuración
CHUNK_SIZE_MB = 5
SUBTHREADS = 10
MAX_FILES = 10
REINTENTOS_SEGMENTO = 5
ESPERA_REINTENTO = 3
CARPETA = "descargas"
CSV_FILE = "enlaces_descargar.csv"

resultados = {"ok": [], "fail": []}

def limpiar_nombre_archivo(nombre):
    nombre = nombre.strip()
    nombre = re.sub(r'[\\/*?:"<>|]', "_", nombre)  # Sustituye caracteres inválidos
    return nombre

def obtener_tam_archivo(url):
    try:
        # Intento con HEAD
        r = requests.head(url, allow_redirects=True, timeout=15)
        tama = int(r.headers.get("Content-Length", 0))
        if tama > 0:
            return tam
    except Exception:
        pass

    try:
        # Fallback con GET parcial
        r = requests.get(url, headers={"Range": "bytes=0-0"}, stream=True, timeout=15)
        content_range = r.headers.get("Content-Range")
        if content_range:
            return int(content_range.split("/")[-1])
    except Exception:
        pass

    return 0



def detectar_encoding(ruta):
    with open(ruta, 'rb') as f:
        resultado = chardet.detect(f.read())
    return resultado['encoding']

def descargar_parcial(url, start, end, idx, nombre_base, barra):
    nombre_temp = os.path.join(CARPETA, f"{nombre_base}.part{idx}")
    modo = "ab" if os.path.exists(nombre_temp) else "wb"
    bytes_existentes = os.path.getsize(nombre_temp) if os.path.exists(nombre_temp) else 0
    start += bytes_existentes

    for _ in range(REINTENTOS_SEGMENTO):
        try:
            headers = {"Range": f"bytes={start}-{end}"}
            r = requests.get(url, headers=headers, stream=True, timeout=30)
            r.raise_for_status()
            with open(nombre_temp, modo) as f:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        barra.update(len(chunk))
            return nombre_temp
        except Exception:
            time.sleep(ESPERA_REINTENTO)
    raise RuntimeError(f"Fallo el segmento {idx}")

def descargar_archivo(nombre, url, extension):
    try:
        os.makedirs(CARPETA, exist_ok=True)
        base_temp = limpiar_nombre_archivo(nombre.replace(" ", "_"))
        salida = os.path.join(CARPETA, f"{base_temp}.{extension}")
        tam = obtener_tam_archivo(url)
        if tam == 0:
            raise RuntimeError("No se pudo obtener tamaño del archivo (ni con fallback)")
        rango = tam // SUBTHREADS
        base_temp = nombre.replace(" ", "_")

        # Progreso previo
        progreso_existente = 0
        for i in range(SUBTHREADS):
            parte = os.path.join(CARPETA, f"{base_temp}.part{i}")
            if os.path.exists(parte):
                progreso_existente += os.path.getsize(parte)

        with tqdm(total=tam, initial=progreso_existente, unit="B", unit_scale=True, desc=nombre, ascii=True) as barra:
            while True:
                try:
                    with ThreadPoolExecutor(max_workers=SUBTHREADS) as subexec:
                        futuros = []
                        for i in range(SUBTHREADS):
                            start = i * rango
                            end = tam - 1 if i == SUBTHREADS - 1 else (start + rango - 1)
                            futuros.append(
                                subexec.submit(descargar_parcial, url, start, end, i, base_temp, barra)
                            )
                        for f in futuros:
                            f.result()

                    # Ensamblar
                    with open(salida, "wb") as final:
                        for i in range(SUBTHREADS):
                            parte = os.path.join(CARPETA, f"{base_temp}.part{i}")
                            with open(parte, "rb") as pf:
                                final.write(pf.read())
                            os.remove(parte)

                    print(f"\n✅ Descarga finalizada: {salida} ({tam/1024/1024:.2f} MB)")
                    resultados["ok"].append(nombre)
                    break
                except Exception:
                    time.sleep(ESPERA_REINTENTO)
    except Exception as e:
        print(f"\n❌ Error en {nombre}: {e}")
        traceback.print_exc(limit=1)
        resultados["fail"].append(nombre)

encoding_detectado = detectar_encoding(CSV_FILE)
def leer_csv_y_descargar():
    with open(CSV_FILE, mode='r', encoding=encoding_detectado, errors='replace') as f:
        lector = csv.DictReader(f)
        with ThreadPoolExecutor(max_workers=MAX_FILES) as executor:
            futuros = []
            for fila in lector:
                nombre, enlace, extension = fila["nombre"], fila["enlace"], fila["extension"]
                futuros.append(
                    executor.submit(
                        descargar_archivo,
                        nombre.strip(),
                        enlace.strip(),
                        extension.strip(),
                    )
                )
            for f in as_completed(futuros):
                pass  # forzar captura de excepciones

if __name__ == "__main__":
    try:
        leer_csv_y_descargar()
    except Exception as e:
        print(f"💥 Error no controlado: {e}")
    finally:
        print("\n📊 Resumen:")
        print(f"   ✅ Completados: {len(resultados['ok'])}")
        print(f"   ❌ Fallidos: {len(resultados['fail'])}")
        if resultados["fail"]:
            print("   Archivos fallidos:", ", ".join(resultados["fail"]))
        input("\nProceso terminado. Pulsa Enter para cerrar...")