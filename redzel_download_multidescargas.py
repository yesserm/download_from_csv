import requests, os, time, csv, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import re
import chardet

# =============================
# Configuración
# =============================
CHUNK_SIZE_MB = 5
SUBTHREADS = 10
MAX_FILES = 20
REINTENTOS_SEGMENTO = 50
ESPERA_REINTENTO = 3
CARPETA = "descargas"
CSV_FILE = "enlaces_descargar.csv"

resultados = {"ok": [], "fail": []}

# =============================
# Utilidades
# =============================
def limpiar_nombre_archivo(nombre):
    nombre = nombre.strip()
    return re.sub(r'[\\/*?:"<>|]', "_", nombre)

def detectar_encoding(ruta):
    with open(ruta, 'rb') as f:
        return chardet.detect(f.read())['encoding']

def obtener_url_vauth(url):
    """Devuelve la URL final si hay redirect por Range, o None si no."""
    try:
        r = requests.get(url, headers={"Range": "bytes=0-0"},
                         stream=True, timeout=15, allow_redirects=False)
        if r.is_redirect or r.status_code in (301, 302, 303, 307, 308):
            return r.headers.get("Location")
    except Exception:
        pass
    return None

# =============================
# Detección de tamaño
# =============================
def obtener_tam_archivo(url):
    """Intenta determinar el tamaño del archivo (HEAD, Range) sobre URL original o final."""
    # 1. Probar con la original
    try:
        r = requests.head(url, allow_redirects=True, timeout=15)
        if "Content-Length" in r.headers and int(r.headers["Content-Length"]) > 0:
            return int(r.headers["Content-Length"])
    except Exception:
        pass
    try:
        r = requests.get(url, headers={"Range": "bytes=0-0"}, stream=True,
                         timeout=15, allow_redirects=True)
        cr = r.headers.get("Content-Range")
        if cr and "/" in cr:
            return int(cr.split("/")[-1])
    except Exception:
        pass

    # 2. Probar con la URL vauth
    url_vauth = obtener_url_vauth(url)
    if url_vauth:
        try:
            r = requests.head(url_vauth, allow_redirects=True, timeout=15)
            if "Content-Length" in r.headers and int(r.headers["Content-Length"]) > 0:
                return int(r.headers["Content-Length"])
        except Exception:
            pass
        try:
            r = requests.get(url_vauth, headers={"Range": "bytes=0-0"}, stream=True,
                             timeout=15, allow_redirects=True)
            cr = r.headers.get("Content-Range")
            if cr and "/" in cr:
                return int(cr.split("/")[-1])
        except Exception:
            pass

    return 0

# =============================
# Descargas
# =============================
def descargar_directo(nombre, url, extension, tam=None):
    salida = os.path.join(CARPETA, f"{nombre}.{extension}")
    if not tam:
        print(f"⚠️ Tamaño desconocido para {nombre}, usando descarga directa...")
    else:
        print(f"⬇️ Descarga directa de {nombre} ({tam/1024/1024:.2f} MB)")
    try:
        with requests.get(url, stream=True, timeout=60, allow_redirects=True) as r:
            r.raise_for_status()
            total_bytes = tam if tam and tam > 0 else None
            with open(salida, "wb") as f, tqdm(total=total_bytes, unit="B", unit_scale=True,
                                               desc=nombre, ascii=True) as barra:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        barra.update(len(chunk))
        print(f"✅ Descarga directa finalizada: {salida}")
        resultados["ok"].append(nombre)
    except Exception as e:
        print(f"❌ Error en descarga directa de {nombre}: {e}")
        resultados["fail"].append(nombre)

def descargar_parcial(url, start, end, idx, nombre_base, barra):
    nombre_temp = os.path.join(CARPETA, f"{nombre_base}.part{idx}")
    modo = "ab" if os.path.exists(nombre_temp) else "wb"
    bytes_exist = os.path.getsize(nombre_temp) if os.path.exists(nombre_temp) else 0
    start += bytes_exist
    for intento in range(REINTENTOS_SEGMENTO):
        try:
            headers = {"Range": f"bytes={start}-{end}"}
            r = requests.get(url, headers=headers, stream=True, timeout=30, allow_redirects=True)
            r.raise_for_status()
            with open(nombre_temp, modo) as f:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        barra.update(len(chunk))
            return
        except Exception:
            time.sleep(min(ESPERA_REINTENTO * (2 ** intento), 60))
    raise RuntimeError(f"Fallo el segmento {idx}")

def descargar_segmentado(nombre, url, extension, tam):
    base_temp = limpiar_nombre_archivo(nombre.replace(" ", "_"))
    salida = os.path.join(CARPETA, f"{base_temp}.{extension}")
    rango = tam // SUBTHREADS
    progreso_existente = sum(
        os.path.getsize(os.path.join(CARPETA, f"{base_temp}.part{i}"))
        for i in range(SUBTHREADS)
        if os.path.exists(os.path.join(CARPETA, f"{base_temp}.part{i}"))
    )
    with tqdm(total=tam, initial=progreso_existente, unit="B", unit_scale=True,
              desc=nombre, ascii=True) as barra:
        while True:
            try:
                with ThreadPoolExecutor(max_workers=SUBTHREADS) as subexec:
                    futuros = [
                        subexec.submit(descargar_parcial, url,
                                       i * rango,
                                       tam - 1 if i == SUBTHREADS - 1 else (i + 1) * rango - 1,
                                       i, base_temp, barra)
                        for i in range(SUBTHREADS)
                    ]
                    for f in futuros:
                        f.result()
                with open(salida, "wb") as final:
                    for i in range(SUBTHREADS):
                        parte = os.path.join(CARPETA, f"{base_temp}.part{i}")
                        with open(parte, "rb") as pf:
                            final.write(pf.read())
                        os.remove(parte)
                print(f"✅ Descarga segmentada finalizada: {salida}")
                resultados["ok"].append(nombre)
                break
            except Exception:
                time.sleep(ESPERA_REINTENTO)

# =============================
# Orquestador
# =============================
def descargar_archivo(nombre, url, extension):
    try:
        os.makedirs(CARPETA, exist_ok=True)
        tam = obtener_tam_archivo(url)  # tamaño desde original o vauth
        if tam == 0:
            descargar_directo(nombre, url, extension)
        else:
            descargar_segmentado(nombre, url, extension, tam)
    except Exception as e:
        print(f"❌ Error en {nombre}: {e}")
        resultados["fail"].append(nombre)

# =============================
# Lector CSV
# =============================
def leer_csv_y_descargar():
    encoding_detectado = detectar_encoding(CSV_FILE)
    with open(CSV_FILE, mode='r', encoding=encoding_detectado, errors='replace') as f:
        lector = csv.DictReader(f)
        with ThreadPoolExecutor(max_workers=MAX_FILES) as executor:
            futuros = [
                executor.submit(descargar_archivo,
                                fila["nombre"].strip(),
                                fila["enlace"].strip(),
                                fila["extension"].strip())
                for fila in lector
                if all(k in fila for k in ("nombre", "enlace", "extension"))
            ]
            for f in as_completed(futuros):
                pass

# =============================
# Main
# =============================
if __name__ == "__main__":
    try:
        leer_csv_y_descargar()
    finally:
        print("\n📊 Resumen:")
        print(f"   ✅ Completados: {len(resultados['ok'])}")
        print(f"   ❌ Fallidos: {len(resultados['fail'])}")
        if resultados["fail"]:
            print("   Archivos fallidos:", ", ".join(resultados["fail"]))