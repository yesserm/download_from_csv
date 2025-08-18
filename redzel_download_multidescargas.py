import requests, os, time, csv, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Configuración
CHUNK_SIZE_MB = 5
SUBTHREADS = 3
MAX_FILES = 10
REINTENTOS_SEGMENTO = 5
ESPERA_REINTENTO = 3
CARPETA = "descargas"
CSV_FILE = "enlaces_descargar.csv"

resultados = {"ok": [], "fail": []}

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
        salida = os.path.join(CARPETA, f"{nombre}.{extension}")
        tamaño = int(requests.head(url).headers.get("Content-Length", 0))
        if tamaño == 0:
            raise RuntimeError("No se pudo obtener tamaño del archivo")
        rango = tamaño // SUBTHREADS
        base_temp = nombre.replace(" ", "_")

        # Progreso previo
        progreso_existente = 0
        for i in range(SUBTHREADS):
            parte = os.path.join(CARPETA, f"{base_temp}.part{i}")
            if os.path.exists(parte):
                progreso_existente += os.path.getsize(parte)

        with tqdm(total=tamaño, initial=progreso_existente, unit="B", unit_scale=True, desc=nombre, ascii=True) as barra:
            while True:
                try:
                    with ThreadPoolExecutor(max_workers=SUBTHREADS) as subexec:
                        futuros = []
                        for i in range(SUBTHREADS):
                            start = i * rango
                            end = tamaño - 1 if i == SUBTHREADS - 1 else (start + rango - 1)
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

                    print(f"\n✅ Descarga finalizada: {salida} ({tamaño/1024/1024:.2f} MB)")
                    resultados["ok"].append(nombre)
                    break
                except Exception:
                    time.sleep(ESPERA_REINTENTO)
    except Exception as e:
        print(f"\n❌ Error en {nombre}: {e}")
        traceback.print_exc(limit=1)
        resultados["fail"].append(nombre)

def leer_csv_y_descargar():
    with open(CSV_FILE, encoding="utf-8", newline="") as f:
        lector = csv.DictReader(f, delimiter=",", quotechar='"')
        next(f)
        with ThreadPoolExecutor(max_workers=MAX_FILES) as executor:
            futuros = []
            for fila in lector:
                print(fila)
                nombre, enlace, extension = fila["nombre"], fila["enlace"], fila["extension"]
                print(nombre, " - ",enlace, " - ", extension)
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