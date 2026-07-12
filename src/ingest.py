"""
Script de ingesta - Fase 2

Descarga las 12 horas puntuales de GH Archive (https://data.gharchive.org/)
elegidas para el proyecto (una por mes, ver PROGRESO.md), las descomprime,
y cuenta cuantos eventos hay de los tipos que nos interesan. En Fase 1 se
probo esto mismo con solo 3 horas y funciono bien.

No usa Spark todavia: son pocos MB por hora, asi que Python normal alcanza.
La limpieza "de verdad" (duplicados, nulos criticos) se hace en la fase
siguiente, ya con PySpark.
"""

import gzip
import json
import os
import urllib.request

# Carpeta donde se guardan los archivos crudos descargados
CARPETA_RAW = "data/raw"

# Las 12 horas de muestreo del proyecto, una por mes (ver PROGRESO.md)
HORAS = [
    "2025-08-12-14",
    "2025-09-23-18",
    "2025-10-15-10",
    "2025-11-20-21",
    "2025-12-10-14",
    "2026-01-14-16",
    "2026-02-11-20",
    "2026-03-18-13",
    "2026-04-22-17",
    "2026-05-19-10",
    "2026-06-16-19",
    "2026-07-08-15",
]

# Tipos de evento que nos interesan para el proyecto (el resto se descarta)
EVENTOS_DE_INTERES = {
    "PushEvent",
    "IssuesEvent",
    "PullRequestEvent",
    "WatchEvent",
    "ForkEvent",
}


def descargar_hora(hora):
    """Descarga el archivo .json.gz de una hora especifica de GH Archive,
    si todavia no lo tenemos guardado en data/raw."""
    nombre_archivo = f"{hora}.json.gz"
    ruta_local = os.path.join(CARPETA_RAW, nombre_archivo)

    if os.path.exists(ruta_local):
        print(f"  ya existe: {ruta_local} (no se vuelve a descargar)")
        return ruta_local

    url = f"https://data.gharchive.org/{nombre_archivo}"
    print(f"  descargando {url} ...")

    # GH Archive (detras de Cloudflare) rechaza el user-agent por defecto
    # de urllib con un 403, asi que simulamos uno de navegador comun.
    peticion = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(peticion) as respuesta, open(ruta_local, "wb") as f:
        f.write(respuesta.read())

    print(f"  guardado en {ruta_local}")
    return ruta_local


def contar_eventos(ruta_archivo):
    """Lee un archivo .json.gz de GH Archive linea por linea (cada linea es
    un evento en JSON) y cuenta cuantos eventos hay de cada tipo de interes.
    Tambien cuenta cuantas lineas totales y cuantas vinieron de bots."""

    conteo_por_tipo = {tipo: 0 for tipo in EVENTOS_DE_INTERES}
    total_lineas = 0
    total_bots = 0

    with gzip.open(ruta_archivo, "rt", encoding="utf-8") as f:
        for linea in f:
            total_lineas += 1

            try:
                evento = json.loads(linea)
            except json.JSONDecodeError:
                # linea corrupta, la saltamos
                continue

            tipo = evento.get("type")
            if tipo not in EVENTOS_DE_INTERES:
                continue

            actor = evento.get("actor") or {}
            login = actor.get("login", "")
            es_bot = (
                login.endswith("-bot")
                or login.endswith("[bot]")
                or "dependabot" in login.lower()
            )
            if es_bot:
                total_bots += 1
                continue

            conteo_por_tipo[tipo] += 1

    return total_lineas, total_bots, conteo_por_tipo


def main():
    os.makedirs(CARPETA_RAW, exist_ok=True)

    for hora in HORAS:
        print(f"\n=== Hora: {hora} ===")
        ruta = descargar_hora(hora)

        total_lineas, total_bots, conteo_por_tipo = contar_eventos(ruta)

        print(f"  total de eventos en el archivo: {total_lineas}")
        print(f"  eventos de bots descartados: {total_bots}")
        print("  eventos de interes (sin bots):")
        for tipo, cantidad in conteo_por_tipo.items():
            print(f"    {tipo}: {cantidad}")


if __name__ == "__main__":
    main()
