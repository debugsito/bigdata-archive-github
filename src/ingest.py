"""
Script de ingesta - Paso 2 (12 horas) + Paso 7 (expansion a ~125 horas)

Descarga horas puntuales de GH Archive (https://data.gharchive.org/).
En Paso 1-2 bajamos 12 horas (una por mes) para probar el pipeline. En
Paso 7 el profe... digo, el usuario pidio mucha mas data para un trabajo
mas solido, asi que expandimos a ~125 horas: las 12 originales + 57 horas
dispersas nuevas (5 por mes) + un bloque de 4 dias seguidos (para tener
por fin algo parecido a una serie de tiempo real por repo, no solo una
foto puntual como haciamos antes).

Hallazgo raro al armar la lista nueva: TODAS las horas entre 00 y 09 UTC
devuelven 404 en este GH Archive, sin importar la fecha (lo probamos con
curl -sI en varias fechas distintas). Solo existen las horas 10 a 23. No
tengo del todo claro por que (¿el crawler que arma estos archivos no corre
de madrugada UTC?), pero bueno, por eso todas las horas nuevas de abajo
estan entre 10 y 23.
"""

import gzip
import json
import os
import urllib.request

# Carpeta donde se guardan los archivos crudos descargados
CARPETA_RAW = "data/raw"

# Las 12 horas originales de Paso 1-2, una por mes (ver PROGRESO.md)
HORAS_ORIGINALES = [
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

# --- Paso 7: expansion ---
# 5 horas nuevas por mes, en dias y horas distintas a la original de ese
# mes (para tener mas variedad dentro de cada mes), siempre entre 10 y 23
# por el hallazgo de arriba.
_MESES = [
    ("2025", "08"), ("2025", "09"), ("2025", "10"), ("2025", "11"),
    ("2025", "12"), ("2026", "01"), ("2026", "02"), ("2026", "03"),
    ("2026", "04"), ("2026", "05"), ("2026", "06"), ("2026", "07"),
]
_DIAS = [3, 8, 15, 22, 27]
_HORAS_DEL_DIA = ["10", "13", "16", "19", "22"]

HORAS_DISPERSAS_NUEVAS = []
for _anio, _mes in _MESES:
    for _dia, _hora in zip(_DIAS, _HORAS_DEL_DIA):
        # julio 2026 solo tiene datos hasta "hoy" (11 de julio), asi que
        # nos saltamos los dias que todavia no existen
        if _anio == "2026" and _mes == "07" and _dia > 8:
            continue
        HORAS_DISPERSAS_NUEVAS.append(f"{_anio}-{_mes}-{_dia:02d}-{_hora}")

# Bloque "cuasi-continuo": 4 dias seguidos (1-4 junio 2026), horas 10 a 23
# (las unicas que existen). Esto es lo que nos va a permitir ver el MISMO
# repo varias veces en dias distintos y armar algo parecido a una
# tendencia real, no solo un conteo de una hora suelta.
HORAS_BLOQUE_CONTINUO = [
    f"2026-06-{dia:02d}-{hora:02d}"
    for dia in [1, 2, 3, 4]
    for hora in range(10, 24)
]

HORAS = HORAS_ORIGINALES + HORAS_DISPERSAS_NUEVAS + HORAS_BLOQUE_CONTINUO

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
    Tambien cuenta cuantas lineas totales y cuantas vinieron de bots.

    Esto lo usamos en Paso 1-2 para validar que el formato funcionaba con
    pocas horas. Con las ~125 horas del Paso 7 ya no lo llamamos para
    cada hora (seria lento hacerlo en Python puro para tanto archivo) --
    el conteo "de verdad", con la limpieza completa, se hace en el Paso 3
    con Spark, que es mucho mas rapido para esto. Dejamos la funcion aca
    por si se quiere revisar una hora suelta a mano."""

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

    print(f"total de horas a descargar: {len(HORAS)}")
    for i, hora in enumerate(HORAS, start=1):
        print(f"[{i}/{len(HORAS)}] {hora}")
        descargar_hora(hora)


if __name__ == "__main__":
    main()
