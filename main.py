from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from dbfread import DBF
from dbf import Table, READ_WRITE
from datetime import datetime, date
import os

# ================================
# CONFIGURACIÓN FASTAPI
# ================================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================================
# ARCHIVOS DBF
# ================================
ZETH50T = "ZETH50T.DBF"
ZETH51T = "ZETH51T.DBF"
ZETH70 = "ZETH70.DBF"
ZETH70_EXT = "ZETH70_EXT.DBF"
HISTORICO_DBF = "VENTAS_HISTORICO.DBF"

CAMPOS_HISTORICO = (
    "EERR C(20);"
    "FECHA D;"
    "N_TICKET C(10);"
    "NOMBRES C(50);"
    "TIPO C(5);"
    "CANT N(6,0);"
    "P_UNIT N(12,2);"
    "CATEGORIA C(20);"
    "SUB_CAT C(20);"
    "COST_UNIT N(12,2);"
    "PRONUM C(10);"
    "DESCRI C(50)"
)

# ================================
# FUNCIONES AUXILIARES
# ================================
def limpiar_texto(valor):
    if isinstance(valor, str):
        return valor.encode("cp850", errors="replace").decode("cp850")
    return valor

def crear_dbf_historico():
    if not os.path.exists(HISTORICO_DBF):
        table = Table(HISTORICO_DBF, CAMPOS_HISTORICO, codepage="cp850")
        table.open(mode=READ_WRITE)
        table.close()
        print("✅ VENTAS_HISTORICO.DBF creado.")

def leer_dbf_existente():
    if not os.path.exists(HISTORICO_DBF):
        return set()
    return {(r["N_TICKET"], r["PRONUM"]) for r in DBF(HISTORICO_DBF, load=True, encoding="cp850")}

def agregar_al_historico(nuevos_registros):
    table = Table(HISTORICO_DBF, codepage="cp850")
    table.open(mode=READ_WRITE)
    for reg in nuevos_registros:
        for k, v in reg.items():
            reg[k] = limpiar_texto(v)
        table.append(reg)
    table.close()

def obtener_costo_producto(pronum, productos):
    producto = productos.get(pronum)
    if producto:
        return float(producto.get("ULCOSREP", 0.0))
    return 0.0

def parsear_fecha(fec):
    """Soporta fechas en formato dd/mm/yy como 19/07/25 y otros."""
    if not fec:
        return None
    if isinstance(fec, datetime):
        return fec.date()
    if isinstance(fec, str):
        fec = fec.strip().replace(".", "-").replace("/", "-").replace(" ", "-")
        formatos = [
            "%Y-%m-%d",  # 2025-07-19
            "%d-%m-%Y",  # 19-07-2025
            "%Y%m%d",    # 20250719
            "%d-%m-%y",  # 19-07-25
            "%d/%m/%y"   # 19/07/25 ✅ ahora soportado
        ]
        for fmt in formatos:
            try:
                return datetime.strptime(fec, fmt).date()
            except:
                continue
    return None

def formatear_fecha(fecha):
    """Devuelve la fecha en formato YYYY-MM-DD, o vacío si es inválida."""
    if not fecha:
        return ""
    if isinstance(fecha, (datetime, date)):
        return fecha.strftime("%Y-%m-%d")
    if isinstance(fecha, str):
        try:
            return datetime.strptime(fecha.strip(), "%Y-%m-%d").strftime("%Y-%m-%d")
        except:
            try:
                return datetime.strptime(fecha.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
            except:
                return ""
    return ""

# ================================
# ENDPOINTS
# ================================
@app.get("/")
def home():
    return {
        "mensaje": "✅ API ZQRED funcionando correctamente",
        "usar_endpoint": "/historico → Devuelve datos guardados",
        "actualizar": "/reporte → Actualiza el histórico",
        "descargar": "/descargar/historico → Descarga el archivo DBF"
    }

@app.get("/historico")
def historico_json():
    """Devuelve TODO el histórico completo, no solo las actualizaciones."""
    try:
        if not os.path.exists(HISTORICO_DBF):
            return {"total": 0, "datos": []}

        registros = []
        for r in DBF(HISTORICO_DBF, load=True, encoding="cp850"):
            fila = {k: v for k, v in r.items()}
            fila["FECHA"] = formatear_fecha(fila.get("FECHA", ""))
            registros.append(fila)

        return {"total": len(registros), "datos": registros}

    except Exception as e:
        return {"error": str(e)}

@app.get("/reporte")
def generar_reporte():
    try:
        for archivo in [ZETH50T, ZETH51T, ZETH70]:
            if not os.path.exists(archivo):
                return {"error": f"No se encontró {archivo}"}

        crear_dbf_historico()
        registros_existentes = leer_dbf_existente()

        productos = {r["PRONUM"]: r for r in DBF(ZETH70, load=True, encoding="cp850")}
        productos_ext = (
            {r["PRONUM"]: r for r in DBF(ZETH70_EXT, load=True, encoding="cp850")}
            if os.path.exists(ZETH70_EXT)
            else {}
        )
        cabeceras = {r["NUMCHK"]: r for r in DBF(ZETH50T, load=True, encoding="cp850")}

        nuevos_registros = []

        for detalle in DBF(ZETH51T, encoding="cp850"):
            numchk = str(detalle.get("NUMCHK", "")).strip()
            pronum = str(detalle.get("PRONUM", "")).strip()

            if (numchk, pronum) in registros_existentes:
                continue

            cab = cabeceras.get(numchk)
            if not cab:
                continue

            fecchk = parsear_fecha(cab.get("FECCHK"))

            nuevo = {
                "EERR": productos_ext.get(pronum, {}).get("EERR", ""),
                "FECHA": fecchk,
                "N_TICKET": numchk,
                "NOMBRES": cab.get("CUSNAM", ""),
                "TIPO": cab.get("TYPPAG", ""),
                "CANT": float(detalle.get("QTYPRO", 0)),
                "P_UNIT": float(detalle.get("PRIPRO", 0)),
                "CATEGORIA": productos_ext.get(pronum, {}).get("CATEGORIA", ""),
                "SUB_CAT": productos_ext.get(pronum, {}).get("SUB_CAT", ""),
                "COST_UNIT": obtener_costo_producto(pronum, productos),
                "PRONUM": pronum,
                "DESCRI": productos_ext.get(pronum, {}).get("DESCRI", "")
            }

            nuevos_registros.append(nuevo)

        if nuevos_registros:
            agregar_al_historico(nuevos_registros)

        total_acumulado = len(DBF(HISTORICO_DBF, load=True, encoding="cp850"))

        return {
            "nuevos_agregados": len(nuevos_registros),
            "total_historico": total_acumulado,
            "nuevos": nuevos_registros
        }

    except Exception as e:
        return {"error": str(e)}

@app.get("/descargar/historico")
def descargar_historico():
    if not os.path.exists(HISTORICO_DBF):
        return {"error": "El archivo histórico aún no existe."}
    return FileResponse(
        HISTORICO_DBF,
        media_type="application/octet-stream",
        filename=HISTORICO_DBF
    )


