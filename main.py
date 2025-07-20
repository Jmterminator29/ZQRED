from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from dbfread import DBF
from dbf import Table, READ_WRITE
from datetime import datetime
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

# ✅ FECHA COMO TEXTO PARA EVITAR NULL
CAMPOS_HISTORICO = (
    "EERR C(20);"
    "FECHA C(20);"  # <-- TEXTO, NUNCA NULL
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
    if not fec:
        return None
    if isinstance(fec, datetime):
        return fec.date()
    if isinstance(fec, str):
        fec = fec.strip().replace(".", "-").replace("/", "-").replace(" ", "-")
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d-%m-%y", "%d/%m/%y", "%Y%m%d"):
            try:
                return datetime.strptime(fec, fmt).date()
            except:
                continue
    return None

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
    if not os.path.exists(HISTORICO_DBF):
        return {"total": 0, "datos": []}
    datos = list(DBF(HISTORICO_DBF, load=True, encoding="cp850"))
    return {"total": len(datos), "datos": datos}

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

            # ✅ FECHA SIEMPRE COMO TEXTO
            fecchk_date = parsear_fecha(cab.get("FECCHK"))
            fecchk_str = str(fecchk_date) if fecchk_date else str(cab.get("FECCHK", "")).strip()

            prod_ext = productos_ext.get(pronum, {})
            cost_unit = obtener_costo_producto(pronum, productos)

            cant = float(detalle.get("QTYPRO", 0))
            p_unit = float(detalle.get("PRIPRO", 0))

            nuevo = {
                "EERR": prod_ext.get("EERR", ""),
                "FECHA": fecchk_str,
                "N_TICKET": numchk,
                "NOMBRES": cab.get("CUSNAM", ""),
                "TIPO": cab.get("TYPPAG", ""),
                "CANT": cant,
                "P_UNIT": p_unit,
                "CATEGORIA": prod_ext.get("CATEGORIA", ""),
                "SUB_CAT": prod_ext.get("SUB_CAT", ""),
                "COST_UNIT": cost_unit,
                "PRONUM": pronum,
                "DESCRI": prod_ext.get("DESCRI", "")
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
