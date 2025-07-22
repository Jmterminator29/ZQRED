from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from dbfread import DBF
from dbf import Table, READ_WRITE
from datetime import datetime
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ZETH50T = "ZETH50T.DBF"
ZETH51T = "ZETH51T.DBF"
ZETH70 = "ZETH70.DBF"
ZETH70_EXT = "ZETH70_EXT.DBF"
HISTORICO_DBF = "VENTAS_HISTORICO.DBF"

CAMPOS_HISTORICO = (
    "EERR C(20);"
    "FECHA C(20);"
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
    return {
        (str(r["N_TICKET"]).strip().upper(), str(r["PRONUM"]).strip().upper())
        for r in DBF(HISTORICO_DBF, load=True, encoding="cp850")
    }

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
        return float(producto.get("ULCOSREP") or 0.0)
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

def agrupar_registros_visual(registros):
    agrupados = []
    for r in registros:
        fila = dict(r)
        tipo = fila.get("TIPO", "").strip()
        fila["EERR_CONC"] = "CUENTAS x COBRAR" if tipo == "C" else "VENTAS al CONTADO"
        agrupados.append(fila)
    return agrupados

@app.get("/")
def home():
    return {
        "mensaje": "✅ API ZQRED funcionando correctamente",
        "usar_endpoint": "/historico → Devuelve datos",
        "actualizar": "/reporte → Actualiza histórico sin duplicados",
        "descargar": "/descargar/historico → Descarga el DBF"
    }

@app.get("/historico")
def historico_json():
    try:
        if not os.path.exists(HISTORICO_DBF):
            return {"total": 0, "datos": []}
        table = Table(HISTORICO_DBF, codepage="cp850")
        table.open()
        registros = []
        for rec in table:
            fila = {}
            for field in table.field_names:
                valor = rec[field]
                if isinstance(valor, str):
                    valor = valor.strip()
                fila[field] = valor or 0
            registros.append(fila)
        table.close()
        datos_agrupados = agrupar_registros_visual(registros)
        return {"total": len(datos_agrupados), "datos": datos_agrupados}
    except Exception as e:
        return {"error": str(e)}

@app.get("/reporte")
def generar_reporte():
    try:
        for archivo in [ZETH50T, ZETH51T, ZETH70]:
            if not os.path.exists(archivo):
                return {"error": f"No se encontró {archivo}"}

        crear_dbf_historico()
        productos = {r["PRONUM"]: r for r in DBF(ZETH70, load=True, encoding="cp850")}
        productos_ext = (
            {r["PRONUM"]: r for r in DBF(ZETH70_EXT, load=True, encoding="cp850")}
            if os.path.exists(ZETH70_EXT)
            else {}
        )
        cabeceras = {r["NUMCHK"]: r for r in DBF(ZETH50T, load=True, encoding="cp850")}

        nuevos_registros = []
        for detalle in DBF(ZETH51T, encoding="cp850"):
            numchk = str(detalle.get("NUMCHK") or "").strip().upper()
            pronum = str(detalle.get("PRONUM") or "").strip().upper()

            if (numchk, pronum) in leer_dbf_existente():
                print(f"⛔ YA EXISTE → Ticket: {numchk}, PRONUM: {pronum}")
                continue

            cab = cabeceras.get(numchk)
            if not cab:
                continue

            fecchk_date = parsear_fecha(cab.get("FECCHK"))
            fecchk_str = str(fecchk_date) if fecchk_date else str(cab.get("FECCHK") or "").strip()

            prod_ext = productos_ext.get(pronum, {})
            producto = productos.get(pronum, {})

            cost_unit = float(obtener_costo_producto(pronum, productos) or 0)
            cant = float(detalle.get("QTYPRO") or 0)
            p_unit = float(detalle.get("PRIPRO") or 0)

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
                "DESCRI": producto.get("DESCRI", "")
            }

            print(f"➕ AGREGADO → Ticket: {numchk}, PRONUM: {pronum}")
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



