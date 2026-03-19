import streamlit as st
import pandas as pd
from io import BytesIO
from processor import procesar_reportes

# ------------------------------------------------------------
# CONFIGURACIÓN
# ------------------------------------------------------------
st.set_page_config(page_title="Consolidador CMI Aeropuerto", layout="wide")
st.title("🟦 Consolidador CMI – Aeropuerto Cabify")

st.markdown("""
Sube los reportes correspondientes, selecciona el rango de fechas
y la app consolidará **Ventas**, **Performance** y **Auditorías**, incluyendo:

- Reporte Diario  
- Reporte Semanal  
- Resumen Total por Agente  
- **Resumen por Coordinador / Supervisor (NUEVO)**
- Cruce con planilla maestra de trabajadores
""")

# ------------------------------------------------------------
# SUBIDA DE ARCHIVOS
# ------------------------------------------------------------
st.header("📤 Cargar Archivos")

col1, col2 = st.columns(2)

with col1:
    ventas_file = st.file_uploader("Reporte de Ventas (.xlsx)", type=["xlsx"])
    performance_file = st.file_uploader("Reporte de Performance (.csv)", type=["csv"])

with col2:
    auditorias_file = st.file_uploader("Reporte Auditorías (.csv ;)", type=["csv"])
    agentes_file = st.file_uploader("Planilla de Trabajadores (.csv o .xlsx)", type=["csv", "xlsx", "xls"])

st.divider()

# ------------------------------------------------------------
# RANGO DE FECHAS
# ------------------------------------------------------------
st.header("📅 Seleccionar Rango de Fechas")

colf1, colf2 = st.columns(2)
date_from = colf1.date_input("Desde:")
date_to = colf2.date_input("Hasta:")

if date_from > date_to:
    st.error("❌ La fecha inicial no puede ser mayor que la final.")
    st.stop()

st.divider()

# ------------------------------------------------------------
# BOTÓN DE PROCESAR
# ------------------------------------------------------------
if st.button("🔄 Procesar Reportes"):

    if not ventas_file or not performance_file or not auditorias_file or not agentes_file:
        st.error("❌ Debes cargar los 4 archivos para continuar.")
        st.stop()

    # === LECTURA DINÁMICA DE ARCHIVOS ===
    
    # 1. LEER VENTAS
    try:
        ventas_file.seek(0)
        df_ventas = pd.read_excel(ventas_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Ventas: {e}")
        st.stop()

    # 2. LEER PERFORMANCE (A prueba de balas ; y ,)
    try:
        performance_file.seek(0)
        df_performance = pd.read_csv(performance_file, sep=";", encoding="utf-8-sig")
        if len(df_performance.columns) < 5:  # Si lee todo en 1 columna, forzar el except
            raise ValueError("Separador incorrecto")
    except:
        try: 
            performance_file.seek(0)
            df_performance = pd.read_csv(performance_file, sep=",", encoding="utf-8-sig")
        except Exception as e:
            st.error(f"❌ Error leyendo Performance: {e}")
            st.stop()

    # 3. LEER AUDITORÍAS (A prueba de balas ; y ,)
    try:
        auditorias_file.seek(0)
        df_auditorias = pd.read_csv(auditorias_file, sep=";", encoding="utf-8-sig", engine="python")
        if len(df_auditorias.columns) < 2:
            raise ValueError("Separador incorrecto")
    except:
        try:
            auditorias_file.seek(0)
            df_auditorias = pd.read_csv(auditorias_file, sep=",", encoding="utf-8-sig", engine="python")
        except Exception as e:
            st.error(f"❌ Error leyendo Auditorías: {e}")
            st.stop()

    # 4. LEER AGENTES (A prueba de balas ; , o Excel)
    try:
        agentes_file.seek(0)
        if agentes_file.name.endswith(('.csv', '.txt')):
            try:
                df_agentes = pd.read_csv(agentes_file, sep=";", encoding="utf-8-sig", engine="python")
                if len(df_agentes.columns) < 2:
                    raise ValueError("Separador incorrecto")
            except:
                agentes_file.seek(0)
                df_agentes = pd.read_csv(agentes_file, sep=",", encoding="utf-8-sig", engine="python")
        else:
            df_agentes = pd.read_excel(agentes_file)
    except Exception as e:
        st.error(f"❌ Error leyendo Planilla de Trabajadores: {e}")
        st.stop()

    # =====================================================
    # PROCESAR TODO
    # =====================================================
    try:
        resultados = procesar_reportes(
            df_ventas,
            df_performance,
            df_auditorias,
            df_agentes,
            date_from,
            date_to
        )
    except Exception as e:
        st.error(f"❌ Error interno al procesar datos en processor.py: {e}")
        st.stop()

    df_diario = resultados["diario"]
    df_semanal = resultados["semanal"]
    df_resumen = resultados["resumen"]
    df_coordinadores = resultados["coordinadores"]

    st.success("✔ Reportes procesados correctamente.")

    # ----------------------------------------------------
    # MOSTRAR RESULTADOS
    # ----------------------------------------------------
    st.header("👥 Resumen Desempeño por Coordinador")
    st.dataframe(df_coordinadores, use_container_width=True)
    
    st.header("📊 Resumen Total Agentes")
    st.dataframe(df_resumen, use_container_width=True)

    st.header("📆 Reporte Semanal")
    st.dataframe(df_semanal, use_container_width=True)

    st.header("📅 Reporte Diario")
    st.dataframe(df_diario, use_container_width=True)

    # ----------------------------------------------------
    # DESCARGA
    # ----------------------------------------------------
    st.header("📥 Descargar Excel Consolidado")

    def to_excel(diario, semanal, resumen, coord):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        coord.to_excel(writer, sheet_name="Resumen Supervisores", index=False)
        resumen.to_excel(writer, sheet_name="Resumen Agentes", index=False)
        semanal.to_excel(writer, sheet_name="Semanal", index=False)
        diario.to_excel(writer, sheet_name="Diario", index=False)
        writer.close()
        return output.getvalue()

    excel_bytes = to_excel(df_diario, df_semanal, df_resumen, df_coordinadores)

    st.download_button(
        "⬇ Descargar Excel Consolidado",
        data=excel_bytes,
        file_name="CMI_Aeropuerto_Consolidado_V2.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("Sube los archivos, selecciona rango de fechas y presiona **Procesar Reportes**.")
