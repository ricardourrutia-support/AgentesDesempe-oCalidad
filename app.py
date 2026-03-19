import streamlit as st
import pandas as pd
import re
from io import BytesIO
from processor import procesar_reportes

st.set_page_config(page_title="Consolidador Calidad Aeropuerto", layout="wide")
st.title("🟦 Consolidador CMI – Performance y Auditorías")

st.markdown("""
Sube los reportes operativos y pega los correos de los agentes. 
La app consolidará las métricas de **Performance (Tickets)** y **Calidad (Auditorías)** exclusivamente para esos ejecutivos, **respetando el orden** en que los ingreses.
""")

col1, col2 = st.columns(2)

with col1:
    st.header("📤 Cargar Archivos")
    performance_file = st.file_uploader("Reporte de Performance (.csv)", type=["csv"])
    auditorias_file = st.file_uploader("Reporte Auditorías (.csv)", type=["csv"])
    
    st.header("📅 Rango de Fechas")
    colf1, colf2 = st.columns(2)
    date_from = colf1.date_input("Desde:")
    date_to = colf2.date_input("Hasta:")
    if date_from > date_to:
        st.error("❌ La fecha inicial no puede ser mayor que la final.")
        st.stop()

with col2:
    st.header("📧 Correos de Agentes")
    correos_input = st.text_area(
        "Pega aquí los correos corporativos de los agentes a evaluar (el reporte respetará este orden):",
        height=250,
        placeholder="ejemplo1@cabify.com\nejemplo2@cabify.com\nejemplo3@cabify.com"
    )

st.divider()

if st.button("🔄 Procesar Reportes"):
    if not performance_file or not auditorias_file:
        st.error("❌ Debes cargar ambos reportes (Performance y Auditorías).")
        st.stop()
        
    # --- EXTRAER CORREOS DE LA CAJA DE TEXTO ---
    # Usamos expresiones regulares para detectar cualquier correo sin importar cómo lo peguen
    raw_emails = re.split(r'[,\s\n]+', correos_input.strip())
    lista_correos = [e.lower().strip() for e in raw_emails if "@" in e]
    
    if not lista_correos:
        st.warning("⚠️ No pegaste ningún correo en la caja. Se procesarán todos los agentes encontrados en los reportes (sin orden específico).")

    # === LECTURA DE ARCHIVOS ===
    try:
        performance_file.seek(0)
        df_performance = pd.read_csv(performance_file, sep=";", encoding="utf-8-sig")
        if len(df_performance.columns) < 5: raise ValueError()
    except:
        performance_file.seek(0)
        df_performance = pd.read_csv(performance_file, sep=",", encoding="utf-8-sig")

    try:
        auditorias_file.seek(0)
        df_auditorias = pd.read_csv(auditorias_file, sep=";", encoding="utf-8-sig", engine="python")
        if len(df_auditorias.columns) < 2: raise ValueError()
    except:
        auditorias_file.seek(0)
        df_auditorias = pd.read_csv(auditorias_file, sep=",", encoding="utf-8-sig", engine="python")

    # === PROCESAMIENTO ===
    try:
        resultados = procesar_reportes(df_performance, df_auditorias, lista_correos, date_from, date_to)
    except Exception as e:
        st.error(f"❌ ERROR DURANTE EL CRUCE: {e}")
        st.stop()

    df_diario = resultados["diario"]
    df_semanal = resultados["semanal"]
    df_resumen = resultados["resumen"]

    if df_resumen.empty:
        st.warning("⚠️ Las tablas están vacías. Verifica que los correos que pegaste coincidan con los de los reportes, o que el rango de fechas sea correcto.")
        st.stop()

    st.success("✔ Reportes procesados correctamente.")

    # === MOSTRAR TABLAS ===
    st.header("📊 Resumen Total Agentes")
    st.dataframe(df_resumen, use_container_width=True)
    
    st.header("📆 Reporte Semanal")
    st.dataframe(df_semanal, use_container_width=True)
    
    st.header("📅 Reporte Diario")
    st.dataframe(df_diario, use_container_width=True)

    # === DESCARGA ===
    st.header("📥 Descargar Excel Consolidado")
    def to_excel(diario, semanal, resumen):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        resumen.to_excel(writer, sheet_name="Resumen Agentes", index=False)
        semanal.to_excel(writer, sheet_name="Semanal", index=False)
        diario.to_excel(writer, sheet_name="Diario", index=False)
        writer.close()
        return output.getvalue()

    excel_bytes = to_excel(df_diario, df_semanal, df_resumen)
    st.download_button(
        "⬇ Descargar Excel Consolidado", 
        data=excel_bytes, 
        file_name="CMI_Calidad_Agentes.xlsx", 
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.info("Sube los archivos, selecciona rango de fechas y presiona **Procesar Reportes**.")
