import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def to_date(x):
    if pd.isna(x): return None
    s = str(x).strip()
    if isinstance(x, (int, float)) and x > 30000:
        try: return (datetime(1899, 12, 30) + timedelta(days=float(x))).date()
        except: pass
    if "/" in s and len(s.split("/")[0]) == 4:
        try: return datetime.strptime(s, "%Y/%m/%d").date()
        except: pass
    if "-" in s and len(s.split("-")[2]) == 4 and len(s.split("-")[0]) <= 2:
        try: return datetime.strptime(s, "%d-%m-%Y").date()
        except: pass
    if "/" in s and len(s.split("/")[2]) == 4:
        try: return datetime.strptime(s, "%m/%d/%Y").date()
        except: pass
    try: return pd.to_datetime(s).date()
    except: return None

def normalize_headers(df):
    df.columns = df.columns.astype(str).str.replace("﻿", "").str.replace("\ufeff", "").str.strip()
    return df

def filtrar_rango(df, col, d_from, d_to):
    if col not in df.columns: return df
    df[col] = df[col].apply(to_date)
    df = df[df[col].notna()]
    df = df[(df[col] >= d_from) & (df[col] <= d_to)]
    return df

def process_performance(df, d_from, d_to):
    if df is None or df.empty: return pd.DataFrame()
    df = normalize_headers(df.copy())
    if "Group Support Service" not in df.columns: return pd.DataFrame()
    df = df[df["Group Support Service"] == "C_Ops Support"]
    if "Fecha de Referencia" not in df.columns: raise KeyError("Falta 'Fecha de Referencia' en Performance")
    
    df["fecha"] = df["Fecha de Referencia"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)
    
    if "Assignee Email" not in df.columns: raise KeyError("Falta 'Assignee Email' en Performance")
    df["Correo Corporativo"] = df["Assignee Email"].astype(str).str.lower().str.strip()

    df["Q_Encuestas"] = df.apply(lambda x: 1 if (not pd.isna(x.get("CSAT")) or not pd.isna(x.get("NPS Score"))) else 0, axis=1)
    df["Q_Tickets"] = 1
    
    # Soporte para Solved y Closed
    df["Q_Tickets_Resueltos"] = df["Status"].apply(
        lambda x: 1 if str(x).strip().lower() in ["solved", "closed"] else 0
    )
    
    df["Q_Reopen"] = pd.to_numeric(df.get("Reopen", 0), errors="coerce").fillna(0)

    for c in ["CSAT", "NPS Score", "Firt (h)", "Furt (h)", "% Firt", "% Furt"]: 
        df[c] = pd.to_numeric(df.get(c, np.nan), errors="coerce")

    out = df.groupby(["Correo Corporativo", "fecha"], as_index=False).agg({"Q_Encuestas": "sum", "CSAT": "mean", "NPS Score": "mean", "Firt (h)": "mean", "% Firt": "mean", "Furt (h)": "mean", "% Furt": "mean", "Q_Reopen": "sum", "Q_Tickets": "sum", "Q_Tickets_Resueltos": "sum"})
    return out.rename(columns={"NPS Score": "NPS", "Firt (h)": "FIRT", "% Firt": "%FIRT", "Furt (h)": "FURT", "% Furt": "%FURT"})

def process_auditorias(df, d_from, d_to):
    if df is None or df.empty: return pd.DataFrame()
    df = normalize_headers(df.copy())
    if "Date Time" not in df.columns: raise KeyError("Falta 'Date Time' en Auditorías")
    df["fecha"] = df["Date Time"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)
    
    if "Audited Agent" not in df.columns: raise KeyError("Falta 'Audited Agent' en Auditorías")
    df = df[df["Audited Agent"].astype(str).str.contains("@")]
    df["Correo Corporativo"] = df["Audited Agent"].astype(str).str.lower().str.strip()
    df["Q_Auditorias"] = 1
    df["Nota_Auditorias"] = pd.to_numeric(df.get("Total Audit Score", np.nan), errors="coerce")

    out = df.groupby(["Correo Corporativo", "fecha"], as_index=False).agg({"Q_Auditorias": "sum", "Nota_Auditorias": "mean"})
    if out.empty: return pd.DataFrame(columns=["Correo Corporativo", "fecha", "Q_Auditorias", "Nota_Auditorias"])
    out["Nota_Auditorias"] = out["Nota_Auditorias"].fillna(0)
    return out

# === SISTEMA DE ORDENAMIENTO A PRUEBA DE BALAS ===
def aplicar_orden(df, lista_correos):
    if len(lista_correos) > 0 and not df.empty:
        # Quitamos duplicados manteniendo el orden
        lista_unica = list(dict.fromkeys(lista_correos))
        orden_dict = {correo: index for index, correo in enumerate(lista_unica)}
        
        # Filtramos para asegurarnos de que solo queden los de la lista
        df = df[df["Correo Corporativo"].isin(lista_unica)].copy()
        
        # Creamos una columna temporal secreta con el número del orden
        df["_orden_secreto"] = df["Correo Corporativo"].map(orden_dict)
        
        # Ordenamos la tabla usando ese número secreto
        if "fecha" in df.columns:
            df = df.sort_values(["_orden_secreto", "fecha"])
        elif "Semana" in df.columns:
            df = df.sort_values(["_orden_secreto", "Semana"])
        else:
            df = df.sort_values(["_orden_secreto"])
            
        # Borramos la columna secreta y reiniciamos el índice de Pandas (evita el error que te salió)
        df = df.drop(columns=["_orden_secreto"]).reset_index(drop=True)
        
    return df

def build_daily(df_list, lista_correos):
    merged = None
    for df in df_list:
        if df is not None and not df.empty:
            merged = df if merged is None else merged.merge(df, on=["Correo Corporativo", "fecha"], how="outer")

    if merged is None or merged.empty: return pd.DataFrame()

    for c in ["Q_Encuestas", "Q_Tickets", "Q_Tickets_Resueltos", "Q_Reopen", "Q_Auditorias"]:
        if c in merged.columns: merged[c] = merged[c].fillna(0).astype(int)

    for c in ["NPS", "CSAT", "FIRT", "%FIRT", "FURT", "%FURT", "Nota_Auditorias"]:
        if c in merged.columns: merged[c] = merged[c].round(2)

    # Aplicamos el filtro y el orden
    merged = aplicar_orden(merged, lista_correos)
    if merged.empty: return pd.DataFrame()

    order = ["fecha", "Correo Corporativo"] + [c for c in merged.columns if c not in ["fecha", "Correo Corporativo"]]
    return merged[order]

def build_weekly(df_daily, lista_correos):
    if df_daily.empty: return pd.DataFrame()
    df = df_daily.copy()
    meses = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}
    fecha_min = df["fecha"].min()
    inicio_sem = fecha_min - timedelta(days=fecha_min.weekday())

    def nombre_semana(fecha):
        delta = (fecha - inicio_sem).days
        s = delta // 7
        ini = inicio_sem + timedelta(days=s*7)
        fin = ini + timedelta(days=6)
        return f"Semana {ini.day} al {fin.day} de {meses[fin.month]}"

    df["Semana"] = df["fecha"].apply(nombre_semana)
    agg = {"Q_Encuestas":"sum", "NPS":"mean", "CSAT":"mean", "FIRT":"mean", "%FIRT":"mean", "FURT":"mean", "%FURT":"mean", "Q_Reopen":"sum", "Q_Tickets":"sum", "Q_Tickets_Resueltos":"sum", "Q_Auditorias":"sum", "Nota_Auditorias":"mean"}
    
    cols_to_agg = {k: v for k, v in agg.items() if k in df.columns}
    weekly = df.groupby(["Correo Corporativo","Semana"], as_index=False).agg(cols_to_agg)

    for c in ["NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"]:
        if c in weekly.columns: weekly[c] = weekly[c].round(2)

    # Aplicamos el filtro y el orden
    weekly = aplicar_orden(weekly, lista_correos)
    if weekly.empty: return pd.DataFrame()

    order = ["Semana", "Correo Corporativo"] + [c for c in weekly.columns if c not in ["Semana", "Correo Corporativo"]]
    return weekly[order]

def build_summary(df_daily, lista_correos):
    if df_daily.empty: return pd.DataFrame()
    agg = {"Q_Encuestas":"sum", "NPS":"mean", "CSAT":"mean", "FIRT":"mean", "%FIRT":"mean", "FURT":"mean", "%FURT":"mean", "Q_Reopen":"sum", "Q_Tickets_Resueltos":"sum", "Q_Auditorias":"sum", "Nota_Auditorias":"mean"}
    cols_to_agg = {k: v for k, v in agg.items() if k in df_daily.columns}
    resumen = df_daily.groupby("Correo Corporativo", as_index=False).agg(cols_to_agg)

    for c in ["NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"]:
        if c in resumen.columns: resumen[c] = resumen[c].round(2)

    # Aplicamos el filtro y el orden
    resumen = aplicar_orden(resumen, lista_correos)
    if resumen.empty: return pd.DataFrame()

    order = ["Correo Corporativo"] + [c for c in resumen.columns if c != "Correo Corporativo"]
    return resumen[order]

def procesar_reportes(df_performance, df_auditorias, lista_correos, d_from, d_to):
    perf = process_performance(df_performance, d_from, d_to)
    auds = process_auditorias(df_auditorias, d_from, d_to)

    diario = build_daily([perf, auds], lista_correos)
    semanal = build_weekly(diario, lista_correos)
    resumen = build_summary(diario, lista_correos)

    return {"diario": diario, "semanal": semanal, "resumen": resumen}
