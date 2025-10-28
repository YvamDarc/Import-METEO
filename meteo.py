import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px

DATASET_ID = "donnees-synop-essentielles-omm"
BASE_CATALOG_URL = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/{DATASET_ID}"
BASE_RECORDS_URL = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/{DATASET_ID}/records"

STATIONS = {
    "07110": "BREST",
    "07630": "PARIS-MONTSOURIS",
    "07761": "AJACCIO",
}

st.set_page_config(
    page_title="Météo live SYNOP",
    page_icon="🌦️",
    layout="wide",
)

st.title("🌦️ Météo live (SYNOP / Opendatasoft)")
st.caption(
    "On tente d'interroger le schéma réel du dataset + les mesures météo d'une station. "
    "Si ça ne remonte rien, on affiche tout ce qu'on sait en debug."
)

# -------------------------------------------------
# 1. FONCTIONS UTILITAIRES
# -------------------------------------------------

@st.cache_data(ttl=3600)
def raw_get_schema_json():
    """
    On ne fait plus d'hypothèse sur la structure.
    On renvoie le JSON brut de /catalog/datasets?include_schema=true
    + on laisse l'appelant se débrouiller.
    """
    params = {"include_schema": "true"}
    r = requests.get(BASE_CATALOG_URL, params=params, timeout=30)
    return {
        "status_code": r.status_code,
        "url": r.url,
        "text": r.text[:1000],  # on tronque pour éviter le pâté énorme
        "json": (r.json() if r.headers.get("Content-Type", "").startswith("application/json") else None),
    }

def extract_fields_df(schema_payload):
    """
    Essaie d'extraire la liste des champs sous forme de DataFrame,
    quel que soit le format retourné.
    On est tolérant : si on ne trouve pas, on renvoie un DF vide.
    """
    if not schema_payload:
        return pd.DataFrame()

    js = schema_payload.get("json")
    if not isinstance(js, dict):
        return pd.DataFrame()

    # On essaie différentes clés possibles :
    # 1) structure attendue: {"dataset": {"fields": [ {...}, {...} ]}}
    if "dataset" in js and isinstance(js["dataset"], dict):
        maybe_fields = js["dataset"].get("fields")
        if isinstance(maybe_fields, list):
            return pd.DataFrame(maybe_fields)

    # 2) parfois c'est renvoyé direct: {"fields": [...]}
    if "fields" in js and isinstance(js["fields"], list):
        return pd.DataFrame(js["fields"])

    # 3) fallback : rien trouvé
    return pd.DataFrame()

def guess_columns(fields_df: pd.DataFrame):
    """
    Heuristique pour repérer les colonnes utiles.
    Si on ne trouve rien, on renvoie juste {}
    """
    if fields_df.empty or "name" not in fields_df.columns:
        return {}

    def find_field(substr_list, must_all=False):
        for name in fields_df["name"]:
            low = str(name).lower()
            if must_all:
                if all(sub in low for sub in substr_list):
                    return name
            else:
                if any(sub in low for sub in substr_list):
                    return name
        return None

    date_col = find_field(["date", "time", "datetime"])
    station_id_col = (
        find_field(["omm", "station", "id"], must_all=True)
        or find_field(["omm", "station"])
        or find_field(["station", "id"])
        or find_field(["omm"])
    )
    station_name_col = find_field(["station", "name"]) or find_field(["station"])
    temp_col = find_field(["temp"]) or find_field(["temperat"])
    rain_col = find_field(["rain", "pluie", "precip"])
    wind_col = find_field(["wind", "vent", "ff"])

    if station_id_col == station_name_col:
        refine = find_field(["omm"]) or find_field(["id"])
        if refine:
            station_id_col = refine

    return {
        "date": date_col,
        "station_id": station_id_col,
        "station_name": station_name_col,
        "temp": temp_col,
        "rain": rain_col,
        "wind": wind_col,
    }

def fetch_data_for_station(cols_map, station_id, start_dt, end_dt, limit):
    """
    Appel /records pour récupérer les lignes météo.
    On construit la requête avec les noms de colonnes trouvés.
    Si cols_map est incomplet -> on renvoie DataFrame vide + message.
    """
    if not cols_map:
        st.error("❌ Pas de mapping de colonnes (le schéma est vide).")
        return pd.DataFrame()

    date_col_api = cols_map.get("date")
    id_col_api = cols_map.get("station_id")

    if not date_col_api or not id_col_api:
        st.error(f"❌ Colonnes critiques manquantes. date={date_col_api}, station_id={id_col_api}")
        return pd.DataFrame()

    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # where dynamique
    where_clause = (
        f"`{id_col_api}` = '{station_id}' "
        f"AND `{date_col_api}` >= '{start_iso}' "
        f"AND `{date_col_api}` <= '{end_iso}'"
    )

    select_parts = [
        f"`{date_col_api}` as date_utc",
        f"`{id_col_api}` as station_id",
    ]

    if cols_map.get("station_name"):
        select_parts.append(f"`{cols_map['station_name']}` as station_name")
    if cols_map.get("temp"):
        select_parts.append(f"`{cols_map['temp']}` as temperature_raw")
    if cols_map.get("rain"):
        select_parts.append(f"`{cols_map['rain']}` as rain_raw")
    if cols_map.get("wind"):
        select_parts.append(f"`{cols_map['wind']}` as wind_raw")

    params = {
        "where": where_clause,
        "limit": limit,
        "order_by": f"`{date_col_api}` ASC",
        "select": ", ".join(select_parts),
    }

    r = requests.get(BASE_RECORDS_URL, params=params, timeout=30)

    # On remonte TOUJOURS le statut et la réponse, même si 200,
    # pour qu'on voie enfin ce que l'API retourne réellement
    st.write("🛰️ DEBUG /records status_code:", r.status_code)
    st.write("🛰️ DEBUG URL appelée:", r.url)
    st.write("🛰️ DEBUG réponse (début):", r.text[:500])

    if r.status_code != 200:
        return pd.DataFrame()

    try:
        data_json = r.json()
    except Exception:
        return pd.DataFrame()

    results = data_json.get("results", [])
    return pd.DataFrame(results)

def normalize_synop_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme la réponse brute en colonnes prêtes à afficher.
    """
    if df.empty:
        return df

    out = pd.DataFrame()

    if "date_utc" in df.columns:
        out["date_utc"] = pd.to_datetime(df["date_utc"], errors="coerce", utc=True)
        out["date_local"] = out["date_utc"].dt.tz_convert("Europe/Paris")
    else:
        out["date_utc"] = pd.NaT
        out["date_local"] = pd.NaT

    out["station_id"] = df.get("station_id")
    out["station_name"] = df.get("station_name")

    if "temperature_raw" in df.columns:
        t_raw = pd.to_numeric(df["temperature_raw"], errors="coerce")
        out["temperature_C"] = t_raw.where(t_raw < 200, t_raw - 273.15)
    else:
        out["temperature_C"] = None

    if "rain_raw" in df.columns:
        out["pluie_brute"] = pd.to_numeric(df["rain_raw"], errors="coerce")
    else:
        out["pluie_brute"] = None

    if "wind_raw" in df.columns:
        out["vent_brut"] = pd.to_numeric(df["wind_raw"], errors="coerce")
    else:
        out["vent_brut"] = None

    out = out.sort_values("date_local").reset_index(drop=True)
    return out


# -------------------------------------------------
# 2. CHARGER LE SCHÉMA + AFFICHER DEBUG
# -------------------------------------------------

schema_payload = raw_get_schema_json()
schema_df = extract_fields_df(schema_payload)
cols_map = guess_columns(schema_df)

with st.expander("🔎 Debug schéma / colonnes"):
    st.write("Réponse brute du schéma (/catalog … include_schema=true) :")
    st.json(schema_payload)

    st.write("DataFrame des champs détectés :")
    if schema_df.empty:
        st.warning("⚠ Pas de champs détectés (schema_df est vide).")
    else:
        show_cols = [c for c in ["name", "type", "label", "description"] if c in schema_df.columns]
        st.dataframe(schema_df[show_cols], use_container_width=True)

    st.write("Mapping heuristique actuel :")
    st.json(cols_map)


# -------------------------------------------------
# 3. SIDEBAR (PARAMÈTRES UTILISATEUR)
# -------------------------------------------------

with st.sidebar:
    st.header("⚙️ Paramètres")

    station_codes = list(STATIONS.keys())
    station_labels = [f"{STATIONS[c]} ({c})" for c in station_codes]

    station_idx = st.selectbox(
        "Station météo",
        options=range(len(station_codes)),
        format_func=lambda i: station_labels[i],
    )

    chosen_station_id = station_codes[station_idx]
    chosen_station_name = STATIONS[chosen_station_id]

    st.write(f"ID (station OMM supposé) : `{chosen_station_id}`")
    st.write(f"Nom affiché : {chosen_station_name}")

    default_end = datetime.utcnow()
    default_start = default_end - timedelta(days=2)

    start_date = st.date_input("Date début (UTC)", default_start.date())
    end_date = st.date_input("Date fin (UTC)", default_end.date())

    start_hour = st.number_input("Heure début (0-23)", min_value=0, max_value=23, value=0)
    end_hour = st.number_input("Heure fin (0-23)", min_value=0, max_value=23, value=23)

    limit = st.slider(
        "Nombre max de lignes (<=100)",
        min_value=10,
        max_value=100,
        value=50,
        step=10,
    )

    run_query = st.button("🔍 Charger les données")


# -------------------------------------------------
# 4. APPEL MESURES + PLOTS
# -------------------------------------------------

if run_query:
    start_dt = datetime(
        year=start_date.year,
        month=start_date.month,
        day=start_date.day,
        hour=int(start_hour),
        minute=0,
        second=0,
    )
    end_dt = datetime(
        year=end_date.year,
        month=end_date.month,
        day=end_date.day,
        hour=int(end_hour),
        minute=0,
        second=0,
    )

    with st.spinner("Appel API mesures /records..."):
        raw_df = fetch_data_for_station(cols_map, chosen_station_id, start_dt, end_dt, limit)
        synop_df = normalize_synop_df(raw_df)

    if synop_df.empty:
        st.warning("Aucune donnée renvoyée (ou mapping pas encore bon). Regarde le debug juste au-dessus.")
    else:
        st.subheader("Aperçu des données normalisées")
        st.dataframe(synop_df.tail(20), use_container_width=True)

        if synop_df["temperature_C"].notna().any():
            fig_temp = px.line(
                synop_df,
                x="date_local",
                y="temperature_C",
                title="Température (°C estimée)",
                markers=True,
            )
            fig_temp.update_layout(
                xaxis_title="Heure (Europe/Paris)",
                yaxis_title="°C",
            )
            st.plotly_chart(fig_temp, use_container_width=True)

        if synop_df["pluie_brute"].notna().any():
            fig_rain = px.bar(
                synop_df,
                x="date_local",
                y="pluie_brute",
                title="Pluie (valeur brute API)",
            )
            fig_rain.update_layout(
                xaxis_title="Heure (Europe/Paris)",
                yaxis_title="Pluie (unité API)",
            )
            st.plotly_chart(fig_rain, use_container_width=True)

        if synop_df["vent_brut"].notna().any():
            fig_wind = px.line(
                synop_df,
                x="date_local",
                y="vent_brut",
                title="Vent moyen (valeur brute API)",
                markers=True,
            )
            fig_wind.update_layout(
                xaxis_title="Heure (Europe/Paris)",
                yaxis_title="Vent (unité API)",
            )
            st.plotly_chart(fig_wind, use_container_width=True)

else:
    st.info("➡ Choisis une station, une période et clique sur 'Charger les données'.")
