import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px

# ---------------------------------
# CONFIG DE BASE
# ---------------------------------

DATASET_ID = "donnees-synop-essentielles-omm"
BASE_CATALOG_URL = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/{DATASET_ID}"
BASE_RECORDS_URL = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/{DATASET_ID}/records"

# Stations à tester manuellement (code OMM supposé → label humain)
STATIONS = {
    "07110": "BREST",
    "07630": "PARIS-MONTSOURIS",
    "07761": "AJACCIO",
}


# ---------------------------------
# 1. MÉTADONNÉES DU DATASET
# ---------------------------------

@st.cache_data(ttl=3600)
def get_schema():
    """
    Récupère les métadonnées complètes du dataset, y compris ses 'fields'.
    On utilise include_schema=true parce que /fields n'existe pas sur ce domaine.
    Retourne un DataFrame des champs techniques (name, type, label).
    """
    params = {
        "include_schema": "true",
    }
    r = requests.get(BASE_CATALOG_URL, params=params, timeout=30)

    if r.status_code != 200:
        st.error(
            f"❌ Erreur récupération schéma (HTTP {r.status_code}).\n"
            f"URL: {r.url}\nRéponse: {r.text[:500]}"
        )
        return pd.DataFrame()

    catalog_json = r.json()

    # Sur Opendatasoft explore v2.1, la réponse du /catalog/datasets/<id>
    # contient typiquement { "dataset": {... "fields": [ {...}, {...} ] } }
    dataset_info = catalog_json.get("dataset", {})
    fields = dataset_info.get("fields", [])

    df_fields = pd.DataFrame(fields)
    # df_fields devrait avoir au moins: name (nom technique), type, label (nom lisible)
    return df_fields


def guess_columns(fields_df: pd.DataFrame):
    """
    À partir de la liste des champs techniques renvoyés par le schéma,
    on essaie de deviner quelles colonnes correspondent à quoi.

    On cherche :
    - date_col : horodatage
    - station_id_col : identifiant station OMM
    - station_name_col : nom humain de la station
    - temp_col : température
    - rain_col : pluie
    - wind_col : vent moyen

    Heuristiques textuelles : on matche sur .lower()
    """
    if fields_df.empty:
        return {}

    def find_field(candidates_substrings, must_all=False):
        for _, row in fields_df.iterrows():
            fname = str(row.get("name", "")).lower()
            if must_all:
                if all(sub in fname for sub in candidates_substrings):
                    return row.get("name")
            else:
                if any(sub in fname for sub in candidates_substrings):
                    return row.get("name")
        return None

    # champs temps
    date_col = find_field(["date", "time", "datetime"])

    # ID station : souvent contient "omm" ou "station" ou "id"
    station_id_col = (
        find_field(["omm", "station", "id"], must_all=True)
        or find_field(["omm", "station"])
        or find_field(["station", "id"])
        or find_field(["omm"])
    )

    # nom station : souvent juste "station", "name"
    station_name_col = find_field(["station", "name"]) or find_field(["station"])

    # température
    temp_col = find_field(["temp"]) or find_field(["temperat"])

    # pluie
    rain_col = (
        find_field(["rain"])
        or find_field(["pluie"])
        or find_field(["precip"])
    )

    # vent
    wind_col = (
        find_field(["wind"])
        or find_field(["vent"])
        or find_field(["ff"])  # ff = vent moyen en m/s sur les SYNOP
    )

    # Nettoyage : si station_id_col == station_name_col, essaie de raffiner
    if station_id_col == station_name_col:
        # on cherche explicitement "id" ou "omm"
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


# ---------------------------------
# 2. RÉCUPÉRATION DES MESURES
# ---------------------------------

def fetch_data_for_station(cols_map, station_id, start_dt, end_dt, limit):
    """
    Récupère les enregistrements météo pour une station et une période.
    Utilise les noms techniques détectés dans cols_map.
    """
    # sécurité minimale
    if not cols_map.get("date") or not cols_map.get("station_id"):
        st.error("❌ Pas de colonne 'date' ou 'station_id' détectée dans le schéma.")
        return pd.DataFrame()

    date_col_api = cols_map["date"]
    id_col_api = cols_map["station_id"]

    # horodatages ISO8601 en UTC avec 'Z'
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # On backtick systématiquement les noms de champs car certains peuvent contenir des espaces.
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
        "limit": limit,  # max 100
        "order_by": f"`{date_col_api}` ASC",
        "select": ", ".join(select_parts),
    }

    r = requests.get(BASE_RECORDS_URL, params=params, timeout=30)

    if r.status_code != 200:
        st.error(
            f"❌ Erreur API mesures (HTTP {r.status_code}).\n"
            f"URL: {r.url}\n"
            f"Réponse: {r.text[:500]}"
        )
        return pd.DataFrame()

    try:
        results = r.json().get("results", [])
    except Exception as e:
        st.error(f"❌ Réponse JSON illisible. Détail: {e}")
        return pd.DataFrame()

    return pd.DataFrame(results)


def normalize_synop_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    - parse date_utc -> datetime TZ-aware
    - convertit en Europe/Paris
    - convertit temperature_raw en °C si ça ressemble à du Kelvin
    - garde pluie et vent bruts pour visualisation
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

    # Température
    if "temperature_raw" in df.columns:
        t_raw = pd.to_numeric(df["temperature_raw"], errors="coerce")
        # heuristique Kelvin -> °C
        out["temperature_C"] = t_raw.where(t_raw < 200, t_raw - 273.15)
    else:
        out["temperature_C"] = None

    # Pluie brute
    if "rain_raw" in df.columns:
        out["pluie_brute"] = pd.to_numeric(df["rain_raw"], errors="coerce")
    else:
        out["pluie_brute"] = None

    # Vent brut
    if "wind_raw" in df.columns:
        out["vent_brut"] = pd.to_numeric(df["wind_raw"], errors="coerce")
    else:
        out["vent_brut"] = None

    out = out.sort_values("date_local").reset_index(drop=True)
    return out


# ---------------------------------
# 3. UI STREAMLIT
# ---------------------------------

st.set_page_config(
    page_title="Météo live SYNOP",
    page_icon="🌦️",
    layout="wide",
)

st.title("🌦️ Météo live (SYNOP / Opendatasoft)")
st.caption(
    "On récupère le schéma réel du dataset via include_schema=true, "
    "on devine les colonnes techniques, puis on interroge les mesures pour la station choisie."
)

# Récup schéma
schema_df = get_schema()
if schema_df.empty:
    st.stop()

cols_map = guess_columns(schema_df)

with st.expander("🔎 Debug schéma détecté"):
    st.write("Champs exposés par l'API (nom technique = 'name') :")
    show_cols = [c for c in ["name", "type", "label", "description"] if c in schema_df.columns]
    st.dataframe(schema_df[show_cols], use_container_width=True)
    st.write("Mapping heuristique pour les colonnes clés :")
    st.json(cols_map)

with st.sidebar:
    st.header("⚙️ Paramètres")

    # Choix station
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

    # Période par défaut = dernières 48h
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
        value=80,
        step=10,
    )

    run_query = st.button("🔍 Charger les données")


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

    with st.spinner("Appel API en cours..."):
        raw_df = fetch_data_for_station(cols_map, chosen_station_id, start_dt, end_dt, limit)
        synop_df = normalize_synop_df(raw_df)

    if synop_df.empty:
        st.warning("Aucune donnée renvoyée (ou mapping pas encore bon).")
    else:
        st.subheader("Aperçu des données normalisées")
        st.dataframe(synop_df.tail(20), use_container_width=True)

        # Température
        if "temperature_C" in synop_df.columns and synop_df["temperature_C"].notna().any():
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

        # Pluie brute
        if "pluie_brute" in synop_df.columns and synop_df["pluie_brute"].notna().any():
            fig_rain = px.bar(
                synop_df,
                x="date_local",
                y="pluie_brute",
                title="Pluie (valeur brute retournée par l'API)",
            )
            fig_rain.update_layout(
                xaxis_title="Heure (Europe/Paris)",
                yaxis_title="Pluie (unité API)",
            )
            st.plotly_chart(fig_rain, use_container_width=True)

        # Vent brut
        if "vent_brut" in synop_df.columns and synop_df["vent_brut"].notna().any():
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


with st.expander("Notes techniques"):
    st.markdown(
        "- On appelle maintenant /catalog/datasets/<id>?include_schema=true pour obtenir le schéma, "
        "car /fields n'est pas exposé sur ce domaine.\n"
        "- On détecte ensuite les colonnes probables par heuristique (date, température, pluie...).\n"
        "- On s'en sert pour construire la requête dynamique sur /records.\n"
        "- Dès que ça fonctionne et qu'on voit les bons noms (par ex. `id_omm_station`, `temperature`, etc.), "
        "on pourra les figer en dur, enlever toute la détection, et passer à l'étape cache local."
    )
