import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px

# ---------------------------------
# CONFIG GLOBALE
# ---------------------------------

# Identifiant du dataset SYNOP sur Opendatasoft
DATASET_ID = "donnees-synop-essentielles-omm"

# Base URL de l'API Opendatasoft (Explore v2)
BASE_URL = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/{DATASET_ID}"

# Champs courants attendus dans ce dataset :
# - date (timestamp UTC de l'observation)
# - id (code station)
# - nom (nom de la station)
# - latitude / longitude
# - temperature (°C)
# - rr1 (pluie en mm sur 1h)
# - ff (vent moyen m/s)
# suivant les portails, certains noms de colonnes peuvent varier un peu, on gère ça plus bas.


# ---------------------------------
# FONCTIONS API
# ---------------------------------

@st.cache_data(ttl=3600)
def get_stations(limit=200):
    """
    Récupère la liste des stations distinctes (id + nom + lat/lon).
    On limite volontairement pour éviter de spam l'API.
    Renvoie un DataFrame avec colonnes:
        - id
        - nom
        - latitude
        - longitude
        - station_label (pour affichage)
    En cas d'erreur API, renvoie un DataFrame vide et affiche l'erreur dans l'UI.
    """
    url = f"{BASE_URL}/records"
    params = {
        "select": "id,nom,latitude,longitude",
        "group_by": "id,nom,latitude,longitude",
        "limit": limit,
        "order_by": "nom ASC",
    }

    r = requests.get(url, params=params, timeout=30)

    if r.status_code != 200:
        st.error(
            f"❌ Erreur lors de la récupération des stations "
            f"(HTTP {r.status_code}).\nURL: {r.url}\nRéponse: {r.text[:500]}"
        )
        return pd.DataFrame()

    try:
        data = r.json().get("results", [])
    except Exception as e:
        st.error(f"❌ Réponse illisible (pas du JSON valide). Détail: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if "id" not in df.columns:
        st.error(
            "❌ Le champ 'id' n'existe pas dans la réponse. "
            "Le schéma du dataset a peut-être changé."
        )
        return pd.DataFrame()

    # Si pas de nom on fallback sur l'id
    df["station_label"] = df.apply(
        lambda row: f"{row.get('nom') or 'Station'} ({row['id']})", axis=1
    )

    df = df.sort_values("station_label").reset_index(drop=True)
    return df


def fetch_data_for_station(station_id: str, start_dt: datetime, end_dt: datetime, limit=10000):
    """
    Récupère les enregistrements SYNOP pour une station donnée entre start_dt et end_dt.
    On retourne un DataFrame brut.
    """
    url = f"{BASE_URL}/records"

    # Opendatasoft attend des timestamps ISO8601 style 2025-10-28T00:00:00Z
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    where_clause = (
        f"id = '{station_id}' AND date >= '{start_iso}' AND date <= '{end_iso}'"
    )

    params = {
        "where": where_clause,
        "limit": limit,
        "order_by": "date ASC",
    }

    r = requests.get(url, params=params, timeout=30)

    if r.status_code != 200:
        st.error(
            f"❌ Erreur lors de la récupération des mesures "
            f"(HTTP {r.status_code}).\nURL: {r.url}\nRéponse: {r.text[:500]}"
        )
        return pd.DataFrame()

    try:
        results = r.json().get("results", [])
    except Exception as e:
        st.error(f"❌ Réponse mesures illisible (pas du JSON valide). Détail: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    return df


def normalize_synop_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rend le DataFrame plus propre / standard :
    - mappe le timestamp -> date_utc / date_local
    - normalise les colonnes météo clefs (température, pluie 1h, vent moyen).
    On renvoie uniquement ce qui est utile pour l'affichage.
    """
    if df.empty:
        return df

    # Petits alias possibles pour certains champs
    col_map_candidates = {
        "date": ["date", "obs_time", "time", "timestamp"],
        "temperature": ["temperature", "t", "ta", "tc", "temp"],
        "rr1": ["rr1", "precipitation_1h", "rain_1h", "rr", "pluie"],
        "ff": ["ff", "ffmoy", "wind_speed", "vent_moyen"],
        "nom": ["nom", "station", "station_name"],
    }

    def pick_col(possible_names):
        for c in possible_names:
            if c in df.columns:
                return c
        return None

    date_col = pick_col(col_map_candidates["date"])
    temp_col = pick_col(col_map_candidates["temperature"])
    rain_col = pick_col(col_map_candidates["rr1"])
    wind_col = pick_col(col_map_candidates["ff"])
    name_col = pick_col(col_map_candidates["nom"])

    out = pd.DataFrame()

    # date UTC
    if date_col:
        out["date_utc"] = pd.to_datetime(df[date_col], errors="coerce", utc=True)
        # conversion fuseau pour lecture humaine (Europe/Paris)
        out["date_local"] = out["date_utc"].dt.tz_convert("Europe/Paris")
    else:
        out["date_utc"] = pd.NaT
        out["date_local"] = pd.NaT

    # température
    if temp_col:
        out["temperature_C"] = pd.to_numeric(df[temp_col], errors="coerce")
    else:
        out["temperature_C"] = None

    # pluie 1h
    if rain_col:
        out["pluie_mm_1h"] = pd.to_numeric(df[rain_col], errors="coerce")
    else:
        out["pluie_mm_1h"] = None

    # vent moyen
    if wind_col:
        out["vent_moyen_m_s"] = pd.to_numeric(df[wind_col], errors="coerce")
    else:
        out["vent_moyen_m_s"] = None

    # nom station
    if name_col:
        out["station_nom"] = df[name_col]
    else:
        out["station_nom"] = None

    out = out.sort_values("date_local").reset_index(drop=True)
    return out


# ---------------------------------
# CONFIG STREAMLIT
# ---------------------------------

st.set_page_config(
    page_title="Météo live SYNOP",
    page_icon="🌦️",
    layout="wide",
)

st.title("🌦️ Météo live (SYNOP / Opendatasoft)")
st.caption(
    "Prototype branché en direct sur l'API Opendatasoft (données SYNOP Météo France). "
    "Choisis une station, une période et visualise température, pluie, vent."
)

# ---------------------------------
# SIDEBAR (sélection utilisateur)
# ---------------------------------

with st.sidebar:
    st.header("⚙️ Paramètres")

    # 1. Liste des stations
    stations_df = get_stations()

    if stations_df.empty:
        st.error("Aucune station récupérée.")
        st.stop()

    station_choice = st.selectbox(
        "Station météo",
        options=stations_df.index,
        format_func=lambda idx: stations_df.loc[idx, "station_label"],
    )

    chosen_station_id = stations_df.loc[station_choice, "id"]
    st.write(f"ID station sélectionnée : `{chosen_station_id}`")

    # 2. Sélecteur de période
    # par défaut: dernières 48h
    default_end = datetime.utcnow()
    default_start = default_end - timedelta(days=2)

    start_date = st.date_input("Date début (UTC)", default_start.date())
    end_date = st.date_input("Date fin (UTC)", default_end.date())

    start_hour = st.number_input("Heure début (0-23)", min_value=0, max_value=23, value=0)
    end_hour = st.number_input("Heure fin (0-23)", min_value=0, max_value=23, value=23)

    run_query = st.button("🔍 Charger les données")

# ---------------------------------
# ZONE PRINCIPALE
# ---------------------------------

if run_query:
    # reconstruction datetimes UTC
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

    with st.spinner("Récupération des données météo en cours..."):
        raw_df = fetch_data_for_station(chosen_station_id, start_dt, end_dt)
        synop_df = normalize_synop_df(raw_df)

    if synop_df.empty:
        st.warning("Aucune donnée renvoyée pour cette période / station.")
    else:
        st.subheader("Aperçu des données normalisées")
        st.dataframe(synop_df.tail(20), use_container_width=True)

        # --------- Graph Température
        if "temperature_C" in synop_df.columns and synop_df["temperature_C"].notna().any():
            fig_temp = px.line(
                synop_df,
                x="date_local",
                y="temperature_C",
                title="Température (°C) - heure par heure",
                markers=True,
            )
            fig_temp.update_layout(
                xaxis_title="Heure (Europe/Paris)",
                yaxis_title="°C",
            )
            st.plotly_chart(fig_temp, use_container_width=True)
        else:
            st.info("Pas de température exploitable sur cette période.")

        # --------- Graph Pluie
        if "pluie_mm_1h" in synop_df.columns and synop_df["pluie_mm_1h"].notna().any():
            fig_rain = px.bar(
                synop_df,
                x="date_local",
                y="pluie_mm_1h",
                title="Précipitations (mm sur 1h)",
            )
            fig_rain.update_layout(
                xaxis_title="Heure (Europe/Paris)",
                yaxis_title="mm",
            )
            st.plotly_chart(fig_rain, use_container_width=True)
        else:
            st.info("Pas de pluie mesurée (ou champ absent).")

        # --------- Graph Vent
        if "vent_moyen_m_s" in synop_df.columns and synop_df["vent_moyen_m_s"].notna().any():
            fig_wind = px.line(
                synop_df,
                x="date_local",
                y="vent_moyen_m_s",
                title="Vent moyen (m/s)",
                markers=True,
            )
            fig_wind.update_layout(
                xaxis_title="Heure (Europe/Paris)",
                yaxis_title="m/s",
            )
            st.plotly_chart(fig_wind, use_container_width=True)
        else:
            st.info("Pas de vent moyen exploitable.")

else:
    st.info("➡ Choisis une station, une plage de dates, puis clique sur 'Charger les données'.")


# ---------------------------------
# DEBUG / INFOS TECH
# ---------------------------------

with st.expander("Détails techniques / debug"):
    st.write("Dataset utilisé :", DATASET_ID)
    st.write("URL base API :", BASE_URL)
    st.write("Notes :")
    st.markdown(
        "- On convertit l'heure UTC en fuseau Europe/Paris pour l'affichage.\n"
        "- Les champs météo (température, pluie, vent) sont normalisés.\n"
        "- Les appels API sont limités en volume public (risque de 429 si trop de requêtes)."
    )
