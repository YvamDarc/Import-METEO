import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

# ---------------------------------
# CONFIG
# ---------------------------------
# Dataset SYNOP sur Opendatasoft (Météo France - observations horaires)
DATASET_ID = "donnees-synop-essentielles-omm@public"  # si besoin on ajustera
BASE_URL = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/{DATASET_ID}"

# Champs utiles classiques dans ce jeu de données :
#  - date (obs time UTC)
#  - nom (nom de la station)
#  - id (id station WMO)
#  - latitude / longitude
#  - temperature (°C)
#  - rr1 (pluie dernière heure en mm)
#  - ff (vent moyen 10m en m/s)
#  - dd (direction vent en °)
#
# Les noms exacts peuvent légèrement varier selon la version du dataset,
# on gère ça un minimum plus bas.


# ---------------------------------
# PETITES FONCTIONS API
# ---------------------------------

@st.cache_data(ttl=3600)
def get_stations(limit=200):
    """
    Récupère la liste des stations distinctes (id + nom + lat/lon).
    On limite volontairement pour ne pas spam l'API.
    """
    url = f"{BASE_URL}/records"
    params = {
        "select": "id,nom,latitude,longitude",
        "group_by": "id,nom,latitude,longitude",
        "limit": limit,
        "order_by": "nom ASC",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("results", [])

    df = pd.DataFrame(data)
    # nettoyage minimal
    if "id" not in df.columns:
        st.error("Le champ 'id' n'existe pas dans la réponse. Le dataset a peut-être changé.")
        return pd.DataFrame()

    # Certaines stations peuvent être sans nom -> on fallback sur l'id
    df["station_label"] = df.apply(
        lambda row: f"{row.get('nom') or 'Station'} ({row['id']})", axis=1
    )
    return df.sort_values("station_label").reset_index(drop=True)


def fetch_data_for_station(station_id: str, start_dt: datetime, end_dt: datetime, limit=10000):
    """
    Récupère les enregistrements SYNOP pour une station donnée
    entre start_dt et end_dt (inclus).
    On demande JSON, puis on transforme en DataFrame.
    """
    url = f"{BASE_URL}/records"
    # Format datetime attendu par Opendatasoft = ISO8601 'YYYY-MM-DDTHH:MM:SSZ'
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
    r.raise_for_status()
    results = r.json().get("results", [])
    df = pd.DataFrame(results)

    return df


def normalize_synop_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Harmonise les colonnes pour éviter les surprises si le schéma bouge :
    - renomme les colonnes clés si besoin
    - parse la date
    - garde un sous-ensemble intéressant
    """
    if df.empty:
        return df

    # On essaye plusieurs variantes de noms possibles
    col_map_candidates = {
        "date": ["date", "obs_time", "time", "timestamp"],
        "temperature": ["temperature", "t", "ta", "tc", "temp"],
        "rr1": ["rr1", "precipitation_1h", "rain_1h", "rr", "pluie"],
        "ff": ["ff", "ffmoy", "wind_speed", "vent_moyen"],
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

    # On reconstruit un petit df standardisé
    out = pd.DataFrame()

    if date_col:
        out["date"] = pd.to_datetime(df[date_col], errors="coerce", utc=True)
        out["date_local"] = out["date"].dt.tz_convert("Europe/Paris")
    else:
        out["date"] = pd.NaT
        out["date_local"] = pd.NaT

    if temp_col:
        out["temperature_C"] = pd.to_numeric(df[temp_col], errors="coerce")
    else:
        out["temperature_C"] = None

    if rain_col:
        out["pluie_mm_1h"] = pd.to_numeric(df[rain_col], errors="coerce")
    else:
        out["pluie_mm_1h"] = None

    if wind_col:
        out["vent_moyen_m_s"] = pd.to_numeric(df[wind_col], errors="coerce")
    else:
        out["vent_moyen_m_s"] = None

    # garde aussi le nom de la station si dispo
    if "nom" in df.columns:
        out["station_nom"] = df["nom"]
    else:
        out["station_nom"] = None

    return out.sort_values("date_local").reset_index(drop=True)


# ---------------------------------
# UI STREAMLIT
# ---------------------------------

st.set_page_config(
    page_title="Météo live SYNOP",
    page_icon="🌦️",
    layout="wide"
)

st.title("🌦️ Météo live (SYNOP / Opendatasoft)")
st.caption(
    "Prototype connecté en direct à l'API Opendatasoft. "
    "On peut sélectionner une station météo et une plage de dates pour visualiser température, pluie, vent."
)

with st.sidebar:
    st.header("⚙️ Paramètres")

    # 1. Récupérer liste de stations
    stations_df = get_stations()

    if stations_df.empty:
        st.error("Impossible de récupérer les stations. Vérifie le dataset_id ou la connexion réseau.")
        st.stop()

    station_choice = st.selectbox(
        "Station météo",
        options=stations_df.index,
        format_func=lambda idx: stations_df.loc[idx, "station_label"]
    )

    chosen_station_id = stations_df.loc[station_choice, "id"]
    st.write(f"ID station sélectionnée : `{chosen_station_id}`")

    # 2. Période de dates
    # par défaut: les dernières 48h
    default_end = datetime.utcnow()
    default_start = default_end - timedelta(days=2)

    start_date = st.date_input("Date début (UTC)", default_start.date())
    end_date = st.date_input("Date fin (UTC)", default_end.date())

    # On récupère l'heure aussi si on veut être précis
    start_hour = st.number_input("Heure début (0-23)", min_value=0, max_value=23, value=0)
    end_hour = st.number_input("Heure fin (0-23)", min_value=0, max_value=23, value=23)

    run_query = st.button("🔍 Charger les données")

# zone principale
if run_query:
    with st.spinner("Récupération en cours depuis l'API Opendatasoft..."):
        # reconstruire datetimes complets
        start_dt = datetime(
            year=start_date.year,
            month=start_date.month,
            day=start_date.day,
            hour=start_hour,
            minute=0,
            second=0,
        )

        end_dt = datetime(
            year=end_date.year,
            month=end_date.month,
            day=end_date.day,
            hour=end_hour,
            minute=0,
            second=0,
        )

        raw_df = fetch_data_for_station(chosen_station_id, start_dt, end_dt)
        synop_df = normalize_synop_df(raw_df)

    if synop_df.empty:
        st.warning("Aucune donnée renvoyée pour cette période / station.")
    else:
        st.subheader("Aperçu brut standardisé")
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
            fig_temp.update_layout(xaxis_title="Heure (Europe/Paris)", yaxis_title="°C")
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
            fig_rain.update_layout(xaxis_title="Heure (Europe/Paris)", yaxis_title="mm")
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
            fig_wind.update_layout(xaxis_title="Heure (Europe/Paris)", yaxis_title="m/s")
            st.plotly_chart(fig_wind, use_container_width=True)
        else:
            st.info("Pas de vent moyen exploitable.")


else:
    st.info("➡ Choisis une station, une plage de dates, puis clique sur 'Charger les données'.")


# ---------------------------------
# Notes techniques affichées en bas
# ---------------------------------
with st.expander("Détails techniques / debug"):
    st.write("Dataset utilisé :", DATASET_ID)
    st.write("URL base :", BASE_URL)
    st.write("Exemple de requête récup stations : GET /records avec select/group_by.")
    st.write("On convertit l'heure UTC en heure locale Europe/Paris pour l'affichage.")
