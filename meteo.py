import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px

# ---------------------------------
# CONFIG GLOBALE
# ---------------------------------

DATASET_ID = "donnees-synop-essentielles-omm"
BASE_URL = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/{DATASET_ID}"

# Liste de stations en dur pour l’instant.
STATIONS = {
    "07110": "BREST",
    "07630": "PARIS-MONTSOURIS",
    "07761": "AJACCIO",
    # ajoute tes stations ici
}

# Noms des colonnes EXACTS (avec espaces) côté Opendatasoft
COL_DATE = "Date"
COL_ID = "ID OMM station"
COL_TEMP = "Temperature"
COL_RAIN = "Rainfall 3 last hours"
COL_WIND = "Average wind 10 mn"
COL_NAME = "Station"


# ---------------------------------
# FONCTIONS METEO
# ---------------------------------

def fetch_data_for_station(station_id: str, start_dt: datetime, end_dt: datetime, limit: int):
    """
    Récupère les enregistrements SYNOP pour une station donnée entre start_dt et end_dt.
    limit doit être <= 100 (contrainte Opendatasoft v2.1).
    On renvoie un DataFrame brut.
    """
    url = f"{BASE_URL}/records"

    # Format ISO8601 UTC attendu par Opendatasoft
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # les champs avec espaces doivent être entourés de backticks `
    where_clause = (
        f"`{COL_ID}` = '{station_id}' "
        f"AND `{COL_DATE}` >= '{start_iso}' "
        f"AND `{COL_DATE}` <= '{end_iso}'"
    )

    params = {
        "where": where_clause,
        "limit": limit,  # <= 100 obligatoire
        "order_by": f"`{COL_DATE}` ASC",
        "select": (
            f"`{COL_DATE}` as date_utc, "
            f"`{COL_ID}` as station_id, "
            f"`{COL_NAME}` as station_name, "
            f"`{COL_TEMP}` as temperature_K, "
            f"`{COL_RAIN}` as rain_mm_3h, "
            f"`{COL_WIND}` as wind_avg_ms"
        ),
    }

    r = requests.get(url, params=params, timeout=30)

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
    Nettoie les colonnes :
    - parse dates utc -> local (Europe/Paris)
    - Kelvin -> °C
    - renomme pluie/vent
    """
    if df.empty:
        return df

    out = pd.DataFrame()

    # horodatage
    if "date_utc" in df.columns:
        out["date_utc"] = pd.to_datetime(df["date_utc"], errors="coerce", utc=True)
        out["date_local"] = out["date_utc"].dt.tz_convert("Europe/Paris")
    else:
        out["date_utc"] = pd.NaT
        out["date_local"] = pd.NaT

    # infos station
    out["station_id"] = df.get("station_id")
    out["station_name"] = df.get("station_name")

    # température K -> °C
    if "temperature_K" in df.columns:
        out["temperature_C"] = pd.to_numeric(df["temperature_K"], errors="coerce") - 273.15
    else:
        out["temperature_C"] = None

    # pluie cumulée sur 3h
    if "rain_mm_3h" in df.columns:
        out["pluie_mm_3h"] = pd.to_numeric(df["rain_mm_3h"], errors="coerce")
    else:
        out["pluie_mm_3h"] = None

    # vent moyen 10 min (m/s)
    if "wind_avg_ms" in df.columns:
        out["vent_moyen_m_s"] = pd.to_numeric(df["wind_avg_ms"], errors="coerce")
    else:
        out["vent_moyen_m_s"] = None

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
    "Sélectionne une station et une période. On tape l’API en direct et on trace température / pluie / vent."
)

# ---------------------------------
# SIDEBAR (PARAMÈTRES UTILISATEUR)
# ---------------------------------

with st.sidebar:
    st.header("⚙️ Paramètres")

    # 1. Choix de la station
    station_codes = list(STATIONS.keys())
    station_labels = [f"{STATIONS[c]} ({c})" for c in station_codes]

    station_idx = st.selectbox(
        "Station météo",
        options=range(len(station_codes)),
        format_func=lambda i: station_labels[i],
    )

    chosen_station_id = station_codes[station_idx]
    chosen_station_name = STATIONS[chosen_station_id]

    st.write(f"ID OMM station : `{chosen_station_id}`")
    st.write(f"Nom : {chosen_station_name}")

    # 2. Période par défaut = dernières 48h
    default_end = datetime.utcnow()
    default_start = default_end - timedelta(days=2)

    start_date = st.date_input("Date début (UTC)", default_start.date())
    end_date = st.date_input("Date fin (UTC)", default_end.date())

    start_hour = st.number_input("Heure début (0-23)", min_value=0, max_value=23, value=0)
    end_hour = st.number_input("Heure fin (0-23)", min_value=0, max_value=23, value=23)

    # 3. Limit API (max 100 autorisé)
    limit = st.slider(
        "Nombre max de lignes à récupérer (limite API 100)",
        min_value=10,
        max_value=100,
        value=80,
        step=10,
        help="L'API Opendatasoft renvoie au max 100 enregistrements par appel.",
    )

    run_query = st.button("🔍 Charger les données")


# ---------------------------------
# CONTENU PRINCIPAL
# ---------------------------------

if run_query:
    # reconstituer les timestamps UTC complets
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

    with st.spinner("Récupération des données météo..."):
        raw_df = fetch_data_for_station(chosen_station_id, start_dt, end_dt, limit=limit)
        synop_df = normalize_synop_df(raw_df)

    if synop_df.empty:
        st.warning("Aucune donnée renvoyée pour cette période / station.")
    else:
        st.subheader("Aperçu des données (normalisées)")
        st.dataframe(synop_df.tail(20), use_container_width=True)

        # --------- Température (°C)
        if "temperature_C" in synop_df.columns and synop_df["temperature_C"].notna().any():
            fig_temp = px.line(
                synop_df,
                x="date_local",
                y="temperature_C",
                title="Température (°C)",
                markers=True,
            )
            fig_temp.update_layout(
                xaxis_title="Heure (Europe/Paris)",
                yaxis_title="°C",
            )
            st.plotly_chart(fig_temp, use_container_width=True)
        else:
            st.info("Pas de temperature exploitable.")

        # --------- Pluie cumulée 3h (mm)
        if "pluie_mm_3h" in synop_df.columns and synop_df["pluie_mm_3h"].notna().any():
            fig_rain = px.bar(
                synop_df,
                x="date_local",
                y="pluie_mm_3h",
                title="Précipitations cumulées sur 3h (mm)",
            )
            fig_rain.update_layout(
                xaxis_title="Heure (Europe/Paris)",
                yaxis_title="mm / 3h",
            )
            st.plotly_chart(fig_rain, use_container_width=True)
        else:
            st.info("Pas de pluie mesurée (ou champ absent).")

        # --------- Vent moyen 10 min (m/s)
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
    st.info("➡ Choisis une station, une plage de dates, règle le nombre de lignes, puis clique sur 'Charger les données'.")


# ---------------------------------
# DEBUG / INFOS TECH
# ---------------------------------

with st.expander("Détails techniques / debug"):
    st.write("Dataset utilisé :", DATASET_ID)
    st.write("URL base API :", BASE_URL)
    st.markdown(
        "- Les colonnes du dataset ont des espaces, donc on les entoure avec des backticks ` dans le where.\n"
        "- Température fournie en Kelvin -> convertie en °C.\n"
        "- Pluie dispo en cumul 3h (pas toujours 1h).\n"
        "- Vent = vent moyen 10 minutes (m/s).\n"
        "- L'API limite 'limit' à 100 lignes max par appel."
    )
