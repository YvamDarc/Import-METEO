import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px

# ------------------------------
# CONFIG
# ------------------------------

DATASET_ID = "donnees-synop-essentielles-omm"
BASE_RECORDS_URL = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/{DATASET_ID}/records"

# Dictionnaire station_id -> label humain
# ATTENTION: maintenant on sait que la colonne station côté API s'appelle numer_sta
# donc les clés ici doivent correspondre à numer_sta.
STATIONS = {
    "07110": "BREST",
    "07630": "PARIS-MONTSOURIS",
    "07761": "AJACCIO",
}

# Champs confirmés dans le dataset :
COL_STATION_ID = "numer_sta"   # code OMM station
COL_DATE       = "date"        # horodatage UTC
COL_TEMP       = "tc"          # Température (°C)
COL_RAIN_1H    = "rr1"         # Précipitations dernière heure (mm)
COL_WIND       = "ff"          # Vitesse vent moyen 10 mn (m/s)
COL_NAME       = "nom"         # Nom humain de la station


# ------------------------------
# FONCTION APPEL API
# ------------------------------

def fetch_data_for_station(station_id, start_dt, end_dt, limit):
    """
    Récupère les observations météo depuis l'API Opendatasoft pour une station OMM donnée (numer_sta),
    filtrées entre start_dt et end_dt (UTC), triées par date.
    On récupère uniquement les colonnes utiles : date, tc, rr1, ff, nom.
    limit doit être <= 100 (contrainte API).
    """

    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso   = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # WHERE dynamique, avec les bons noms de colonnes (confirmés par le schéma)
    where_clause = (
        f"`{COL_STATION_ID}` = '{station_id}' "
        f"AND `{COL_DATE}` >= '{start_iso}' "
        f"AND `{COL_DATE}` <= '{end_iso}'"
    )

    # SELECT : on renomme pour travailler propre ensuite
    select_clause = ", ".join([
        f"`{COL_DATE}` as date_utc",
        f"`{COL_STATION_ID}` as station_id",
        f"`{COL_NAME}` as station_name",
        f"`{COL_TEMP}` as temperature_C",
        f"`{COL_RAIN_1H}` as rain_mm_1h",
        f"`{COL_WIND}` as wind_ms"
    ])

    params = {
        "where": where_clause,
        "order_by": f"`{COL_DATE}` ASC",
        "limit": int(limit),
        "select": select_clause,
    }

    r = requests.get(BASE_RECORDS_URL, params=params, timeout=30)

    # debug doux -> ça s'affiche toujours pour qu'on puisse inspecter si souci
    st.write("🛰️ DEBUG /records status_code:", r.status_code)
    st.write("🛰️ DEBUG URL appelée:", r.url)

    if r.status_code != 200:
        st.write("🛰️ DEBUG Réponse brute:", r.text[:500])
        st.error("Erreur API sur /records")
        return pd.DataFrame()

    try:
        data_json = r.json()
    except Exception:
        st.write("🛰️ DEBUG Réponse brute JSON invalide:", r.text[:500])
        st.error("Réponse API illisible (pas du JSON)")
        return pd.DataFrame()

    results = data_json.get("results", [])
    df = pd.DataFrame(results)
    return df


# ------------------------------
# NORMALISATION DES DONNÉES
# ------------------------------

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    - parse des dates UTC
    - convertit en Europe/Paris pour affichage lisible
    - renomme clairement les colonnes
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

    # température déjà en °C (champ tc)
    out["temperature_C"] = pd.to_numeric(df.get("temperature_C"), errors="coerce")

    # pluie dernière heure en mm
    out["rain_mm_1h"] = pd.to_numeric(df.get("rain_mm_1h"), errors="coerce")

    # vent moyen en m/s
    out["wind_ms"] = pd.to_numeric(df.get("wind_ms"), errors="coerce")

    out = out.sort_values("date_local").reset_index(drop=True)
    return out


# ------------------------------
# STREAMLIT UI
# ------------------------------

st.header("Paramètres")

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

st.write(f"ID OMM station : {chosen_station_id}")
st.write(f"Nom affiché : {chosen_station_name}")

# Période par défaut = dernière journée UTC
default_end = datetime.utcnow()
default_start = default_end - timedelta(days=1)

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Date début (UTC)", default_start.date())
    start_hour = st.number_input("Heure début (0-23)", min_value=0, max_value=23, value=0)
with col2:
    end_date = st.date_input("Date fin (UTC)", default_end.date())
    end_hour = st.number_input("Heure fin (0-23)", min_value=0, max_value=23, value=23)

limit = st.slider(
    "Nombre max de lignes renvoyées (<=100)",
    min_value=10,
    max_value=100,
    value=50,
    step=10,
)

run_query = st.button("🔍 Charger les données")

st.markdown("---")

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

    with st.spinner("Appel API /records avec les bons champs..."):
        raw_df = fetch_data_for_station(chosen_station_id, start_dt, end_dt, limit)
        norm_df = normalize_df(raw_df)

    if norm_df.empty:
        st.warning("Aucune donnée reçue pour cette station/période (ou limite trop petite).")
    else:
        st.subheader("Aperçu des données normalisées")
        st.dataframe(norm_df.tail(20), use_container_width=True)

        # Température °C
        if norm_df["temperature_C"].notna().any():
            fig_temp = px.line(
                norm_df,
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
            st.info("Pas de température exploitable sur la période.")

        # Pluie mm/1h
        if norm_df["rain_mm_1h"].notna().any():
            fig_rain = px.bar(
                norm_df,
                x="date_local",
                y="rain_mm_1h",
                title="Précipitations (mm sur la dernière heure)",
            )
            fig_rain.update_layout(
                xaxis_title="Heure (Europe/Paris)",
                yaxis_title="mm / h",
            )
            st.plotly_chart(fig_rain, use_container_width=True)
        else:
            st.info("Pas de précipitation mesurée (ou pas de champ rr1 dispo).")

        # Vent m/s
        if norm_df["wind_ms"].notna().any():
            fig_wind = px.line(
                norm_df,
                x="date_local",
                y="wind_ms",
                title="Vent moyen 10 min (m/s)",
                markers=True,
            )
            fig_wind.update_layout(
                xaxis_title="Heure (Europe/Paris)",
                yaxis_title="m/s",
            )
            st.plotly_chart(fig_wind, use_container_width=True)
        else:
            st.info("Pas de vent exploitable (champ ff vide ?).")

else:
    st.info("➡ Règle la période puis clique sur 'Charger les données'.")
