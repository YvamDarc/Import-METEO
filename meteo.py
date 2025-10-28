import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px


# -------------------------------------------------
# CONFIG DE BASE
# -------------------------------------------------

DATASET_ID = "donnees-synop-essentielles-omm"
BASE_DATA_URL = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/{DATASET_ID}"
BASE_META_URL = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/{DATASET_ID}/fields"

# Stations qu'on teste
STATIONS = {
    "07110": "BREST",
    "07630": "PARIS-MONTSOURIS",
    "07761": "AJACCIO",
}


# -------------------------------------------------
# 1. RÉCUPÉRATION DES MÉTADONNÉES DE CHAMPS
# -------------------------------------------------

@st.cache_data(ttl=3600)
def get_fields():
    """
    Va lire la description des colonnes exposées par l'API Opendatasoft
    pour ce dataset. On récupère le 'name' technique de chaque champ.
    """
    r = requests.get(BASE_META_URL, timeout=30)
    if r.status_code != 200:
        st.error(
            f"❌ Erreur récupération métadonnées champs (HTTP {r.status_code}).\n"
            f"URL: {BASE_META_URL}\nRéponse: {r.text[:500]}"
        )
        return pd.DataFrame()

    data = r.json()  # c'est censé être une liste de champs
    df = pd.DataFrame(data)

    # On affiche dans l'UI plus tard pour debug
    return df


def guess_columns(fields_df: pd.DataFrame):
    """
    À partir de la liste des champs techniques de l'API, on essaie d'identifier :
    - col_date            (horodatage UTC)
    - col_station_id      (code station OMM)
    - col_station_name    (nom lisible)
    - col_temp            (température en Kelvin)
    - col_rain            (pluie cumulée)
    - col_wind            (vent moyen)

    On fait ça par heuristique (recherche de bouts de mots).
    """
    # normaliser pour chercher
    def find_col(candidates_substrings, must_contain_all=False):
        for fname in fields_df["name"]:
            low = fname.lower()
            if must_contain_all:
                if all(sub in low for sub in candidates_substrings):
                    return fname
            else:
                if any(sub in low for sub in candidates_substrings):
                    return fname
        return None

    col_date         = find_col(["date"])  # souvent "date"
    col_station_id   = find_col(["omm", "station"], must_contain_all=True) or find_col(["omm"])
    col_station_name = find_col(["station"])
    col_temp         = find_col(["temp"])      # "temperature"
    col_rain         = find_col(["rain", "pluie", "precip"])
    col_wind         = find_col(["wind", "vent", "ff"])

    # petit correctif : le nom "station" sert parfois pour le nom humain.
    # mais si col_station_id == col_station_name, on va essayer d'être plus fin.
    if col_station_id == col_station_name:
        # on va réessayer station_id en cherchant "id" dedans
        maybe_id = [n for n in fields_df["name"] if "omm" in n.lower() or "id" in n.lower()]
        if maybe_id:
            col_station_id = maybe_id[0]

    return {
        "date": col_date,
        "station_id": col_station_id,
        "station_name": col_station_name,
        "temp": col_temp,
        "rain": col_rain,
        "wind": col_wind,
    }


# -------------------------------------------------
# 2. RÉCUPÉRATION DES MESURES
# -------------------------------------------------

def fetch_data_for_station(cols_map, station_id, start_dt, end_dt, limit):
    """
    Va chercher les données météo depuis l'API Opendatasoft pour une station et une période,
    en utilisant les noms RÉELS des colonnes (cols_map).
    """

    # Vérif minimum : il nous faut au moins la date et l'id station
    if not cols_map["date"] or not cols_map["station_id"]:
        st.error("❌ Impossible d'identifier les colonnes 'date' ou 'station_id' depuis les métadonnées.")
        return pd.DataFrame()

    url = f"{BASE_DATA_URL}/records"

    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Construction dynamique du WHERE
    # Attention : les noms de colonnes techniques n'ont en général PAS besoin de backticks s'ils sont déjà safe,
    # mais on va les backticher par sécurité. Si l'API râle encore, on essaiera sans.
    col_date_api = cols_map["date"]
    col_id_api = cols_map["station_id"]

    where_clause = (
        f"`{col_id_api}` = '{station_id}' "
        f"AND `{col_date_api}` >= '{start_iso}' "
        f"AND `{col_date_api}` <= '{end_iso}'"
    )

    # Construction du SELECT dynamique
    select_parts = [
        f"`{col_date_api}` as date_utc",
        f"`{col_id_api}` as station_id",
    ]

    if cols_map["station_name"]:
        select_parts.append(f"`{cols_map['station_name']}` as station_name")
    if cols_map["temp"]:
        select_parts.append(f"`{cols_map['temp']}` as temperature_raw")
    if cols_map["rain"]:
        select_parts.append(f"`{cols_map['rain']}` as rain_raw")
    if cols_map["wind"]:
        select_parts.append(f"`{cols_map['wind']}` as wind_raw")

    params = {
        "where": where_clause,
        "limit": limit,  # max 100
        "order_by": f"`{col_date_api}` ASC",
        "select": ", ".join(select_parts),
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
    Nettoie les colonnes récupérées :
    - parse la date UTC
    - convertit en heure locale Europe/Paris
    - essaie de convertir temperature_raw K -> °C si ça ressemble à du Kelvin
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

    # station
    out["station_id"] = df.get("station_id")
    out["station_name"] = df.get("station_name")

    # température
    if "temperature_raw" in df.columns:
        temp_series = pd.to_numeric(df["temperature_raw"], errors="coerce")
        # heuristique Kelvin -> °C (si >200 on assume Kelvin, sinon déjà °C)
        out["temperature_C"] = temp_series.where(temp_series < 200, temp_series - 273.15)
    else:
        out["temperature_C"] = None

    # pluie brute
    if "rain_raw" in df.columns:
        out["pluie_raw"] = pd.to_numeric(df["rain_raw"], errors="coerce")
    else:
        out["pluie_raw"] = None

    # vent brut
    if "wind_raw" in df.columns:
        out["vent_moyen_raw"] = pd.to_numeric(df["wind_raw"], errors="coerce")
    else:
        out["vent_moyen_raw"] = None

    out = out.sort_values("date_local").reset_index(drop=True)
    return out


# -------------------------------------------------
# CONFIG STREAMLIT / UI
# -------------------------------------------------

st.set_page_config(
    page_title="Météo live SYNOP",
    page_icon="🌦️",
    layout="wide",
)

st.title("🌦️ Météo live (SYNOP / Opendatasoft)")
st.caption(
    "On récupère d'abord la structure du dataset (noms réels des colonnes), "
    "puis on interroge les mesures pour la station choisie."
)

fields_df = get_fields()
if fields_df.empty:
    st.stop()

cols_map = guess_columns(fields_df)

with st.expander("Debug: champs détectés dans le dataset"):
    st.write("Champs exposés par l'API :")
    st.dataframe(fields_df[["name", "type", "label"]], use_container_width=True)
    st.write("Mapping détecté pour les colonnes clés :")
    st.json(cols_map)

with st.sidebar:
    st.header("⚙️ Paramètres")

    # 1. Choix station
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

    # 2. Période par défaut: dernières 48h
    default_end = datetime.utcnow()
    default_start = default_end - timedelta(days=2)

    start_date = st.date_input("Date début (UTC)", default_start.date())
    end_date = st.date_input("Date fin (UTC)", default_end.date())

    start_hour = st.number_input("Heure début (0-23)", min_value=0, max_value=23, value=0)
    end_hour = st.number_input("Heure fin (0-23)", min_value=0, max_value=23, value=23)

    # 3. Limit API
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
        st.warning("Aucune donnée renvoyée (ou mapping colonnes encore faux).")
    else:
        st.subheader("Aperçu des données normalisées")
        st.dataframe(synop_df.tail(20), use_container_width=True)

        # Température
        if "temperature_C" in synop_df.columns and synop_df["temperature_C"].notna().any():
            fig_temp = px.line(
                synop_df,
                x="date_local",
                y="temperature_C",
                title="Température (°C) estimée",
                markers=True,
            )
            fig_temp.update_layout(
                xaxis_title="Heure (Europe/Paris)",
                yaxis_title="°C",
            )
            st.plotly_chart(fig_temp, use_container_width=True)

        # Pluie brute
        if "pluie_raw" in synop_df.columns and synop_df["pluie_raw"].notna().any():
            fig_rain = px.bar(
                synop_df,
                x="date_local",
                y="pluie_raw",
                title="Pluie brute (unité fournie API)",
            )
            fig_rain.update_layout(
                xaxis_title="Heure (Europe/Paris)",
                yaxis_title="Valeur pluie API",
            )
            st.plotly_chart(fig_rain, use_container_width=True)

        # Vent brut
        if "vent_moyen_raw" in synop_df.columns and synop_df["vent_moyen_raw"].notna().any():
            fig_wind = px.line(
                synop_df,
                x="date_local",
                y="vent_moyen_raw",
                title="Vent moyen brut (unité API)",
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
        "- On interroge d'abord /fields pour découvrir les VRAIS noms backend des colonnes exposées par Opendatasoft. "
        "C'est pour ça qu'on affiche un tableau 'Champs exposés par l'API'.\n"
        "- Ensuite on reconstruit dynamiquement la requête `where` / `select`.\n"
        "- Si ça marche, on saura exactement comment s'appellent les colonnes (ex: `id_omm_station`, `temperature`, etc.).\n"
        "- Après ça, on pourra figer ces noms et passer en mode cache local."
    )
