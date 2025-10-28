import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta, date
import plotly.express as px
from io import BytesIO

# ------------------------------
# CONFIG
# ------------------------------

DATASET_ID = "donnees-synop-essentielles-omm"
BASE_RECORDS_URL = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/{DATASET_ID}/records"

# Dictionnaire station_id -> label humain
# DOIT correspondre à la colonne numer_sta côté API
STATIONS = {
    "07110": "BREST",
    "07630": "PARIS-MONTSOURIS",
    "07761": "AJACCIO",
}

# Champs confirmés par le schéma live
COL_STATION_ID = "numer_sta"   # code OMM station
COL_DATE       = "date"        # horodatage UTC
COL_TEMP       = "tc"          # Température (°C)
COL_RAIN_1H    = "rr1"         # Précipitations dernière heure (mm)
COL_WIND       = "ff"          # Vent moyen 10 mn (m/s)
COL_NAME       = "nom"         # Nom humain de la station


# ------------------------------
# FONCTIONS
# ------------------------------

def fetch_data_for_station(station_id, start_dt, end_dt, limit):
    """
    Récupère les observations météo dans la période donnée (UTC),
    filtrées sur numer_sta (station_id) et triées par date asc.
    """
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso   = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    where_clause = (
        f"`{COL_STATION_ID}` = '{station_id}' "
        f"AND `{COL_DATE}` >= '{start_iso}' "
        f"AND `{COL_DATE}` <= '{end_iso}'"
    )

    select_clause = ", ".join([
        f"`{COL_DATE}` as date_utc",
        f"`{COL_STATION_ID}` as station_id",
        f"`{COL_NAME}` as station_name",
        f"`{COL_TEMP}` as temperature_C",
        f"`{COL_RAIN_1H}` as rain_mm_1h",
        f"`{COL_WIND}` as wind_ms",
    ])

    params = {
        "where": where_clause,
        "order_by": f"`{COL_DATE}` ASC",
        "limit": int(limit),  # <= 100
        "select": select_clause,
    }

    r = requests.get(BASE_RECORDS_URL, params=params, timeout=30)

    # debug API minimal
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
    return pd.DataFrame(results)


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    - parse date_utc en timezone-aware
    - convertit en heure locale Europe/Paris
    - re-range les colonnes en clair
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

    out["temperature_C"] = pd.to_numeric(df.get("temperature_C"), errors="coerce")
    out["rain_mm_1h"] = pd.to_numeric(df.get("rain_mm_1h"), errors="coerce")
    out["wind_ms"] = pd.to_numeric(df.get("wind_ms"), errors="coerce")

    # colonnes utilitaires pour le regroupement journalier
    out["jour_local"] = out["date_local"].dt.date           # date civile Europe/Paris
    out["heure_locale"] = out["date_local"].dt.hour         # heure locale (0..23)

    out = out.sort_values("date_local").reset_index(drop=True)
    return out


def pick_one_row_per_day(df: pd.DataFrame, heure_cible: int) -> pd.DataFrame:
    """
    Pour chaque jour (jour_local), on garde la ligne
    dont l'heure_locale est la plus proche de heure_cible.
    Ex: heure_cible=12 -> garde midi +/- 1h la plus proche.
    """
    if df.empty:
        return df

    # distance en heures à l'heure cible
    df = df.copy()
    df["ecart_h"] = (df["heure_locale"] - heure_cible).abs()

    # pour chaque jour_local, on prend la ligne avec ecart_h minimal
    # puis, en cas d'égalité (ex 11h et 13h sont à 1h de 12h), on prend la plus proche dans le temps réel (donc la plus petite ecart_h puis la plus tôt)
    df = df.sort_values(["jour_local", "ecart_h", "date_local"])
    daily = df.groupby("jour_local", as_index=False).first()

    # on renomme proprement les colonnes finales
    daily = daily[[
        "jour_local",
        "date_local",
        "station_id",
        "station_name",
        "temperature_C",
        "rain_mm_1h",
        "wind_ms",
        "heure_locale",
    ]].copy()

    return daily


def check_missing_days(daily_df: pd.DataFrame, start_dt_local: date, end_dt_local: date):
    """
    Vérifie qu'on a bien une ligne pour chaque jour entre start_dt_local et end_dt_local (inclus),
    en se basant sur la colonne 'jour_local' du daily_df.
    Renvoie (missing_days_list, all_good_bool)
    """
    # liste théorique de jours attendus
    expected_days = pd.date_range(start=start_dt_local, end=end_dt_local, freq="D").date

    if daily_df.empty:
        return list(expected_days), False

    got_days = set(daily_df["jour_local"].astype("object"))
    missing = [d for d in expected_days if d not in got_days]

    return missing, (len(missing) == 0)


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    """
    Exporte le DataFrame en mémoire au format Excel (.xlsx)
    et renvoie les bytes.
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="meteo_filtre")
    return output.getvalue()


# ------------------------------
# STREAMLIT UI
# ------------------------------

st.title("🌦️ Météo vs Activité (mode jour)")

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

# Bornes de période
default_end = datetime.utcnow()
default_start = default_end - timedelta(days=7)

col_left, col_right = st.columns(2)
with col_left:
    start_date = st.date_input("Date début (UTC)", default_start.date())
    start_hour = st.number_input("Heure début (0-23)", min_value=0, max_value=23, value=0)
with col_right:
    end_date = st.date_input("Date fin (UTC)", default_end.date())
    end_hour = st.number_input("Heure fin (0-23)", min_value=0, max_value=23, value=23)

# Heure cible pour l'analyse journalière
heure_cible = st.number_input(
    "Heure de référence (locale) pour la comparaison journalière",
    min_value=0, max_value=23, value=12,
    help="On ne garde qu'une seule mesure par jour : celle dont l'heure locale est la plus proche de cette heure."
)

# Limit API
limit = st.slider(
    "Nombre max de lignes (API Opendatasoft, max 100)",
    min_value=10,
    max_value=100,
    value=80,
    step=10,
)

run_query = st.button("🔍 Charger les données")


st.markdown("---")

if run_query:
    # Construit les deux datetimes UTC complets
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

    with st.spinner("Récupération météo brute..."):
        raw_df = fetch_data_for_station(chosen_station_id, start_dt, end_dt, limit)
        norm_df = normalize_df(raw_df)

    if norm_df.empty:
        st.warning("Aucune donnée brute renvoyée sur cet intervalle.")
    else:
        st.subheader("Données brutes normalisées (toutes les heures)")
        st.dataframe(norm_df, use_container_width=True)

        # Filtre '1 ligne par jour' basé sur heure_cible
        daily_df = pick_one_row_per_day(norm_df, heure_cible)

        st.subheader(f"Données résumées (1 ligne / jour autour de {heure_cible}h locale)")
        st.dataframe(daily_df, use_container_width=True)

        # Vérif des jours manquants
        # On regarde en timezone locale (Europe/Paris) : on prend simplement les dates calendrier
        missing_days, ok_all_days = check_missing_days(
            daily_df,
            start_dt_local=start_date,
            end_dt_local=end_date,
        )

        if ok_all_days:
            st.success("✅ Toutes les dates entre début et fin sont présentes après filtrage.")
        else:
            st.warning(
                "⚠ Certaines dates n'ont pas de point météo retenu (peut-être pas de mesure proche de l'heure cible ou pas de donnée API) : "
                + ", ".join(str(d) for d in missing_days)
            )

        # Graph température journalière (valeur retenue)
        if daily_df["temperature_C"].notna().any():
            fig_temp_day = px.line(
                daily_df,
                x="jour_local",
                y="temperature_C",
                markers=True,
                title=f"Température journalière (°C) autour de {heure_cible}h",
            )
            fig_temp_day.update_layout(
                xaxis_title="Jour (Europe/Paris)",
                yaxis_title="°C",
            )
            st.plotly_chart(fig_temp_day, use_container_width=True)

        # Pluie journalière (valeur de l'heure retenue, mm/1h)
        if daily_df["rain_mm_1h"].notna().any():
            fig_rain_day = px.bar(
                daily_df,
                x="jour_local",
                y="rain_mm_1h",
                title=f"Pluie mesurée l'heure retenue (mm sur 1h)",
            )
            fig_rain_day.update_layout(
                xaxis_title="Jour (Europe/Paris)",
                yaxis_title="mm / h",
            )
            st.plotly_chart(fig_rain_day, use_container_width=True)

        # Vent moyen journalière (valeur heure retenue)
        if daily_df["wind_ms"].notna().any():
            fig_wind_day = px.line(
                daily_df,
                x="jour_local",
                y="wind_ms",
                markers=True,
                title=f"Vent moyen (m/s) à l'heure retenue",
            )
            fig_wind_day.update_layout(
                xaxis_title="Jour (Europe/Paris)",
                yaxis_title="m/s",
            )
            st.plotly_chart(fig_wind_day, use_container_width=True)

        # Bouton de téléchargement Excel (daily_df, car c'est ça que tu compares au CA journalier)
        excel_bytes = to_excel_bytes(daily_df)
        st.download_button(
            label="⬇ Télécharger les données filtrées (1 ligne / jour) en Excel",
            data=excel_bytes,
            file_name=f"meteo_{chosen_station_id}_{start_date}_to_{end_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

else:
    st.info("➡ Règle la période, l'heure de référence, puis clique sur 'Charger les données'.")
