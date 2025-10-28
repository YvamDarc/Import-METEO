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

STATIONS = {
    "07110": "BREST",
    "07630": "PARIS-MONTSOURIS",
    "07761": "AJACCIO",
}

COL_STATION_ID = "numer_sta"
COL_DATE       = "date"
COL_TEMP       = "tc"
COL_RAIN_1H    = "rr1"
COL_WIND       = "ff"
COL_NAME       = "nom"


# ------------------------------
# API HELPERS
# ------------------------------

def fetch_data_for_station(station_id, start_dt, end_dt, limit):
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
        "limit": int(limit),
        "select": select_clause,
    }

    r = requests.get(BASE_RECORDS_URL, params=params, timeout=30)

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


def fetch_last_observation_for_station(station_id):
    """
    Récupère la DERNIÈRE observation connue pour une station,
    sans contrainte de date : tri date DESC, limit 1.
    Ça nous permet de dire à l'utilisateur jusqu'à quand cette station est alimentée.
    """
    params = {
        "where": f"`{COL_STATION_ID}` = '{station_id}'",
        "order_by": f"`{COL_DATE}` DESC",
        "limit": 1,
        "select": (
            f"`{COL_DATE}` as date_utc, "
            f"`{COL_STATION_ID}` as station_id, "
            f"`{COL_NAME}` as station_name, "
            f"`{COL_TEMP}` as temperature_C, "
            f"`{COL_RAIN_1H}` as rain_mm_1h, "
            f"`{COL_WIND}` as wind_ms"
        ),
    }

    r = requests.get(BASE_RECORDS_URL, params=params, timeout=30)

    st.write("🛰️ DEBUG LAST status_code:", r.status_code)
    st.write("🛰️ DEBUG LAST URL:", r.url)

    if r.status_code != 200:
        st.write("🛰️ DEBUG LAST Réponse brute:", r.text[:500])
        return pd.DataFrame()

    try:
        data_json = r.json()
    except Exception:
        st.write("🛰️ DEBUG LAST Réponse brute JSON invalide:", r.text[:500])
        return pd.DataFrame()

    results = data_json.get("results", [])
    return pd.DataFrame(results)


# ------------------------------
# TRANSFORMATIONS
# ------------------------------

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
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
    out["rain_mm_1h"]    = pd.to_numeric(df.get("rain_mm_1h"), errors="coerce")
    out["wind_ms"]       = pd.to_numeric(df.get("wind_ms"), errors="coerce")

    out["jour_local"]   = out["date_local"].dt.date
    out["heure_locale"] = out["date_local"].dt.hour

    out = out.sort_values("date_local").reset_index(drop=True)
    return out


def pick_one_row_per_day(df: pd.DataFrame, heure_cible: int) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    df = df.copy()
    df["ecart_h"] = (df["heure_locale"] - heure_cible).abs()

    # tri pour garantir qu'on prend la plus proche de l'heure cible
    df = df.sort_values(["jour_local", "ecart_h", "date_local"])
    daily = df.groupby("jour_local", as_index=False).first()

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
    expected_days = pd.date_range(start=start_dt_local, end=end_dt_local, freq="D").date

    if daily_df.empty:
        return list(expected_days), False

    got_days = set(daily_df["jour_local"].astype("object"))
    missing = [d for d in expected_days if d not in got_days]

    return missing, (len(missing) == 0)


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="meteo_filtre")
    return output.getvalue()


# ------------------------------
# UI
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

# Bornes temporelles
default_end = datetime.utcnow()
default_start = default_end - timedelta(days=7)

col_left, col_right = st.columns(2)
with col_left:
    start_date = st.date_input("Date début (UTC)", default_start.date())
    start_hour = st.number_input("Heure début (0-23)", min_value=0, max_value=23, value=0)
with col_right:
    end_date = st.date_input("Date fin (UTC)", default_end.date())
    end_hour = st.number_input("Heure fin (0-23)", min_value=0, max_value=23, value=23)

heure_cible = st.number_input(
    "Heure de référence locale pour l'agrégation journalière",
    min_value=0, max_value=23, value=12,
    help="On garde pour chaque jour la mesure la plus proche de cette heure (ex: 12h).",
)

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
    # datetimes UTC de la requête
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
        st.warning("Aucune donnée brute renvoyée sur cet intervalle pour cette station.")

        # 🔍 On va chercher la dernière donnée dispo pour cette station
        st.info("Je vérifie la dernière observation connue pour cette station...")
        last_df = fetch_last_observation_for_station(chosen_station_id)
        last_norm = normalize_df(last_df)

        if last_norm.empty:
            st.error("Pas de dernière observation trouvée pour cette station (station peut-être inactive ?).")
        else:
            last_row = last_norm.iloc[0]
            st.warning(
                f"⚠ Pas de données entre {start_dt} et {end_dt}.\n\n"
                f"Dernière mesure connue pour {chosen_station_name} ({chosen_station_id}) :\n"
                f"- date locale : {last_row['date_local']}\n"
                f"- temp (°C) : {last_row['temperature_C']}\n"
                f"- pluie 1h (mm) : {last_row['rain_mm_1h']}\n"
                f"- vent (m/s) : {last_row['wind_ms']}"
            )

    else:
        st.subheader("Données brutes normalisées (toutes les heures)")
        st.dataframe(norm_df, use_container_width=True)

        # Une ligne par jour autour de l'heure cible
        daily_df = pick_one_row_per_day(norm_df, heure_cible)

        st.subheader(f"Données agrégées (1 ligne / jour autour de {heure_cible}h locale)")
        st.dataframe(daily_df, use_container_width=True)

        # Vérification des trous de jours
        missing_days, ok_all_days = check_missing_days(
            daily_df,
            start_dt_local=start_date,
            end_dt_local=end_date,
        )

        if ok_all_days:
            st.success("✅ Toutes les dates sont couvertes après agrégation journalière.")
        else:
            st.warning(
                "⚠ Certaines dates n'ont pas de valeur retenue : "
                + ", ".join(str(d) for d in missing_days)
            )

        # Graph température journalière
        if daily_df["temperature_C"].notna().any():
            fig_temp_day = px.line(
                daily_df,
                x="jour_local",
                y="temperature_C",
                markers=True,
                title=f"Température journalière (°C) ~{heure_cible}h",
            )
            fig_temp_day.update_layout(
                xaxis_title="Jour (Europe/Paris)",
                yaxis_title="°C",
            )
            st.plotly_chart(fig_temp_day, use_container_width=True)

        # Graph pluie journalière
        if daily_df["rain_mm_1h"].notna().any():
            fig_rain_day = px.bar(
                daily_df,
                x="jour_local",
                y="rain_mm_1h",
                title="Pluie mesurée à l'heure retenue (mm/h)",
            )
            fig_rain_day.update_layout(
                xaxis_title="Jour (Europe/Paris)",
                yaxis_title="mm / h",
            )
            st.plotly_chart(fig_rain_day, use_container_width=True)

        # Graph vent journalière
        if daily_df["wind_ms"].notna().any():
            fig_wind_day = px.line(
                daily_df,
                x="jour_local",
                y="wind_ms",
                markers=True,
                title="Vent moyen (m/s) à l'heure retenue",
            )
            fig_wind_day.update_layout(
                xaxis_title="Jour (Europe/Paris)",
                yaxis_title="m/s",
            )
            st.plotly_chart(fig_wind_day, use_container_width=True)

        # Export Excel des données agrégées jour
        excel_bytes = to_excel_bytes(daily_df)
        st.download_button(
            label="⬇ Télécharger l'Excel (1 ligne / jour)",
            data=excel_bytes,
            file_name=f"meteo_{chosen_station_id}_{start_date}_to_{end_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

else:
    st.info("➡ Sélectionne ta plage, ton heure cible, puis clique sur 'Charger les données'.")
