import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text

# ----------------------------
# Page config
# ----------------------------
st.set_page_config(
    page_title="Taxi Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ----------------------------
# Dark premium CSS
# ----------------------------
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(135deg, #0b1020 0%, #111827 45%, #0f172a 100%);
        color: #e5e7eb;
    }

    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a 0%, #111827 100%);
        border-right: 1px solid rgba(255,255,255,0.08);
    }

    .main-title {
        font-size: 48px;
        font-weight: 800;
        color: #f9fafb;
        margin-bottom: 4px;
    }

    .subtitle {
        font-size: 18px;
        color: #9ca3af;
        margin-bottom: 24px;
    }

    .section-title {
        font-size: 24px;
        font-weight: 700;
        color: #f3f4f6;
        margin-bottom: 10px;
    }

    .metric-card {
        background: linear-gradient(180deg, rgba(30,41,59,0.95) 0%, rgba(17,24,39,0.95) 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 20px;
        padding: 22px 18px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.30);
        text-align: center;
        margin-bottom: 12px;
    }

    .metric-label {
        font-size: 16px;
        color: #9ca3af;
        margin-bottom: 8px;
    }

    .metric-value {
        font-size: 34px;
        font-weight: 800;
        color: #f9fafb;
    }

    .chart-card, .table-card {
        background: linear-gradient(180deg, rgba(30,41,59,0.92) 0%, rgba(17,24,39,0.92) 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 20px;
        padding: 18px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.30);
        margin-bottom: 20px;
    }

    div[data-testid="stMetric"] {
        background: transparent;
    }

    hr {
        border: none;
        height: 1px;
        background: rgba(255,255,255,0.08);
        margin: 24px 0;
    }
</style>
""", unsafe_allow_html=True)

# ----------------------------
# Header
# ----------------------------
st.markdown('<div class="main-title">Taxi Analytics Dashboard</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="subtitle">Premium dark analytics dashboard for the NYC batch-processing taxi project</div>',
    unsafe_allow_html=True
)

# ----------------------------
# Database connection
# ----------------------------
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "taxi_db"

@st.cache_resource
def get_engine():
    connection_string = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(connection_string)

engine = get_engine()

# ----------------------------
# Get min/max dates
# ----------------------------
@st.cache_data
def get_date_bounds():
    query = """
    SELECT
        MIN(DATE(pickup_datetime)) AS min_date,
        MAX(DATE(pickup_datetime)) AS max_date
    FROM public.taxi_trips;
    """
    return pd.read_sql(query, engine)

date_bounds = get_date_bounds()
min_date = pd.to_datetime(date_bounds.loc[0, "min_date"]).date()
max_date = pd.to_datetime(date_bounds.loc[0, "max_date"]).date()

# ----------------------------
# Sidebar filters
# ----------------------------
st.sidebar.markdown("## Filters")
date_range = st.sidebar.date_input(
    "Pickup date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

max_distance = st.sidebar.slider(
    "Maximum trip distance (miles)",
    min_value=5,
    max_value=50,
    value=30,
    step=1
)

max_fare = st.sidebar.slider(
    "Maximum fare amount ($)",
    min_value=20,
    max_value=300,
    value=150,
    step=5
)

st.sidebar.markdown("---")
st.sidebar.write(f"Selected start date: **{start_date}**")
st.sidebar.write(f"Selected end date: **{end_date}**")

# ----------------------------
# Queries with filters
# ----------------------------
kpi_query = text("""
SELECT
    COUNT(*) AS total_trips,
    ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare,
    ROUND(AVG(trip_distance)::numeric, 2) AS avg_distance,
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue
FROM public.taxi_trips
WHERE DATE(pickup_datetime) BETWEEN :start_date AND :end_date;
""")

hourly_query = text("""
SELECT
    EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
    COUNT(*) AS trip_count
FROM public.taxi_trips
WHERE DATE(pickup_datetime) BETWEEN :start_date AND :end_date
GROUP BY pickup_hour
ORDER BY pickup_hour;
""")

daily_trend_query = text("""
SELECT
    DATE(pickup_datetime) AS trip_date,
    COUNT(*) AS trip_count
FROM public.taxi_trips
WHERE DATE(pickup_datetime) BETWEEN :start_date AND :end_date
GROUP BY trip_date
ORDER BY trip_date;
""")

passenger_query = text("""
SELECT
    passenger_count,
    COUNT(*) AS trip_count
FROM public.taxi_trips
WHERE passenger_count IS NOT NULL
  AND DATE(pickup_datetime) BETWEEN :start_date AND :end_date
GROUP BY passenger_count
ORDER BY passenger_count;
""")

distance_query = text("""
SELECT trip_distance
FROM public.taxi_trips
WHERE trip_distance IS NOT NULL
  AND trip_distance >= 0
  AND trip_distance <= :max_distance
  AND DATE(pickup_datetime) BETWEEN :start_date AND :end_date;
""")

fare_distance_query = text("""
SELECT trip_distance, fare_amount
FROM public.taxi_trips
WHERE trip_distance IS NOT NULL
  AND fare_amount IS NOT NULL
  AND trip_distance > 0
  AND fare_amount > 0
  AND trip_distance <= :max_distance
  AND fare_amount <= :max_fare
  AND DATE(pickup_datetime) BETWEEN :start_date AND :end_date
LIMIT 5000;
""")

sample_query = text("""
SELECT *
FROM public.taxi_trips
WHERE DATE(pickup_datetime) BETWEEN :start_date AND :end_date
LIMIT 20;
""")

params = {
    "start_date": start_date,
    "end_date": end_date,
    "max_distance": max_distance,
    "max_fare": max_fare
}

# ----------------------------
# Load data
# ----------------------------
try:
    kpi_df = pd.read_sql(kpi_query, engine, params=params)
    hourly_df = pd.read_sql(hourly_query, engine, params=params)
    daily_trend_df = pd.read_sql(daily_trend_query, engine, params=params)
    passenger_df = pd.read_sql(passenger_query, engine, params=params)
    distance_df = pd.read_sql(distance_query, engine, params=params)
    fare_distance_df = pd.read_sql(fare_distance_query, engine, params=params)
    sample_df = pd.read_sql(sample_query, engine, params=params)
except Exception as e:
    st.error(f"Database connection/query failed: {e}")
    st.stop()

# ----------------------------
# Handle empty result
# ----------------------------
if kpi_df.empty or pd.isna(kpi_df.loc[0, "total_trips"]) or int(kpi_df.loc[0, "total_trips"]) == 0:
    st.warning("No data found for the selected filters.")
    st.stop()

# ----------------------------
# KPI values
# ----------------------------
total_trips = int(kpi_df.loc[0, "total_trips"])
avg_fare = float(kpi_df.loc[0, "avg_fare"] or 0)
avg_distance = float(kpi_df.loc[0, "avg_distance"] or 0)
total_revenue = float(kpi_df.loc[0, "total_revenue"] or 0)

# ----------------------------
# KPI cards
# ----------------------------
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Total Trips</div>
        <div class="metric-value">{total_trips:,}</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Total Revenue</div>
        <div class="metric-value">${total_revenue:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Average Fare</div>
        <div class="metric-value">${avg_fare:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Average Distance</div>
        <div class="metric-value">{avg_distance:.2f} mi</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<hr>", unsafe_allow_html=True)

# ----------------------------
# Matplotlib dark style helper
# ----------------------------
def style_dark_axes(ax):
    ax.set_facecolor("#111827")
    for spine in ax.spines.values():
        spine.set_color("#374151")
    ax.tick_params(colors="#e5e7eb")
    ax.xaxis.label.set_color("#f3f4f6")
    ax.yaxis.label.set_color("#f3f4f6")
    ax.title.set_color("#f9fafb")
    ax.grid(color="#374151", alpha=0.35)

# ----------------------------
# Charts row 1
# ----------------------------
left, right = st.columns(2)

with left:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Trips by Pickup Hour</div>', unsafe_allow_html=True)

    fig, ax = plt.subplots(figsize=(8, 4))
    fig.patch.set_facecolor("#111827")
    ax.bar(hourly_df["pickup_hour"], hourly_df["trip_count"])
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Number of Trips")
    ax.set_title("Trips by Pickup Hour")
    style_dark_axes(ax)
    st.pyplot(fig)
    plt.close(fig)
    st.markdown('</div>', unsafe_allow_html=True)

with right:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Daily Trip Trend</div>', unsafe_allow_html=True)

    daily_trend_df["trip_date"] = pd.to_datetime(daily_trend_df["trip_date"])
    fig, ax = plt.subplots(figsize=(8, 4))
    fig.patch.set_facecolor("#111827")
    ax.plot(daily_trend_df["trip_date"], daily_trend_df["trip_count"])
    ax.set_xlabel("Date")
    ax.set_ylabel("Trips")
    ax.set_title("Daily Trip Trend")
    style_dark_axes(ax)
    plt.xticks(rotation=45)
    st.pyplot(fig)
    plt.close(fig)
    st.markdown('</div>', unsafe_allow_html=True)

# ----------------------------
# Charts row 2
# ----------------------------
left, right = st.columns(2)

with left:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Passenger Count Distribution</div>', unsafe_allow_html=True)

    fig, ax = plt.subplots(figsize=(8, 4))
    fig.patch.set_facecolor("#111827")
    ax.bar(passenger_df["passenger_count"], passenger_df["trip_count"])
    ax.set_xlabel("Passenger Count")
    ax.set_ylabel("Number of Trips")
    ax.set_title("Passenger Count Distribution")
    style_dark_axes(ax)
    st.pyplot(fig)
    plt.close(fig)
    st.markdown('</div>', unsafe_allow_html=True)

with right:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Trip Distance Distribution</div>', unsafe_allow_html=True)

    fig, ax = plt.subplots(figsize=(8, 4))
    fig.patch.set_facecolor("#111827")
    ax.hist(distance_df["trip_distance"], bins=30)
    ax.set_xlabel("Trip Distance (miles)")
    ax.set_ylabel("Frequency")
    ax.set_title("Trip Distance Distribution")
    style_dark_axes(ax)
    st.pyplot(fig)
    plt.close(fig)
    st.markdown('</div>', unsafe_allow_html=True)

# ----------------------------
# Scatter plot
# ----------------------------
st.markdown('<div class="chart-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Fare vs Distance</div>', unsafe_allow_html=True)

fig, ax = plt.subplots(figsize=(10, 5))
fig.patch.set_facecolor("#111827")
ax.scatter(
    fare_distance_df["trip_distance"],
    fare_distance_df["fare_amount"],
    alpha=0.3
)
ax.set_xlabel("Trip Distance (miles)")
ax.set_ylabel("Fare Amount ($)")
ax.set_title("Fare Amount vs Trip Distance")
style_dark_axes(ax)
st.pyplot(fig)
plt.close(fig)

st.markdown('</div>', unsafe_allow_html=True)

# ----------------------------
# Raw data table
# ----------------------------
st.markdown('<div class="table-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Raw Data Sample</div>', unsafe_allow_html=True)
st.dataframe(sample_df, use_container_width=True)
st.markdown('</div>', unsafe_allow_html=True)