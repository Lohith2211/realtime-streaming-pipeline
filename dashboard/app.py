import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(page_title="Live Sales", page_icon="📈", layout="wide")
st.title("📈 Real-Time Sales Dashboard")
st.caption("Kafka → Spark Structured Streaming → PostgreSQL → Streamlit")

# Spark's "update" mode writes several rows per window as counts grow,
# so keep the most complete row per window + category.
QUERY = """
SELECT DISTINCT ON (window_start, category)
       window_start, category, total_sales, order_count
FROM sales_by_minute
ORDER BY window_start, category, order_count DESC
"""

@st.cache_data(ttl=3)
def load_data():
    conn = psycopg2.connect(host="127.0.0.1", port=5433, dbname="sales_db",
                        user="de_user", password="de_pass")
    df = pd.read_sql(QUERY, conn)
    conn.close()
    return df

# ---------- Sidebar filters ----------
with st.sidebar:
    st.header("Filters")
    minutes = st.slider("Time range (minutes)", 5, 60, 15)
    all_cats = ["electronics", "fashion", "grocery", "books", "toys"]
    cats = st.multiselect("Categories", all_cats, default=all_cats)
    st.caption("Auto-refreshes every 5 seconds")

# ---------- Live section ----------
@st.fragment(run_every=5)
def live():
    df = load_data()
    if df.empty:
        st.info("Waiting for data... make sure the producer and Spark job are running.")
        return

    latest = df.window_start.max()
    df = df[(df.window_start >= latest - pd.Timedelta(minutes=minutes))
            & (df.category.isin(cats))]
    if df.empty:
        st.warning("No data for the selected filters.")
        return

    total = df.total_sales.sum()
    orders = int(df.order_count.sum())
    by_cat = df.groupby("category").agg(sales=("total_sales", "sum"),
                                        orders=("order_count", "sum")).reset_index()
    top = by_cat.loc[by_cat.sales.idxmax(), "category"]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total sales", f"${total:,.2f}")
    k2.metric("Orders", f"{orders:,}")
    k3.metric("Avg order value", f"${total / max(orders, 1):,.2f}")
    k4.metric("Top category", top.title())

    c1, c2 = st.columns([2, 1])
    with c1:
        st.subheader("Sales per minute")
        fig = px.line(df.sort_values("window_start"), x="window_start",
                      y="total_sales", color="category", markers=True,
                      labels={"window_start": "Time", "total_sales": "Sales ($)"})
        fig.update_layout(height=380, margin=dict(l=0, r=0, t=10, b=0),
                          legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.subheader("Order share")
        fig = px.pie(by_cat, names="category", values="orders", hole=0.5)
        fig.update_layout(height=380, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Sales by category")
    fig = px.bar(by_cat.sort_values("sales", ascending=False), x="category",
                 y="sales", color="category", text_auto=".2s",
                 labels={"sales": "Sales ($)", "category": ""})
    fig.update_layout(height=320, showlegend=False, margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Raw data"):
        st.dataframe(df.sort_values("window_start", ascending=False),
                     use_container_width=True)

    st.caption(f"Latest window: {latest:%Y-%m-%d %H:%M} UTC")

live()