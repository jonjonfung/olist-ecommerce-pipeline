import streamlit as st
import pandas as pd
import plotly.express as px
import awswrangler as wr
import os

# Load AWS credentials from Streamlit secrets
os.environ["AWS_ACCESS_KEY_ID"] = st.secrets["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = st.secrets["AWS_SECRET_ACCESS_KEY"]
os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

st.set_page_config(
    page_title="Olist E-Commerce Dashboard",
    page_icon="🛒",
    layout="wide"
)

st.title("🛒 Olist E-Commerce Pipeline Dashboard")
st.markdown("Built with AWS S3 + Glue + Athena + Streamlit")

@st.cache_data
def load_data(query):
    return wr.athena.read_sql_query(
        query,
        database="olist_silver_db",
        s3_output="s3://olist-ecommerce-pipeline-john/output/"
    )

# ---- KPI METRICS ----
st.subheader("📊 Key Metrics")
col1, col2, col3, col4 = st.columns(4)

orders_df = load_data("SELECT COUNT(order_id) as total_orders FROM orders")
revenue_df = load_data("SELECT ROUND(SUM(price), 2) as total_revenue FROM order_items")
customers_df = load_data("SELECT COUNT(DISTINCT customer_id) as total_customers FROM customers")
sellers_df = load_data("SELECT COUNT(DISTINCT seller_id) as total_sellers FROM sellers")

col1.metric("Total Orders", f"{orders_df['total_orders'][0]:,}")
col2.metric("Total Revenue", f"R$ {revenue_df['total_revenue'][0]:,.2f}")
col3.metric("Total Customers", f"{customers_df['total_customers'][0]:,}")
col4.metric("Total Sellers", f"{sellers_df['total_sellers'][0]:,}")

# ---- MONTHLY REVENUE ----
st.subheader("📈 Monthly Revenue Trend")
monthly_df = load_data("SELECT * FROM monthly_revenue ORDER BY year_month")
fig1 = px.line(monthly_df, x="year_month", y="total_revenue",
               title="Monthly Revenue", markers=True,
               color_discrete_sequence=["#FF6B35"])
st.plotly_chart(fig1, use_container_width=True)

# ---- TWO CHARTS SIDE BY SIDE ----
col1, col2 = st.columns(2)

with col1:
    st.subheader("🏆 Top 10 Categories by Revenue")
    cat_df = load_data("SELECT * FROM revenue_by_category ORDER BY total_revenue DESC LIMIT 10")
    fig2 = px.bar(cat_df, x="total_revenue", y="product_category_name",
                  orientation="h", color="total_revenue",
                  color_continuous_scale="Oranges")
    st.plotly_chart(fig2, use_container_width=True)

with col2:
    st.subheader("💳 Payment Method Breakdown")
    pay_df = load_data("SELECT * FROM payment_method_breakdown")
    fig3 = px.pie(pay_df, values="total_orders", names="payment_type",
                  color_discrete_sequence=px.colors.sequential.Oranges_r)
    st.plotly_chart(fig3, use_container_width=True)

# ---- DELIVERY TIME ----
st.subheader("🚚 Average Delivery Time by State")
delivery_df = load_data("SELECT * FROM avg_delivery_time_by_state ORDER BY avg_delivery_days")
fig4 = px.bar(delivery_df, x="customer_state", y="avg_delivery_days",
              color="avg_delivery_days", color_continuous_scale="RdYlGn_r",
              title="Average Delivery Days by State")
st.plotly_chart(fig4, use_container_width=True)

st.markdown("---")
st.markdown("Built by Jonathan Fung | AWS S3 + Glue + Athena + Streamlit")
