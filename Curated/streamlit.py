import json
import pandas as pd
from confluent_kafka import Consumer, KafkaException
import streamlit as st
import altair as alt
from streamlit_autorefresh import st_autorefresh

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'data-segregation-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

# Initialize Consumer
consumer = Consumer(conf)
TOPIC = "aggregated_analysis_data"

# Define keys for segregation
KEYS = [
    "total_sales_per_location",
    "top_products_by_revenue",
    "customer_purchase_trends",
    "payment_method_distribution",
    "avg_customer_spending",
    "top_selling_products"
]

# Function to fetch all data and segregate into DataFrames
def fetch_and_segregate_data():
    try:
        consumer.subscribe([TOPIC])

        # Dictionary to store data for each key
        data_dict = {key: [] for key in KEYS}

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break  # No more messages
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            value = json.loads(msg.value().decode("utf-8"))
            key = msg.key().decode("utf-8")

            if key in KEYS:
                data_dict[key].append(value)

        # Convert each key's data to a DataFrame
        dataframes = {}
        for key, records in data_dict.items():
            if records:  # Only create a DataFrame if data exists for the key
                dataframes[key] = pd.DataFrame(records)
            else:
                dataframes[key] = pd.DataFrame()

        return dataframes

    finally:
        consumer.close()

# Streamlit Setup
st.set_page_config(layout="wide")
st.title("Real-Time Sales Analytics")

# Auto-refresh interval slider
refresh_interval = st.sidebar.slider("Auto-refresh interval (seconds)", 1, 10, 5)
st_autorefresh(interval=refresh_interval * 1000, key="data-refresh")

# Fetch and segregate data
dataframes = fetch_and_segregate_data()

# Visualization functions
def visualize_total_sales(df):
    print("This is the df from the total sales", df)
    if not df.empty:
        chart = alt.Chart(df).mark_bar().encode(
            x="state:N",
            y="total_sales:Q",
            color="state:N",
            tooltip=["state", "city", "branch", "total_sales"]
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.write("No data available for Total Sales Per Location.")

def visualize_top_products(df):
    if not df.empty:
        chart = alt.Chart(df).mark_bar().encode(
            x="product_name:N",
            y="total_revenue:Q",
            color="product_name:N",
            tooltip=["product_name", "total_revenue"]
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.write("No data available for Top Products by Revenue.")

def visualize_customer_trends(df):
    if not df.empty:
        df["start_time"] = pd.to_datetime(df["window"].apply(lambda x: x['start']))
        chart = alt.Chart(df).mark_line().encode(
            x="start_time:T",
            y="total_revenue:Q",
            color=alt.value("blue"),
            tooltip=["start_time", "total_revenue", "total_quantity"]
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.write("No data available for Customer Purchase Trends.")

def visualize_payment_distribution(df):
    if not df.empty:
        print(df)
        chart = alt.Chart(df).mark_arc().encode(
            theta=alt.Theta(field="total_revenue", type="quantitative"),
            color=alt.Color(field="payment_method", type="nominal"),
            tooltip=["payment_method", "total_revenue"]
        ).properties(
            title="Payment Method Distribution"
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.write("No data available for Payment Method Distribution.")


def visualize_avg_spending(df):
    if not df.empty:
        chart = alt.Chart(df).mark_bar().encode(
            x="state:N",
            y="avg_spending_per_item:Q",
            color="state:N",
            tooltip=["state", "city", "branch", "avg_spending_per_item"]
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.write("No data available for Average Customer Spending.")

def visualize_top_selling_products(df):
    if not df.empty:
        chart = alt.Chart(df).mark_bar().encode(
            x="product_name:N",
            y="total_sold:Q",
            color="product_name:N",
            tooltip=["state", "city", "branch", "product_name", "total_sold"]
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.write("No data available for Top Selling Products.")

# Display Data and Visualizations
if st.button("Fetch Latest Data"):
    dataframes = fetch_and_segregate_data()

# Create columns for visualizations
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Total Sales Per Location")
    visualize_total_sales(dataframes["total_sales_per_location"])

with col2:
    st.subheader("Top Products by Revenue")
    visualize_top_products(dataframes["top_products_by_revenue"])

with col3:
    st.subheader("Customer Purchase Trends")
    visualize_customer_trends(dataframes["customer_purchase_trends"])

col4, col5 = st.columns(2)

with col4:
    st.subheader("Payment Method Distribution")
    visualize_payment_distribution(dataframes["payment_method_distribution"])

with col5:
    st.subheader("Average Customer Spending")
    visualize_avg_spending(dataframes["avg_customer_spending"])

st.subheader("Top Selling Products")
visualize_top_selling_products(dataframes["top_selling_products"])
