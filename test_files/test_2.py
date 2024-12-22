from confluent_kafka import Consumer, KafkaException
import json
import pandas as pd
import streamlit as st
import altair as alt
from streamlit_autorefresh import st_autorefresh

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'key-specific-consumer-group',
    'auto.offset.reset': 'earliest',  # Start from the earliest message for the first time
    'enable.auto.commit': True  # Let Kafka handle committing offsets
}

# Topic and Key
TOPIC = "aggregated_analysis_data"

# Streamlit setup
st.set_page_config(layout="wide")
st.title("Real-Time Sales Analytics")


# Persistent Kafka Consumer

def get_kafka_consumer():
    return Consumer(KAFKA_CONFIG)


def fetchData(consumer, topic, key):
    """
    Fetch Kafka messages for a specific key.
    """
    data = []
    try:
        consumer.subscribe([topic])
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
            msg_key = msg.key().decode("utf-8")
            if msg_key == key:
                data.append(value)
    except Exception as e:
        st.error(f"Error fetching data: {e}")
    return pd.DataFrame(data)


def fetchAndVisualize():
    consumer = get_kafka_consumer()

    # Fetch data for "top_selling_products"
    df = fetchData(consumer, TOPIC, "top_selling_products")
    st.subheader("Data Table: Top Selling Products")
    st.dataframe(df)

    # Visualization: Bar Chart
    if not df.empty:
        st.subheader("Top Selling Products")
        chart = alt.Chart(df).mark_bar().encode(
            x='state:N',
            y='total_sold:Q',
            color='state:N',
            tooltip=['state', 'city', 'branch', 'total_sold']
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No data available for visualization.")

    # Fetch data for "top_products_by_revenue"
    revenue_df = fetchData(consumer, TOPIC, "top_products_by_revenue")
    st.subheader("Data Table: Top Products by Revenue")
    st.dataframe(revenue_df)

    # Visualization: Revenue Chart
    if not revenue_df.empty:
        st.subheader("Top Products by Revenue")
        chart_revenue = alt.Chart(revenue_df).mark_bar().encode(
            x='product_name:N',
            y='total_revenue:Q',
            color='product_name:N',
            tooltip=['product_name', 'total_revenue']
        )
        st.altair_chart(chart_revenue, use_container_width=True)
    else:
        st.warning("No data available for revenue visualization.")


# Sidebar with Refresh Button
if st.sidebar.button("Refresh Data"):
    fetchAndVisualize()

