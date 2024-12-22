import streamlit as st
import pandas as pd
import psycopg2
import time
import matplotlib.pyplot as plt
from streamlit_autorefresh import st_autorefresh

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "database": "voting",
    "user": "postgres",
    "password": "postgres"
}

# Function to fetch data
def fetch_data(query):
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            records = cursor.fetchall()
            return pd.DataFrame(records, columns=columns)
    finally:
        conn.close()

# Function to fetch required analysis
def fetch_analysis_data():
    queries = {
        "positive_products": """
            SELECT product_name, COUNT(*) AS positive_count
            FROM sales_data
            WHERE sentiment_result = 'positive'
            GROUP BY product_name
            ORDER BY positive_count DESC;
        """,
        "negative_products": """
            SELECT product_name, COUNT(*) AS negative_count
            FROM sales_data
            WHERE sentiment_result = 'negative'
            GROUP BY product_name
            ORDER BY negative_count DESC;
        """,
        "branch_sentiment": """
            SELECT branch, 
                COUNT(CASE WHEN sentiment_result = 'positive' THEN 1 END) AS positive_count,
                COUNT(CASE WHEN sentiment_result = 'negative' THEN 1 END) AS negative_count
            FROM sales_data
            GROUP BY branch;
        """,
        "state_sentiment": """
            SELECT state, 
                COUNT(CASE WHEN sentiment_result = 'positive' THEN 1 END) AS positive_count,
                COUNT(CASE WHEN sentiment_result = 'negative' THEN 1 END) AS negative_count,
                COUNT(CASE WHEN sentiment_result = 'na' THEN 1 END) AS neutral_count
            FROM sales_data
            GROUP BY state;
        """,
        "product_trends": """
            SELECT product_name, sentiment_result, COUNT(*) AS count
            FROM sales_data
            GROUP BY product_name, sentiment_result
            ORDER BY product_name, sentiment_result;
        """
    }

    return {key: fetch_data(query) for key, query in queries.items()}

# Streamlit App
def main():
    st.title("Sales Sentiment Analysis")
    st.write("This dashboard auto-refreshes every 60 seconds or on demand.")

    st.session_state["data"] = fetch_analysis_data()

    data = st.session_state["data"]

    # Display last updated time
    st.sidebar.write(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Visualization
    st.subheader("Most Positively Reviewed Products")
    st.bar_chart(data["positive_products"].set_index("product_name")["positive_count"])

    st.subheader("Most Negatively Reviewed Products")
    st.bar_chart(data["negative_products"].set_index("product_name")["negative_count"])

    st.subheader("Branch Sentiment Analysis")
    st.bar_chart(data["branch_sentiment"].set_index("branch")[["positive_count", "negative_count"]])

    st.subheader("State Sentiment Distribution")
    st.bar_chart(data["state_sentiment"].set_index("state")[["positive_count", "negative_count", "neutral_count"]])

    st.subheader("Product Sentiment Trends")
    st.write(data["product_trends"])


if __name__ == "__main__":
    # Auto-refresh interval slider
    st.sidebar.header("Controls")
    refresh_interval = st.sidebar.slider("Auto-refresh interval (seconds)", 10, 120, 60)
    st_autorefresh(interval=refresh_interval * 1000, key="data-refresh")
    main()
