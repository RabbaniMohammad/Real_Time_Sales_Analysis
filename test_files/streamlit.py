import streamlit as st
from kafka import KafkaConsumer
import pandas as pd
import json
import altair as alt

st.set_page_config(layout="wide")

# Kafka Consumer Configuration

# Initialize an empty DataFrame to store 'total_sales_per_location' data
total_sales_df = pd.DataFrame(columns=["state", "city", "branch", "total_quantity", "total_sales"])
top_products_df = pd.DataFrame(columns=["product_name", "total_revenue"])
customer_trends_df = pd.DataFrame(columns=["window_start", "window_end", "total_revenue", "total_quantity"])

# Function to consume Kafka messages and update the DataFrame
def consume_data():
    global total_sales_df , top_products_df, customer_trends_df

    consumer = KafkaConsumer(
    "aggregated_sales_data",  # Topic to consume
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer = lambda x :json.loads(x.decode('utf-8')),
    key_deserializer=lambda x: x.decode("utf-8"),
    )

    for message in consumer:
        print("This is the message", message)
        if message.key == "total_sales_per_location":
            # Append new data to the DataFrame
            new_row = pd.DataFrame([message.value])
            total_sales_df = pd.concat([total_sales_df, new_row], ignore_index=True)
            print(f"Consumed: {message.value}")  # For debugging
            break
    print("outside the for loop")

        # elif message.key == "top_products_by_revenue":
        #     # Append to Top Products DataFrame
        #     new_row = pd.DataFrame([message.value])
        #     top_products_df = pd.concat([top_products_df, new_row], ignore_index=True)
        #     print(f"Consumed (Top Products): {message.value}")  # For debugging

        # elif message.key == "customer_purchase_trends":
        #     # Extract the time window and flatten the structure
        #     message.value["window_start"] = message.value["window"]["start"]
        #     message.value["window_end"] = message.value["window"]["end"]
        #     del message.value["window"]  # Remove the nested window structure
            
        #     # Append to Customer Trends DataFrame
        #     new_row = pd.DataFrame([message.value])
        #     customer_trends_df = pd.concat([customer_trends_df, new_row], ignore_index=True)
        #     print(f"Consumed (Customer Trends): {message.value}")  # Debugging
        #     break  # Process one message for now

# Streamlit App
st.title("Real-Time Sales Analytics")

# Button to fetch data from Kafka
if st.button("Fetch Data"):
    print("We are here")
    consume_data()
    st.success("Data Fetched Successfully!")

# Display DataFrame as a Table
st.subheader("Data Table: Total Sales Per Location")
# st.dataframe(total_sales_df)



#Display the barcharts and donut charts 
col1, col2, col3 = st.columns(3)
with col1:
    # Plot Graph Using Altair
    st.subheader("Total Sales Per State")
    # print("why this thing is empty", total_sales_df)
    st.dataframe(total_sales_df)
    if not total_sales_df.empty:
        chart = alt.Chart(total_sales_df).mark_bar().encode(
                x='state:N',
                y='total_sales:Q',
                color='state:N',
            tooltip=['state', 'city', 'branch', 'total_sales']
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.write("No data available yet. Click 'Fetch Data' to load data.")
    
with col2:
    # Visualization 2: Top Products by Revenue
    # st.subheader("Top Products by Revenue")
    # print("why this thing is empty", top_products_df)
    # if not top_products_df.empty:
    #     st.dataframe(top_products_df)
    #     chart_products = alt.Chart(top_products_df).mark_bar().encode(
    #         x='product_name:N',
    #         y='total_revenue:Q',
    #         color='product_name:N',
    #         tooltip=['product_name', 'total_revenue']
    #         )
    #     st.altair_chart(chart_products, use_container_width=True)
    # else:
    #     st.write("No data available yet. Click 'Fetch Data' to load data.")
    pass
with col3:
    # Visualization 3: Customer Purchase Trends
    # st.subheader("Customer Purchase Trends Over Time")
    # if not customer_trends_df.empty:
    #     customer_trends_df["window_start"] = pd.to_datetime(customer_trends_df["window_start"])
    #     chart_trends = alt.Chart(customer_trends_df).mark_line().encode(
    #         x='window_start:T',
    #         y='total_revenue:Q',
    #         color=alt.value("blue"),
    #         tooltip=['window_start', 'total_revenue', 'total_quantity']
    #     ).properties(
    #         title="Total Revenue Trends Over Time"
    #     )
    #     st.altair_chart(chart_trends, use_container_width=True)
    # else:
    #     st.write("No data for Customer Purchase Trends.")
    pass


