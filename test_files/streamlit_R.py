import streamlit as st
from kafka import KafkaConsumer, TopicPartition
import pandas as pd
import json
import altair as alt

# Streamlit Config
st.set_page_config(layout="wide")
st.title("Real-Time Sales Analytics")

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "aggregated_analysis_data"
PARTITION_NAME = "total_sales_per_location"  # Specific partition name


def fetch_partition_data(topic, partition):
    """
    Fetches all data from a specific Kafka partition and stops when done.
    """
    # Create Kafka Consumer
    print("we are here")
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    print("we are outside")
    # Assign the specific topic and partition
    tp = TopicPartition(topic, partition)
    consumer.assign([tp])
    print("This is the tp", tp)
    # Get end offset for the partition
    consumer.seek_to_end(tp)
    end_offset = consumer.position(tp)

    # Rewind to the beginning of the partition
    consumer.seek_to_beginning(tp)
    print("This is the consumer", consumer)
    # Fetch data until end offset
    data = []
    for message in consumer:
        print("we are inside the for loop")
        data.append(message.value)
        print(f"Consumed message: {message.value}")  # Debugging

        # Stop when reaching the end offset
        if message.offset + 1 == end_offset:
            print(f"End of partition {partition} reached.")
            break
    print("This is the data",data)
    consumer.close()
    return pd.DataFrame(data)



# Fetch data when the button is clicked
if st.button("Fetch Data"):
    st.write("Fetching data from Kafka partition...")
    df = fetch_partition_data(KAFKA_TOPIC, PARTITION_NAME)  # Assuming partition 0 for simplicity
    st.success("Data fetched successfully!")

    # Display DataFrame
    st.subheader("Data Table")
    st.dataframe(df)

    # Visualize Data
    if not df.empty:
        st.subheader("Visualization: Total Sales Per Location")
        chart = alt.Chart(df).mark_bar().encode(
            x="state:N",
            y="total_sales:Q",
            color="state:N",
            tooltip=["state", "city", "branch", "total_sales"]
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No data available for visualization.")
