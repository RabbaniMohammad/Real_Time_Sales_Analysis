<h1>Real Time Sales Analysis</h1>

<h3>Overview</h3>
Real Time Sales Analysis is a comprehensive project designed to monitor, analyze, and visualize sales data in real time. This project leverages modern data engineering and analytics technologies to provide insightful dashboards, enabling businesses to make informed decisions based on real-time data streams.

<h3>Technologies Used</h3>
Docker: For containerization of services and environments, ensuring seamless deployment.
PostgreSQL (PSQL): For persisting processed sales data in a relational database.
Zookeeper: For managing distributed systems and coordinating Kafka brokers.
Kafka: For real-time data streaming and ensuring high-throughput data pipelines.
PySpark: For batch processing, transformations, and aggregations of sales data.
Streamlit: For building dynamic, real-time dashboards and visualizations.
Flask: For managing sales data entry via a user-friendly web interface.

<h3>Key Concepts</h3>
1. Real-Time Data Streaming
Sales data from the Flask web app is pushed into a Kafka topic (sales_info).
A Kafka consumer continuously polls this topic, processes incoming records, and pushes the processed results to another Kafka topic (processed_sales_info).

2. Batch Processing and Transformations
A PySpark job consumes data from Kafka in batches.
Data is cleaned (removal of null values), enriched with computed fields (e.g., timestamps, aggregations), and stored in a PostgreSQL database.

3. Real-Time Visualization
A Streamlit app reads processed data from PostgreSQL.

Dashboards provide key insights such as:
Most positively reviewed product.
Products with the most negative feedback.
Best and worst-performing branches.
Time-series trends of sales volume.
A manual refresh button and auto-refresh functionality ensure data stays updated.

<h3>4. Interactive Sales Data Entry</h3>
A Flask app serves a user-friendly web page for entering sales data.
Users can input details such as product name, quantity, location, and shopping experience.
Each entry is assigned a unique customer_id and timestamp, ensuring traceability.

<h3>5. Data Lineage Tracking</h3>
Comprehensive tracking from data ingestion (Kafka) to final visualization (Streamlit).
Ensures no data loss or discrepancies between the source and the database.

<h3>Project Structure</h3>
automation.py: Simulates automated data generation for testing.
batch_processing.py: Handles PySpark batch processing jobs.
batch_streamlit.py: Streamlit-based real-time visualization app.
main.py: Flask app for manual data entry and integration with Kafka.
last_processed.txt: Tracks the last processed record for CDC (Change Data Capture).
templates/: Contains HTML files for the Flask front end.
requirements.txt: Lists Python dependencies for the project.

docker-compose.yml: Defines Docker services for Kafka, Zookeeper, PostgreSQL, and other components.

<h3>Features</h3>
Dynamic Sales Dashboards:
Visualizations updated every minute.
Filters for date range, branch, and product.
User-Friendly Sales Entry:
Predefined product options for consistency.
Automatic timestamping and data validation.

<h3>Automated Data Processing:</h3>
Real-time Kafka pipelines with PySpark transformations.
Seamless integration with PostgreSQL for historical data storage.

<h3>Scalability and Portability:</h3>
Dockerized environment for easy deployment across systems.
Kafka ensures the system can handle high throughput.
Key Insights Provided by the Project
Top Performing Products: Identifies products with the highest positive feedback.
Worst Reviewed Products: Highlights products with consistent negative feedback.

<h3>Branch Performance:</h3>
Best-performing branches based on sales and sentiment.
Branches with low customer satisfaction for targeted improvements.

Revenue Trends: Tracks total sales and revenue over time.
Customer Experience: Analyzes textual feedback to derive actionable insights.

Usage Instructions
Running the Project Locally

<h3>Clone the Repository:</h3>
<p>git clone <repository_url></p>
<p>cd Real-Time-Sales-Analysis</p>
  
<h3>Build and Start Docker Containers:</h3>
<p>docker-compose up --build</p>

<h3>Install Python Dependencies:</h3>
<p>pip install -r requirements.txt</p>

<h3>Run Flask for Data Entry:</h3>
<p>python main.py</p>

<h3>Run Streamlit for Visualization:</h3>
<p>streamlit run batch_streamlit.py</p>

<h3>Future Enhancements</h3>
Integration with cloud storage for scalability.
Advanced analytics using machine learning (e.g., sentiment prediction).
Notifications for real-time anomalies (e.g., spikes in negative feedback).

Enhanced security with OAuth for data entry and visualization apps.

Feel free to contribute by submitting pull requests or reporting issues. Thank you for exploring Real Time Sales Analysis!

