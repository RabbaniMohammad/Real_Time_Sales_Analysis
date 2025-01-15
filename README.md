<!DOCTYPE html>
<html>
<head>
</head>
<body>

<h1>Real Walmart Sales Analysis</h1>

<h2>Overview</h2>
<p>
The <b>Real Walmart Sales Analysis</b> project is designed to process, analyze, and visualize Walmart sales data 
in real-time and batch modes. The system integrates multiple technologies for data ingestion, storage, processing, 
and visualization, ensuring scalability and performance. The architecture leverages a metadata-driven approach 
for flexibility and efficiency.
</p>

<h2>Technologies Used</h2>

<h3>Real-Time Processing</h3>
<ul>
    <li><b>Flask:</b> API creation for exposing sales data.</li>
    <li><b>Selenium:</b> Automates the process of interacting with the sales API.</li>
    <li><b>Kafka:</b> Facilitates real-time data ingestion.</li>
    <li><b>PostgreSQL:</b> Stores ingested data for further processing.</li>
    <li><b>Apache Spark:</b> Processes real-time data streams.</li>
    <li><b>Streamlit:</b> Provides an interactive dashboard for real-time data visualization.</li>
</ul>

<h3>Batch Processing</h3>
<ul>
    <li><b>PySpark:</b> Efficient batch data processing.</li>
    <li><b>PostgreSQL:</b> Centralized data storage.</li>
    <li><b>Streamlit:</b> Batch data visualization.</li>
</ul>

<h3>Additional Features</h3>
<ul>
    <li><b>Metadata-Driven Architecture:</b> Enables efficient and flexible data handling.</li>
    <li><b>Testing:</b> Implemented with the assert library to ensure data integrity and application functionality.</li>
</ul>

<h2>System Architecture</h2>

<h3>Real-Time Pipeline:</h3>
<ul>
    <li>Flask API serves sales data.</li>
    <li>Selenium automates data retrieval from the sales API.</li>
    <li>Kafka ingests the data into real-time processing pipelines.</li>
    <li>Spark processes real-time data streams and writes results to PostgreSQL.</li>
    <li>Streamlit visualizes real-time data insights.</li>
</ul>

<h3>Batch Processing Pipeline:</h3>
<ul>
    <li>PySpark processes large batches of sales data stored in PostgreSQL.</li>
    <li>Streamlit visualizes historical trends and aggregated insights.</li>
</ul>

<h2>Setup and Execution</h2>

<h3>Prerequisites</h3>
<p>Ensure the following are installed:</p>
<ul>
    <li>Python 3.8+</li>
    <li>Kafka</li>
    <li>PostgreSQL</li>
    <li>Apache Spark</li>
    <li>Streamlit</li>
    <li>Selenium with ChromeDriver</li>
</ul>

<h3>Installation Steps</h3>
<ol>
    <li>Clone the repository:
        <pre><code>git clone https://github.com/yourusername/real-walmart-sales-analysis.git
cd real-walmart-sales-analysis
        </code></pre>
    </li>
    <li>Install dependencies:
        <pre><code>pip install -r requirements.txt</code></pre>
    </li>
    <li>Start PostgreSQL and configure the database.</li>
    <li>Start Kafka:
        <pre><code>
# Start Kafka server
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
        </code></pre>
    </li>
    <li>Initialize Spark:
        <pre><code>spark-submit --master local app.py</code></pre>
    </li>
    <li>Run the Flask API:
        <pre><code>python app.py</code></pre>
    </li>
    <li>Start Selenium automation:
        <pre><code>python selenium_automation.py</code></pre>
    </li>
    <li>Launch the Streamlit dashboard:
        <pre><code>streamlit run dashboard.py</code></pre>
    </li>
</ol>

<h2>Contributors</h2>
<p><b>Rabbani Mohammad</b> - Developer and Architect</p>

</body>
</html>
