from flask import Flask, request, jsonify, render_template, redirect
import psycopg2
import random
from datetime import datetime
from confluent_kafka import SerializingProducer
import json

app = Flask(__name__)

# Database connection settings
def get_db_connection():
    return psycopg2.connect(host="localhost", database="voting", user="postgres", password="postgres")

# Product mapping: predefined product list with IDs
PRODUCTS = {
    1: "Wireless Mouse",
    2: "Keyboard",
    3: "Monitor",
    4: "Laptop",
    5: "Headphones",
    6: "Webcam",
    7: "External Hard Drive",
    8: "Smartphone",
    9: "Tablet",
    10: "Charger",
    11: "Smartwatch",
    12: "Gaming Console",
    13: "Desk Lamp",
    14: "USB Cable",
    15: "Speakers",
    16: "Router",
    17: "Projector",
    18: "Graphic Tablet",
    19: "Microphone",
    20: "Power Bank"
}

def delivery_report(err, msg):
    """ Callback for Kafka producer delivery reports """
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Endpoint to update sales data
@app.route('/buy', methods=['POST'])
def buy_product():
    try:
        # Get form data
        data = request.form
        product_name = data['product_name']
        quantity = int(data['quantity'])
        state = data['state']
        city = data['city']
        branch = data['branch']
        shopping_experience = data['shopping_experience']
        payment_method = data['payment_method']
        total_amount = data['total_amount']

        # Validate data
        if not all([product_name, quantity, state, city, branch, shopping_experience, payment_method, total_amount]):
            return jsonify({"error": "All fields are required"}), 400

        # Map product name to product ID
        product_id = None
        for pid, pname in PRODUCTS.items():
            if pname == product_name:
                product_id = pid
                break

        if not product_id:
            return jsonify({"error": "Invalid product name"}), 400

        # Assign a random customer ID
        customer_id = random.randint(1, 20)

        # Extract timestamp, date, time, month, and year
        current_timestamp = datetime.now()
        current_date = current_timestamp.date()
        current_time = current_timestamp.time()
        current_month = current_timestamp.month
        current_year = current_timestamp.year

        # Insert data into PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE if not exists sales(
                id SERIAL PRIMARY KEY,               
                customer_id INT NOT NULL,           
                product_id INT NOT NULL,             
                product_name VARCHAR(100) NOT NULL,  
                quantity INT NOT NULL,               
                state VARCHAR(100) NOT NULL,         
                city VARCHAR(100) NOT NULL,          
                branch VARCHAR(100) NOT NULL,       
                timestamp TIMESTAMP DEFAULT NOW(),   
                date DATE,                           
                time TIME,                           
                month INT,                           
                year INT,                            
                shopping_experience TEXT NOT NULL,   
                payment_method VARCHAR(50),          
                total_amount NUMERIC(10, 2)          
        ); 
        """)
        conn.commit()
        query = """
        INSERT INTO sales (
            customer_id, product_id, product_name, quantity, state, city, branch, timestamp, date, time, month, year,
            shopping_experience, payment_method, total_amount
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            customer_id, product_id, product_name, quantity, state, city, branch, current_timestamp, current_date,
            current_time, current_month, current_year, shopping_experience, payment_method, total_amount
        ))
        conn.commit()
        cursor.close()
        conn.close()

        # Constructing customer and sales data
        voter_data = {
            "customer_id": customer_id,
            "product_id": product_id,
            "product_name": product_name,
            "quantity": quantity,
            "state": state,
            "city": city,
            "branch": branch,
            "timestamp": current_timestamp.isoformat(),
            "date": str(current_date),
            "time": str(current_time),
            "month": current_month,
            "year": current_year,
            "shopping_experience": shopping_experience,
            "payment_method": payment_method,
            "total_amount": total_amount
        }

        producer = SerializingProducer({'bootstrap.servers':'localhost:9092'})


        producer.produce(
            "sales_info",
            key=str(customer_id),
            value=json.dumps(voter_data),
            on_delivery=delivery_report
        )
        producer.flush()

        return redirect('/')

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Serve the input page
@app.route('/')
def input_page():
    return render_template('input_form.html', products=PRODUCTS)

if __name__ == '__main__':
    app.run(debug=True)
