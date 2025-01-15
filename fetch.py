import psycopg2
import requests
import time
import datetime

# Connection details
connection_params = {
    'dbname': 'star_schema',  
    'user': 'postgres',       
    'password': 'postgres',   
    'host': 'localhost',      
    'port': 5432              
}

flask_app_url = "http://127.0.0.1:5000/get_form_data"

# Function to get or insert a key from dimension tables
def get_or_insert_key(cursor, table, column, value):
    if not value:
        raise ValueError(f"Value for {column} in {table} is missing.")
    
    primary_key_column = f"{table[:-4]}_id"

    # Special handling for payment and shopping dimensions
    if "payment" in table:
        primary_key_column = "payment_id"
    elif "shopping" in table:
        primary_key_column = "experience_id"

    print(primary_key_column, "primary_key_column")

    # For branch_dim, check if the record exists with is_active = TRUE
    if "branch" in table:
        query = f"""
            SELECT {primary_key_column}
            FROM {table}
            WHERE {column} = %s AND is_active = TRUE
        """
        cursor.execute(query, (value,))
        result = cursor.fetchone()

        if result:
            return result[0]

        # If the branch is not active, check for the master_branch_id and is_active = TRUE
        query = f"""
            SELECT {primary_key_column}
            FROM {table}
            WHERE master_branch_id = (
                SELECT master_branch_id
                FROM {table}
                WHERE {column} = %s
            ) AND is_active = TRUE
        """
        cursor.execute(query, (value,))
        result = cursor.fetchone()

        if result:
            return result[0]

    # General case for other dimensions
    else:
        query = f"SELECT {primary_key_column} FROM {table} WHERE {column} = %s"
        cursor.execute(query, (value,))
        result = cursor.fetchone()

        if result:
            return result[0]

    # Insert a new record if no match is found
    if "shopping" in table:
        insert_query = f"INSERT INTO {table} ({column}) VALUES (%s) RETURNING experience_id"
    else:
        insert_query = f"INSERT INTO {table} ({column}) VALUES (%s) RETURNING {primary_key_column}"

    cursor.execute(insert_query, (value,))
    return cursor.fetchone()[0]

# Function to insert data into the sales_fact table
def insert_sales_fact(cursor, conn, data):
    try:
        # Validate input data
        required_fields = ["branch", "city", "state", "product_name", "quantity", "payment_method", "shopping_experience", "total_amount"]
        for field in required_fields:
            if field not in data or not data[field]:
                raise ValueError(f"Missing or invalid value for required field: {field}")

        # Get or insert dimension keys
        branch_id = get_or_insert_key(cursor, "branch_dim", "branch_name", data["branch"])
        city_id = get_or_insert_key(cursor, "city_dim", "city_name", data["city"])
        state_id = get_or_insert_key(cursor, "state_dim", "state_name", data["state"])
        product_id = get_or_insert_key(cursor, "product_dim", "product_name", data["product_name"])
        payment_method_id = get_or_insert_key(cursor, "payment_method_dim", "payment_method", data["payment_method"])
        experience_id = get_or_insert_key(cursor, "shopping_dim", "shopping_experience", data["shopping_experience"])

        # Debug information
        print(f"Resolved IDs: Branch={branch_id}, City={city_id}, State={state_id}, Product={product_id}, PaymentMethod={payment_method_id}")

        # Insert into sales_fact table
        insert_query = """
            INSERT INTO sales_fact (
                product_id, quantity, state_id, city_id, branch_id, 
                shopping_experience, payment_method, total_amount, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            product_id, int(data["quantity"]), state_id, city_id, branch_id,
            experience_id, payment_method_id, float(data["total_amount"]), datetime.datetime.now()
        ))
        conn.commit()
        print("Data successfully inserted into sales_fact.")

    except Exception as e:
        print("Error inserting into sales_fact:", e)
        conn.rollback()

# Function to connect to Flask app and fetch form data
def connFlask():
    try:
        # Send a GET request to fetch form data
        response = requests.get(flask_app_url)
        if response.status_code == 200:
            form_data = response.json()
            print("Received form data:", form_data)
            return form_data
        else:
            print("Failed to fetch form data. Status code:", response.status_code)
            return None
    except Exception as e:
        print("Error connecting to Flask:", e)
        return None

# Main function
def main():
    try:
        # Establish a database connection
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        print("Connected to the database.")

        while True:
            # Fetch form data
            form_data = connFlask()
            if form_data:  # Check if the form data is valid
                insert_sales_fact(cursor, conn, form_data)
            else:
                print("No form data received.")
            time.sleep(10)

    except Exception as e:
        print("Error in main loop:", e)

    finally:
        # Clean up database connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    while True:
        main()
        time.sleep(10)
