import psycopg2
from datetime import datetime

# Connection details
connection_params = {
    'dbname': 'star_schema',  
    'user': 'postgres',       
    'password': 'postgres',   
    'host': 'localhost',      
    'port': 5432              
}

def apply_scd_type_2(cursor, table, column, value, master_id_column, master_id):

    try:
        # Step 1: Check if the new value already exists as an active record
        query_active_record = f"""
            SELECT {table[:-4]}_id
            FROM {table}
            WHERE {column} = %s AND is_active = TRUE AND {master_id_column} = %s;
        """
        cursor.execute(query_active_record, (value, master_id))
        active_record = cursor.fetchone()

        if active_record:
            # If there's already an active record with the value, do nothing
            print(f"Active record with {column} = {value} already exists for master_id = {master_id}.")
            return

        # Step 2: Expire the old active record for the provided master_id
        update_expiry_query = f"""
            UPDATE {table}
            SET is_active = FALSE, expiry_date = %s
            WHERE {master_id_column} = %s AND is_active = TRUE;
        """
        cursor.execute(update_expiry_query, (datetime.now(), master_id))
        print(f"Expired old active record for master_id = {master_id}.")

        # Step 3: Insert the new record with the provided master_id
        insert_new_record_query = f"""
            INSERT INTO {table} ({column}, is_active, {master_id_column}, effective_date)
            VALUES (%s, TRUE, %s, %s)
            RETURNING {table[:-4]}_id;
        """
        cursor.execute(insert_new_record_query, (value, master_id, datetime.now()))
        new_record_id = cursor.fetchone()[0]
        print(f"Inserted new record with ID {new_record_id}, {column} = {value}, and master_id = {master_id}.")

    except Exception as e:
        print(f"Error in apply_scd_type_2: {e}")
        raise



def main():
    try:
        # Establish a database connection
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        print("Connected to the database.")

        # Example input data
        table = "branch_dim"           # Table name
        column = "branch_name"         # Column being updated
        value = "Chicago-southside-downtown"  # New value for the column
        master_id_column = "master_branch_id"  # Name of the master ID column
        master_id = 4



        # Apply SCD Type 2 logic
        apply_scd_type_2(cursor, table, column, value, master_id_column, master_id)

        # Commit the transaction
        conn.commit()

    except Exception as e:
        print(f"Error in main: {e}")

    finally:
        # Clean up database connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()
