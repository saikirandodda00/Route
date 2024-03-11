import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import urllib.parse

def process_csv(csv_file_path, table_name, column_to_clean=None, start_concat=None, end_concat=None,
                database_name="postgres", user="postgres", password="Pttgd@123", host="localhost"):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)

    # Check if cleaning step is requested
    if column_to_clean and start_concat and end_concat:
        # Concatenate rows based on specified start and end characters
        for index, row in df.iterrows():
            cell_value = str(row[column_to_clean])  # Convert to string to handle NaN values
            if not cell_value.startswith(start_concat) or not cell_value.endswith(end_concat):
                prev_index = index - 1
                if prev_index >= 0:
                    prev_cell_value = df.at[prev_index, column_to_clean]
                    if not isinstance(prev_cell_value, float):  # Check if previous value is not NaN
                        df.at[prev_index, column_to_clean] += cell_value

        # Drop the concatenated rows
        df = df[df[column_to_clean].str.startswith(start_concat) & df[column_to_clean].str.endswith(end_concat)]

    # Write the DataFrame to a new CSV file (cleaned or unchanged)
    cleaned_csv_path = f'{csv_file_path}_cleaned.csv' if column_to_clean else csv_file_path
    df.to_csv(cleaned_csv_path, index=False)
    
    df = pd.read_csv(cleaned_csv_path)# Reading  the CSV file into a DataFrame
    df.dropna(axis=1, how='all', inplace=True)  # Dropping  columns with all NaN values if there is any

    # Connection to the PostgreSQL database
    conn = psycopg2.connect(
        host=host,
        database=database_name,
        user=user,
        password=password
    )

    # Creating a cursor object
    cursor = conn.cursor()

    # Generate the CREATE TABLE query dynamically based on the DataFrame columns
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("

    for column_name, dtype in zip(df.columns, df.dtypes):
        if dtype == 'object':
            column_type = 'TEXT'
        elif dtype == 'int64':
            column_type = 'INTEGER'
        elif dtype == 'float64':
            column_type = 'FLOAT'
        else:
            # Handle other data types as needed
            pass
        
        create_table_query += f"\n{column_name} {column_type},"

    # Remove the trailing comma and close the CREATE TABLE query
    create_table_query = create_table_query.rstrip(',') + "\n)"

    # Execute the CREATE TABLE query
    cursor.execute(create_table_query)
    conn.commit()

    # Inserting data into the PostgreSQL table
    encoded_password = urllib.parse.quote_plus(password)
    engine = create_engine(f'postgresql://{user}:{encoded_password}@{host}:5432/{database_name}')
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    # Close the cursor and connection
    cursor.close()
    conn.close()

 # Specifying  the column to clean, the starting and ending characters, and the new table name
process_csv(r"C:\Users\vendodda\Downloads\Accern data (2).csv", "accern_data", 
            column_to_clean="entity_text", start_concat='["', end_concat='"]')
