import psycopg2 as ps

# Define a 'delete' function - which gets rid of old data
def run_delete():
    # Provide AWS' Connection info.
    host_name = 'database-1.*********.us-*********.rds.amazonaws.com'
    dbname = 'weather_db'
    username = 'username'
    password = '*************'
    port = '5432'

    # Create connection
    try:
        conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
    except ps.OperationalError as e:
        raise e
    else:
        print('Connected!')
    
    # Cursor allows python code to execute sql statements
    curr = conn.cursor()

    # We do our delete query for 'old data', execute and commit
    delete_data_command = ("""DELETE FROM public.weather_data;
                            """)
    curr.execute(delete_data_command)
    conn.commit()
    print('RECORDS DELETED')
    # End of run_delete function
# Now we call the delete function
run_delete()
