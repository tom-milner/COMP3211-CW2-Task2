import azure.functions as func
import logging
import pyodbc
import os
import json

# pyodbc needs the SQL driver aswell as the SQL connection string for connecting.
sql_driver = "Driver={ODBC Driver 18 for SQL Server};"

# Get Azure Application Settings.
db_name = os.environ["DatabaseName"]
table_name = os.environ["TableName"]
sql_connection_string = sql_driver + os.environ["SqlConnectionString"]

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# Calculate statistics on the sensor data. Triggered by an HTTP request.
@app.function_name(name="analyse_sensor_data")
@app.route(route="analyse_sensor_data")
def analyse_sensor_data(req: func.HttpRequest) -> func.HttpResponse:
    # Connect to the database.
    conn = pyodbc.connect(sql_connection_string)
    cur = conn.cursor()

    # If the table doesn't exist, exit early.
    if not cur.tables(table=table_name, tableType="TABLE").fetchone():
        conn.close()
        return func.HttpResponse("Sensor data couldn't be found.")

    # Get the IDs of all the sensors in use.
    get_sensor_ids_sql = (
        f"SELECT DISTINCT sensor_id FROM {table_name} ORDER BY sensor_id ASC"
    )
    cur.execute(get_sensor_ids_sql)
    sensors_in_use = []
    for row in cur.fetchall():
        sensors_in_use.append(row[0])  # Store the sensor ID.

    stats = {}
    data_points = ["temp", "wind_speed", "co2", "rel_humidity"]
    for sensor in sensors_in_use:
        stats[sensor] = {}
        # Get min, max, average of each data point.
        for dat in data_points:
            # Make SQL calculate all the stats for us.
            get_stats_sql = f"SELECT MIN({dat}), MAX({dat}), AVG({dat}) FROM {table_name} WHERE (sensor_id = {sensor})"
            cur.execute(get_stats_sql)

            # Parse the results into a python dict.
            results = cur.fetchall()[0]
            data_point_stats = {
                "min": results[0],
                "max": results[1],
                "average": results[2],
            }
            stats[sensor][dat] = data_point_stats

    conn.commit()
    conn.close()

    logging.info("Stats Created.")
    return func.HttpResponse(json.dumps(stats))
