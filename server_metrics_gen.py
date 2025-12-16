from datetime import datetime, timedelta, time
from connectors import connector, engine
import random

schema_name, table_name = 'my_db', 'server_metrics'

def get_time_data():
    # Creation of 30s long intervals for 1H from 12:00:00 of the 25th of November
    current_year = datetime.now().year
    start_date = datetime(current_year, 11, 25)
    noon = datetime.combine(start_date.date(), time(hour=12))  
    next_hour = noon + timedelta(hours=1)  

    current_time = noon
    records = []

    while current_time < next_hour:
        records.append(current_time)
        current_time += timedelta(seconds=30)

    return records

def generate_data():
    metric_names = ['Memory load', 'CPU load', 'Network transfers', 'Disk load']
    metrics = []
    for metric in metric_names:
        value = random.randint(1, 100) if metric != 'Network transfers' else round(random.uniform(0.0, 12.5), 2)
        metrics.append((metric, value))
    return metrics

def data_insertion():
    day_metrics = get_time_data()
    host1, host2 = '192.168.45.23', '192.169.60.30'
    data_to_add = []
    for ts in day_metrics:
        host1_metrics = generate_data()
        for metric in host1_metrics:
            data_to_add.append((ts.strftime("%Y-%m-%d %H:%M:%S"), metric[0], metric[1], host1))

        host2_metrics = generate_data()
        for metric in host2_metrics:
            data_to_add.append((ts.strftime("%Y-%m-%d %H:%M:%S"), metric[0], metric[1], host2))

    insert_query = "INSERT INTO {0}.{1} VALUES {2};"
    values = ", ".join(str(tup) for tup in data_to_add)
    print(insert_query.format(schema_name, table_name, values.replace('"', "'")))
    # connector.sql_execute_to_db(insert_query.format(schema_name, table_name, values.replace('"', "'")), engine.ch_engine)
    return len(data_to_add)

rows = data_insertion()
print(f'Inserted {rows} rows')