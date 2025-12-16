from datetime import datetime, timedelta
from connectors import connector, engine
import random

schema_name, table_name = 'my_db', 'reading_habits'

def get_time_data(lit_type):
    # Random date from beginning of the year until the 25th of November
    current_year = datetime.now().year
    start_date = datetime(current_year, 7, 1)
    end_date = datetime(current_year, 11, 25)
    total_days = (end_date - start_date).days
    random_days = random.randint(0, total_days)
    random_date = start_date + timedelta(days=random_days)

    # Constraints
    special_configs = {
        1: {
            "time_range": (9, 23),
            "min_duration": 35,
            "max_duration": 240
        }
        , 2: {
            "time_range": (10, 15),  
            "min_duration": 10,     
            "max_duration": 50     
        }
        , 3: {
            "time_range": (10, 22),
            "min_duration": 20,
            "max_duration": 180
        }
    }

    config = special_configs.get(lit_type)

    # Constraint calculations
    start_hour, end_hour = config["time_range"]
    business_start_seconds = start_hour * 3600
    business_end_seconds = end_hour * 3600
    
    min_duration_seconds = config["min_duration"] * 60
    max_duration_seconds = config["max_duration"] * 60

    # Beginning datetime
    max_possible_start = business_end_seconds - min_duration_seconds
    start_seconds = random.randint(business_start_seconds, max_possible_start)
    
    # End datetime
    actual_max_duration = min(max_duration_seconds, business_end_seconds - start_seconds)
    duration_seconds = random.randint(min_duration_seconds, actual_max_duration)
    end_seconds = start_seconds + duration_seconds

    # Creation of actual datetime objects
    start_time = random_date + timedelta(seconds=start_seconds)
    end_time = random_date + timedelta(seconds=end_seconds)
    duration = (end_time - start_time).total_seconds() / 60

    return random_date.strftime("%Y-%m-%d"), start_time, end_time, round(duration)

def generate_data():
    read_literature = {
        'Crime and Punishment': ['Fyodor Dostoevsky', 'literary fiction', 1],
        'Divine Comedy': ['Dante Alighieri', 'narrative poem', 1],
        'Introduction to Greenplum Architecture': ['Max Yang', 'science', 2],
        'The Data Science Behind AI': ['William Vorhies', 'science', 2],
        'Missing 411': ['David Paulides', 'non-fiction', 1],
        'The Betrothed': ['Alessandro Manzoni', 'historical fiction', 1],
        'What is a Cryptographic Protocol?': ['SSL Support Team', 'science', 2],
        'The Lord of the Rings': ['J. R. R. Tolkien', 'fantasy', 1],
        'Machine Learning Applications in Healthcare': ['Christopher Toh, James P. Brody', 'academic', 3],
        'The Vampire in Europe': ['Montague Summers', 'horror', 1],
        'The Apache Kafka Handbook – How to Get Started Using Kafka': ['Gerard Hynes', 'science', 2],
        'Understanding ClickHouse: Benefits and Limitations': ['CelerData', 'science', 2],
        'Tales of Horror': ['Edgar Allan Poe', 'horror', 1],
        'History of Databases': ['Kristi Berg, Tom Joseph Seymour', 'academic', 3],
        'Murder in Mesopotamia': ['Agatha Christie', 'detective', 1],
        'Deep Learning with Big Data': ['Muhammad Khojaye', 'academic', 3],
        'What Was the Beast of Gévaudan?': ['Joseph A. Williams', 'history', 2],
        'Clickhouse: Overview and Applications': ['Valiotti Analytics', 'science', 2],
        'The Government Inspector': ['Nikolai Gogol', 'comedy', 1]
    }

    title = random.choice(list(read_literature.keys()))
    author, genre, lit_type = read_literature[title][0], read_literature[title][1], read_literature[title][2]
    rating = random.randint(1, 5)

    random_date, start_time, end_time, duration = get_time_data(lit_type)

    return random_date, start_time, end_time, duration, title, author, lit_type, genre, rating

def data_insertion(row_num):
    exist_flag, title_flag, data_to_add = [], [], []
    inserted_count = 0
    while inserted_count < row_num:
        date_str, start_time, end_time, duration, title, author, lit_type, genre, rating = generate_data()
        has_overlap = any(
            (start_time <= existing_end and end_time >= existing_start)
            for existing_start, existing_end in exist_flag
        )
        if has_overlap:
            continue
        elif title in title_flag and lit_type == 2:
            continue
        else:
            exist_flag.append((start_time, end_time))
            title_flag.append(title)
            data_to_add.append(
                (
                    date_str
                    , start_time.strftime("%Y-%m-%d %H:%M:%S")
                    , end_time.strftime("%Y-%m-%d %H:%M:%S")
                    , duration, title, author, lit_type, genre, rating
                )
            )
            inserted_count += 1

    insert_query = "INSERT INTO {0}.{1} VALUES {2};"
    values = ", ".join(str(tup) for tup in data_to_add)
    print(insert_query.format(schema_name, table_name, values.replace('"', "'")))
    # connector.sql_execute_to_db(insert_query.format(schema_name, table_name, values.replace('"', "'")), engine.ch_engine)
    return len(data_to_add)

row_num = int(input("Insert number of rows with data to add: "))
rows = data_insertion(row_num)
print(f'Inserted {rows} rows')