import requests
from datetime import date
import pandas as pd
import psycopg2
import configparser
import json

# setup the database password as var
with open(r'C:\Users\alexh\PycharmProjects\fpl_data_pull\pw.json') as f:
    config = json.load(f)
# config = configparser.ConfigParser()
# config.read(r'C:\Users\alexh\PycharmProjects\fpl_data_pull\pw.json')
database_password = config['PASSWORDS']['DATABASE_PASSWORD']

# base url for all FPL API endpoints
base_url = 'https://fantasy.premierleague.com/api/'
bootstrap = requests.get(base_url + 'bootstrap-static/').json()

events_df = pd.json_normalize(bootstrap['events'])
events_df = events_df[['id', 'deadline_time', 'average_entry_score', 'highest_score', 'most_selected',
                       'most_transferred_in', 'top_element', 'most_captained', 'most_vice_captained']]

events_df = events_df.fillna(0)
events_df = events_df.astype({'id': 'int', 'average_entry_score': 'int', 'highest_score': 'int', 'most_selected': 'int',
                              'most_transferred_in': 'int', 'top_element': 'int', 'most_captained': 'int',
                              'most_vice_captained': 'int'})

                         
gw_stats_data = zip(events_df.id,
                    events_df.deadline_time,
                    events_df.average_entry_score,
                    events_df.highest_score,
                    events_df.most_selected,
                    events_df.most_transferred_in,
                    events_df.top_element,
                    events_df.most_captained,
                    events_df.most_vice_captained
                    )

# Connect to database
conn = psycopg2.connect("dbname='fpl.db.22.23' user='postgres' host='localhost' password=" + database_password)

# Create cursor
cur = conn.cursor()

cur.execute("""CREATE TEMP TABLE temp AS SELECT * FROM gw_stats WITH NO DATA;""")

cur.executemany("""INSERT INTO temp(id, deadline_time, average_entry_score, highest_score, most_selected,
most_transferred_in, top_element, most_captained, most_vice_captained) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                gw_stats_data)

cur.execute("""INSERT INTO gw_stats(
    id, 
    deadline_time, 
    average_entry_score, 
    highest_score, 
    most_selected,
    most_transferred_in, 
    top_element, 
    most_captained, 
    most_vice_captained)

    SELECT 
    id, 
    deadline_time, 
    average_entry_score, 
    highest_score, 
    most_selected,
    most_transferred_in, 
    top_element, 
    most_captained, 
    most_vice_captained
    FROM temp

    ON CONFLICT (id)
    DO UPDATE
    SET
    deadline_time=EXCLUDED.deadline_time,
    average_entry_score=EXCLUDED.average_entry_score,
    highest_score=EXCLUDED.highest_score,
    most_selected=EXCLUDED.most_selected,
    most_transferred_in=EXCLUDED.most_transferred_in,
    top_element=EXCLUDED.top_element,
    most_captained=EXCLUDED.most_captained,
    most_vice_captained=EXCLUDED.most_vice_captained

    """)


conn.commit()
cur.close()
conn.close()

events_df.to_csv(r'C:\Users\alexh\OneDrive\Documents\Training\Python\Projects\FPL Database\Data Files\events_df.csv')
