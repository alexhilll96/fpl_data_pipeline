import requests
import pandas as pd
from datetime import date
import psycopg2
import configparser
import json

# setup the database password as var
with open(r'C:\Users\alexh\PycharmProjects\fpl_data_pull\pw.json') as f:
    config = json.load(f)
database_password = config['PASSWORDS']['DATABASE_PASSWORD']

# Defining today's date. We will use this when saving the Flat File
today = date.today()

# Base url for all FPL API endpoints
base_url = 'https://fantasy.premierleague.com/api/'

# Get data from bootstrap-static endpoint
bootstrap = requests.get(base_url + 'bootstrap-static/').json()
position_df = pd.json_normalize(bootstrap['element_types'])
player_df = pd.json_normalize(bootstrap['elements'])

player_df.rename(
    columns={'id': 'player_id', 'element_type': 'position_id', 'team': 'team_id', 'now_cost': 'current_price'},
    inplace=True)

position_df.rename(columns={'id': 'position_id', 'plural_name_short': 'position'}, inplace=True)

master_df = pd.merge(left=player_df,
                     right=position_df,
                     left_on='position_id',
                     right_on='position_id')

master_df = master_df[['player_id', 'first_name', 'second_name', 'position', 'team_id', 'current_price',
                       'total_points', 'goals_scored', 'assists', 'clean_sheets', 'goals_conceded', 'own_goals',
                       'penalties_saved', 'penalties_missed', 'yellow_cards', 'red_cards', 'saves', 'bonus', 'minutes',
                       'bps', 'influence', 'creativity', 'threat', 'in_dreamteam', 'dreamteam_count',
                       'selected_by_percent', 'transfers_in', 'transfers_in_event', 'transfers_out',
                       'transfers_out_event', 'chance_of_playing_next_round', 'chance_of_playing_this_round']]

master_df = master_df.fillna(0)

master_df = master_df.astype(
    {'player_id': int,
     'first_name': str,
     'second_name': str,
     'position': str,
     'team_id': int,
     'current_price': float,
     'total_points': int,
     'goals_scored': int,
     'assists': int,
     'clean_sheets': int,
     'goals_conceded': int,
     'own_goals': int,
     'penalties_saved': int,
     'penalties_missed': int,
     'yellow_cards': int,
     'red_cards': int,
     'saves': int,
     'bonus': int,
     'minutes': int,
     'bps': int,
     'influence': float,
     'creativity': float,
     'threat': float,
     'in_dreamteam': str,
     'dreamteam_count': int,
     'selected_by_percent': float,
     'transfers_in': int,
     'transfers_in_event': int,
     'transfers_out': int,
     'transfers_out_event': int,
     'chance_of_playing_next_round': int,
     'chance_of_playing_this_round': int
     })

master_df.to_csv(r'C:\Users\alexh\OneDrive\Documents\Training\Python\Projects\FPL Database\Data Files\player.info ' +
                 str(today) + '.csv', header=True, index=False, encoding='utf-8-sig')

player_info_data = zip(master_df.player_id,
                       master_df.first_name,
                       master_df.second_name,
                       master_df.position,
                       master_df.team_id,
                       master_df.current_price,
                       master_df.total_points,
                       master_df.goals_scored,
                       master_df.assists,
                       master_df.clean_sheets,
                       master_df.goals_conceded,
                       master_df.own_goals,
                       master_df.penalties_saved,
                       master_df.penalties_missed,
                       master_df.yellow_cards,
                       master_df.red_cards,
                       master_df.saves,
                       master_df.bonus,
                       master_df.minutes,
                       master_df.bps,
                       master_df.influence,
                       master_df.creativity,
                       master_df.threat,
                       master_df.in_dreamteam,
                       master_df.dreamteam_count,
                       master_df.selected_by_percent,
                       master_df.transfers_in,
                       master_df.transfers_in_event,
                       master_df.transfers_out,
                       master_df.transfers_out_event,
                       master_df.chance_of_playing_next_round,
                       master_df.chance_of_playing_this_round
                       )
# Connect to database
conn = psycopg2.connect("dbname='fpl.db.22.23' user='postgres' host='localhost' password=" + database_password)

# Create cursor
cur = conn.cursor()

cur.execute("""CREATE TEMP TABLE temp AS SELECT * FROM player_info WITH NO DATA;""")

cur.executemany("""INSERT INTO temp(                
                player_id, 
                first_name, 
                second_name, 
                position, 
                team_id, 
                current_price, 
                total_points, 
                goals_scored, 
                assists, 
                clean_sheets, 
                goals_conceded, 
                own_goals, 
                penalties_saved, 
                penalties_missed, 
                yellow_cards, 
                red_cards, 
                saves, 
                bonus, 
                minutes, 
                bps, 
                influence, 
                creativity, 
                threat, 
                in_dreamteam, 
                dreamteam_count, 
                selected_by_percent, 
                transfers_in, 
                transfers_in_event,
                transfers_out,
                transfers_out_event,
                chance_of_playing_next_round,
                chance_of_playing_this_round
                ) VALUES(%s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", player_info_data)

cur.execute("""INSERT INTO player_info(
            player_id, 
            first_name, 
            second_name, 
            position, 
            team_id, 
            current_price, 
            total_points, 
            goals_scored, 
            assists, 
            clean_sheets, 
            goals_conceded, 
            own_goals, 
            penalties_saved, 
            penalties_missed, 
            yellow_cards, 
            red_cards, 
            saves, 
            bonus, 
            minutes, 
            bps, 
            influence, 
            creativity, 
            threat, 
            in_dreamteam, 
            dreamteam_count, 
            selected_by_percent, 
            transfers_in, 
            transfers_in_event, 
            transfers_out, 
            transfers_out_event, 
            chance_of_playing_next_round, 
            chance_of_playing_this_round)
            
            SELECT
            player_id, 
            first_name, 
            second_name, 
            position, 
            team_id, 
            current_price, 
            total_points, 
            goals_scored, 
            assists, 
            clean_sheets, 
            goals_conceded, 
            own_goals, 
            penalties_saved, 
            penalties_missed, 
            yellow_cards, 
            red_cards, 
            saves, 
            bonus, 
            minutes, 
            bps, 
            influence, 
            creativity, 
            threat, 
            in_dreamteam, 
            dreamteam_count, 
            selected_by_percent, 
            transfers_in, 
            transfers_in_event, 
            transfers_out, 
            transfers_out_event, 
            chance_of_playing_next_round, 
            chance_of_playing_this_round
            FROM temp
            
            ON CONFLICT (player_id)
            DO UPDATE
            SET
            first_name=EXCLUDED.first_name,
            second_name=EXCLUDED.second_name,
            position=EXCLUDED.position,
            team_id=EXCLUDED.team_id,
            current_price=EXCLUDED.current_price,
            total_points=EXCLUDED.total_points,
            goals_scored=EXCLUDED.goals_scored,
            assists=EXCLUDED.assists,
            clean_sheets=EXCLUDED.clean_sheets,
            goals_conceded=EXCLUDED.goals_conceded,
            own_goals=EXCLUDED.own_goals,
            penalties_saved=EXCLUDED.penalties_saved,
            penalties_missed=EXCLUDED.penalties_missed,
            yellow_cards=EXCLUDED.yellow_cards,
            red_cards=EXCLUDED.red_cards,
            saves=EXCLUDED.saves,
            bonus=EXCLUDED.bonus,
            minutes=EXCLUDED.minutes,
            bps=EXCLUDED.bps,
            influence=EXCLUDED.influence,
            creativity=EXCLUDED.creativity,
            threat=EXCLUDED.threat,
            in_dreamteam=EXCLUDED.in_dreamteam,
            dreamteam_count=EXCLUDED.dreamteam_count,
            selected_by_percent=EXCLUDED.selected_by_percent,
            transfers_in=EXCLUDED.transfers_in,
            transfers_in_event=EXCLUDED.transfers_in_event,
            transfers_out=EXCLUDED.transfers_out,
            transfers_out_event=EXCLUDED.transfers_out_event,
            chance_of_playing_next_round=EXCLUDED.chance_of_playing_next_round,
            chance_of_playing_this_round=EXCLUDED.chance_of_playing_this_round
;""")

conn.commit()
cur.close()
conn.close()
