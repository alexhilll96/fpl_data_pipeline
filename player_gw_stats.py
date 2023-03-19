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

def gameweek_func(df, gw_id_col, player_id_col):
    x = df.groupby(gw_id_col).count()
    return x


# Defining today's date. We will use this when saving the Flat File
today = date.today()

# Base url for all FPL API endpoints
base_url = 'https://fantasy.premierleague.com/api/'
bootstrap = requests.get(base_url + 'bootstrap-static/').json()

master_df = pd.DataFrame()

# Pull data from fpl gw summary end point for each player
# Add this all this data to the master_df
for i in range(1, 746):
    print(i)
    data = requests.get(base_url + 'element-summary/' + str(i) + '/').json()
    df = pd.json_normalize(data['history'])
    # Reduce the dataframe to columns we need
    df.rename(columns={'round': 'gameweek_id', 'element': 'player_id', 'total_points': 'points', 'opponent_team': 'opponent_id'}, inplace=True)
    df = df[['player_id', 'gameweek_id', 'opponent_id', 'goals_scored', 'assists', 'clean_sheets',
                    'goals_conceded', 'minutes', 'own_goals', 'penalties_missed', 'saves', 'penalties_saved',
                    'yellow_cards', 'red_cards', 'influence', 'creativity', 'threat', 'points', 'bonus', 'bps',
                    'transfers_balance', 'selected', 'transfers_in', 'transfers_out', 'expected_goals', 'expected_assists', 
                    'expected_goal_involvements', 'expected_goals_conceded']]
    # Get a count of the instances of the gw_id
    x = df['gameweek_id'].value_counts().to_dict()
    # Insert the count of gw_ids to the dataframe to a column named helper
    df['game_count'] = df['gameweek_id'].map(x)
    # Use the helper column to input a dgw flag
    df['DGW'] = df['game_count'].apply(lambda x: 'True' if x > 1 else 'False')
    master_df = pd.concat([master_df, df])

master_df.astype({'player_id': 'int',
'gameweek_id': 'int',
'goals_scored': 'int',
'assists': 'int',
'clean_sheets': 'int',
'goals_conceded': 'int',
'minutes': 'int',
'own_goals': 'int',
'penalties_missed': 'int',
'saves': 'int',
'penalties_saved': 'int',
'yellow_cards': 'int',
'red_cards': 'int',
'influence': 'float',
'creativity': 'float',
'threat': 'float',
'points': 'int',
'bonus': 'int',
'bps': 'int',
'transfers_balance': 'int',
'selected': 'int',
'transfers_in': 'int',
'transfers_out': 'int',
'DGW': 'bool',
'game_count': 'int',
'opponent_id': 'int'
}).dtypes


master_df.to_csv(r'C:\Users\alexh\OneDrive\Documents\Training\Python\Projects\FPL Database\Data Files\player.gw.stats' +
                str(today) + '.csv',header=True, index=False, encoding='utf-8-sig')


player_gw_data = zip(master_df.player_id,
                     master_df.gameweek_id,
                     master_df.goals_scored,
                     master_df.assists,
                     master_df.clean_sheets,
                     master_df.goals_conceded,
                     master_df.minutes,
                     master_df.own_goals,
                     master_df.penalties_missed,
                     master_df.saves,
                     master_df.penalties_saved,
                     master_df.yellow_cards,
                     master_df.red_cards,
                     master_df.influence,
                     master_df.creativity,
                     master_df.threat,
                     master_df.points,
                     master_df.bonus,
                     master_df.bps,
                     master_df.transfers_balance,
                     master_df.selected,
                     master_df.transfers_in,
                     master_df.transfers_out,
                     master_df.DGW,
                     master_df.game_count,
                     master_df.opponent_id,
                     master_df.expected_goals, 
                    master_df.expected_assists, 
                    master_df.expected_goal_involvements, 
                    master_df.expected_goals_conceded 

                     )

# Connect to database
conn = psycopg2.connect("dbname='fpl.db.22.23' user='postgres' host='localhost' password=" + database_password)

# Create cursor
cur = conn.cursor()

cur.execute("""CREATE TEMP TABLE temp AS SELECT * FROM player_gw_stats WITH NO DATA;""")

cur.executemany("""INSERT INTO temp(
                player_id,
                gameweek_id,
                goals_scored,
                assists,
                clean_sheets,
                goals_conceded,
                minutes,
                own_goals,
                penalties_missed,
                saves,
                penalties_saved,
                yellow_cards,
                red_cards,
                influence,
                creativity,
                threat,
                points,
                bonus,
                bps,
                transfers_balance,
                selected,
                transfers_in,
                transfers_out,
                dgw,
                game_count,
                opponent_id,
                expected_goals,
                expected_assists,
                expected_goal_involvements,
                expected_goals_conceded

                ) VALUES(%s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                player_gw_data)

cur.execute("""INSERT INTO player_gw_stats(
                player_id,
                gameweek_id,
                goals_scored,
                assists,
                clean_sheets,
                goals_conceded,
                minutes,
                own_goals,
                penalties_missed,
                saves,
                penalties_saved,
                yellow_cards,
                red_cards,
                influence,
                creativity,
                threat,
                points,
                bonus,
                bps,
                transfers_balance,
                selected,
                transfers_in,
                transfers_out,
                dgw,
                game_count,
                opponent_id,
                expected_goals,
                expected_assists,
                expected_goal_involvements,
                expected_goals_conceded)
                
                SELECT
                player_id,
                gameweek_id,
                goals_scored,
                assists,
                clean_sheets,
                goals_conceded,
                minutes,
                own_goals,
                penalties_missed,
                saves,
                penalties_saved,
                yellow_cards,
                red_cards,
                influence,
                creativity,
                threat,
                points,
                bonus,
                bps,
                transfers_balance,
                selected,
                transfers_in,
                transfers_out,
                dgw,
                game_count,
                opponent_id,
                expected_goals,
                expected_assists,
                expected_goal_involvements,
                expected_goals_conceded
                FROM temp
            
                ON CONFLICT (player_id, gameweek_id, opponent_id)
                DO UPDATE
                SET
                goals_scored=EXCLUDED.goals_scored, 
                assists=EXCLUDED.assists, 
                clean_sheets=EXCLUDED.clean_sheets, 
                goals_conceded=EXCLUDED.goals_conceded, 
                minutes=EXCLUDED.minutes, 
                own_goals=EXCLUDED.own_goals, 
                penalties_missed=EXCLUDED.penalties_missed, 
                saves=EXCLUDED.saves, 
                penalties_saved=EXCLUDED.penalties_saved, 
                yellow_cards=EXCLUDED.yellow_cards, 
                red_cards=EXCLUDED.red_cards, 
                influence=EXCLUDED.influence, 
                creativity=EXCLUDED.creativity, 
                threat=EXCLUDED.threat, 
                points=EXCLUDED.points, 
                bonus=EXCLUDED.bonus, 
                bps=EXCLUDED.bps, 
                transfers_balance=EXCLUDED.transfers_balance, 
                selected=EXCLUDED.selected, 
                transfers_in=EXCLUDED.transfers_in, 
                transfers_out=EXCLUDED.transfers_out,
                dgw=EXCLUDED.dgw,
                game_count=EXCLUDED.game_count,
                expected_goals=EXCLUDED.expected_goals,
                expected_assists=EXCLUDED.expected_assists,
                expected_goal_involvements=EXCLUDED.expected_goal_involvements,
                expected_goals_conceded=EXCLUDED.expected_goals_conceded
                ;""")
conn.commit()
cur.close()
conn.close()


