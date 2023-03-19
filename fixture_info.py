import requests
from datetime import date
import pandas as pd
from functools import reduce
from sqlalchemy import create_engine
import psycopg2
import configparser
import json

# setup the database password as var
with open(r'C:\Users\alexh\PycharmProjects\fpl_data_pull\pw.json') as f:
    config = json.load(f)
database_password = config['PASSWORDS']['DATABASE_PASSWORD']

# base url for all FPL API endpoints
base_url = 'https://fantasy.premierleague.com/api/'
master_df = pd.DataFrame()
date = date.today()
bootstrap = requests.get(base_url + 'bootstrap-static/').json()
team_raw_df = pd.json_normalize(bootstrap['teams'])


# Function to add points after each fixture
def gw_points(s1, s2):
    if s1 is None or s2 is None:
        return 0
    elif s1 > s2:
        return 3
    elif s1 < s2:
        return 0
    else:
        return 1


def result(s1, s2):
    if s1 is None or s2 is None:
        return None
    elif s1 > s2:
        return 'W'
    elif s1 < s2:
        return 'L'
    else:
        return 'D'


# Get details of the fixtures in gameweeks 0 - 38
# Add this data to a dataframe
for n in range(39):
    fixtures = requests.get(base_url + 'fixtures/?event=' + str(n)).json()
    fixtures_df1 = pd.json_normalize(fixtures)
    master_df = pd.concat([master_df, fixtures_df1])
    # master_df = master_df.append(fixtures_df1)

# Rename column names in the master_df
master_df.rename(columns={'event': 'gameweek_id', 'team_a': 'away_team_id', 'team_a_score': 'away_team_score',
                          'team_h': 'home_team_id', 'team_h_score': 'home_team_score'}, inplace=True)

# Reduce the master_df to only the fields required
master_df = master_df[['gameweek_id', 'finished', 'kickoff_time', 'minutes',
                       'away_team_id', 'away_team_score', 'home_team_id', 'home_team_score', 'stats',
                       'team_h_difficulty', 'team_a_difficulty']]

# Using gw_points, add points to each fixture
master_df['home_points'] = master_df.apply(lambda row: gw_points(row['home_team_score'], row['away_team_score']),
                                           axis=1)
master_df['away_points'] = master_df.apply(lambda row: gw_points(row['away_team_score'], row['home_team_score']),
                                           axis=1)
# Using the result function, add win, lose, or draw for the home team for each game
master_df['home_result'] = master_df.apply(lambda row: result(row['home_team_score'], row['away_team_score']), axis=1)

master_df['away_result'] = master_df.apply(lambda row: result(row['away_team_score'], row['home_team_score']), axis=1)

# Create a reduced df containing only the home_team_id and home_team_result
home_result_df = master_df[['home_team_id', 'home_result']]

# Create a new df to sum the number of home wins by each team
home_wins_df = home_result_df[home_result_df.home_result == 'W'].pivot_table(index='home_team_id', values='home_result',
                                                                             aggfunc='count')
# Use the df index to create an id field
home_wins_df['id'] = home_wins_df.index
# Use the home_results column to create the home_wins column
home_wins_df['home_wins'] = home_wins_df['home_result']
# Reduce the df to only the id and home_wins
home_wins_df = home_wins_df[['id', 'home_wins']]

# Create a new df to sum the number of home losses by each team
home_loss_df = home_result_df[home_result_df.home_result == 'L'].pivot_table(index='home_team_id', values='home_result',
                                                                             aggfunc='count')
# Use the df index to create an id field
home_loss_df['id'] = home_loss_df.index
# Use the home_results column to create the home_loss column
home_loss_df['home_loss'] = home_loss_df['home_result']
# Reduce the df to only the id and home_loss
home_loss_df = home_loss_df[['id', 'home_loss']]

# Create a new df to sum the number of home draws by each team
home_draw_df = home_result_df[home_result_df.home_result == 'D'].pivot_table(index='home_team_id', values='home_result',
                                                                             aggfunc='count')
# Use the df index to create an id field
home_draw_df['id'] = home_draw_df.index
# Use the home_results column to create the home_draw column
home_draw_df['home_draw'] = home_draw_df['home_result']
# Reduce the df to only the id and home_draw
home_draw_df = home_draw_df[['id', 'home_draw']]

# Create a df of all of the away results for each team
away_result_df = master_df[['away_team_id', 'away_result']]

# Create a df to sum the number of away wins for each team
away_wins_df = away_result_df[away_result_df.away_result == 'W'].pivot_table(index='away_team_id', values='away_result',
                                                                             aggfunc='count')
# Use the df index to create an id field
away_wins_df['id'] = away_wins_df.index
# Use the away_results column to create the away_wins column
away_wins_df['away_wins'] = away_wins_df['away_result']
# Reduce the df to only the id and away_wins
away_wins_df = away_wins_df[['id', 'away_wins']]

# Create a df to sum the number of away losses for each team
away_loss_df = away_result_df[away_result_df.away_result == 'L'].pivot_table(index='away_team_id', values='away_result',
                                                                             aggfunc='count')
# Use the df index to create an id field
away_loss_df['id'] = away_loss_df.index
# Use the away_results column to create the away_loss column
away_loss_df['away_loss'] = away_loss_df['away_result']
# Reduce the df to only the id and away_loss
away_loss_df = away_loss_df[['id', 'away_loss']]

# Create a df to sum the number of away draws for each team
away_draw_df = away_result_df[away_result_df.away_result == 'D'].pivot_table(index='away_team_id', values='away_result',
                                                                             aggfunc='count', fill_value=1,
                                                                             dropna=False)
# Use the df index to create an id field
away_draw_df['id'] = away_draw_df.index
# Use the away_results column to create the away_draw column
away_draw_df['away_draw'] = away_draw_df['away_result']
# Reduce the df to only the id and away_draw
away_draw_df = away_draw_df[['id', 'away_draw']]

results = [home_wins_df, home_loss_df, home_draw_df, away_wins_df, away_loss_df, away_draw_df]
# results_df = reduce(lambda left, right: pd.merge(left, right, left_on='id', right_on='id'), results)
results_df = reduce(lambda left, right: pd.merge(left, right, how='outer', left_index=True, right_index=True), results)
results_df.fillna(0, inplace=True)

results_df['total_wins'] = results_df['home_wins'] + results_df['away_wins']
results_df['total_loss'] = results_df['home_loss'] + results_df['away_loss']
results_df['total_draw'] = results_df['home_draw'] + results_df['away_draw']
results_df['games_played'] = results_df['total_wins'] + results_df['total_loss'] + results_df['total_draw']

results_df.to_csv(r'C:\Users\alexh\OneDrive\Documents\Training\Python\Projects\FPL Database\Data Files\results.csv',
                  header=True, index=False)

# Using the result function, add win, lose, or draw for the away team for each game
master_df['away_result'] = master_df.apply(lambda row: result(row['away_team_score'], row['home_team_score']), axis=1)

# Sum all home goals scored by each team and add to a separate dataframe
home_goals_scored = master_df.groupby('home_team_id')['home_team_score'].sum()
home_goals_scored_df = pd.DataFrame(home_goals_scored).rename(columns={'home_team_id': 'id'})

# Sum all away goals scored by each team and add to a separate dataframe
away_goals_scored = master_df.groupby('away_team_id')['away_team_score'].sum()
away_goals_scored_df = pd.DataFrame(away_goals_scored).rename(columns={'away_team_id': 'id'})

# Sum all home goals against by each team and add to a separate dataframe
home_goals_against = master_df.groupby('home_team_id')['away_team_score'].sum()
home_goals_against_df = pd.DataFrame(home_goals_against).rename(columns={'home_team_id': 'id',
                                                                         'away_team_score': 'home_goals_against'})

# Sum all away goals scored by each team and add to a separate dataframe
away_goals_against = master_df.groupby('away_team_id')['home_team_score'].sum()
away_goals_against_df = pd.DataFrame(away_goals_against).rename(columns={'away_team_id': 'id',
                                                                         'home_team_score': 'away_goals_against'})

# Sum all home points by each team and add to a separate dataframe
home_points = master_df.groupby('home_team_id')['home_points'].sum()
home_points_df = pd.DataFrame(home_points).rename(columns={'home_team_id': 'id'})

# Sum all away points by each team and add to a separate dataframe
away_points = master_df.groupby('away_team_id')['away_points'].sum()
away_points_df = pd.DataFrame(away_points).rename(columns={'away_team_id': 'id'})

# Enter the dataframes above into a list to merge all together
dfs = [home_goals_scored_df, away_goals_scored_df, home_goals_against_df, away_goals_against_df,
       home_points_df, away_points_df, results_df]

# Merge all dataframes in dfs into one single df
teams_df = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True), dfs)
teams_df = teams_df.rename(columns={'home_team_score': 'home_goals_scored', 'away_team_score': 'away_goals_scored'})

teams_df['total_goals_scored'] = teams_df['home_goals_scored'] + teams_df['away_goals_scored']
teams_df['total_goals_against'] = teams_df['home_goals_against'] + teams_df['away_goals_against']
teams_df['goal_difference'] = teams_df['total_goals_scored'] - teams_df['total_goals_against']
teams_df['home_goal_difference'] = teams_df['home_goals_scored'] - teams_df['home_goals_against']
teams_df['away_goal_difference'] = teams_df['away_goals_scored'] - teams_df['away_goals_against']
teams_df['total_points'] = teams_df['home_points'] + teams_df['away_points']

teams_df = teams_df.rename(columns={'home_team_score': 'home_goals_scored', 'away_team_score': 'away_goals_scored'})

teams_df['league_position'] = teams_df['total_points'].rank(method='max', ascending=False)
teams_df['id'] = teams_df.index

teams_df = teams_df.merge(team_raw_df,
                          how='inner',
                          left_on='id',
                          right_on='id')

teams_df = teams_df.rename(
    columns={'id': 'team_id', 'name': 'team_name', 'draw': 'drawn', 'win': 'won', 'loss': 'lost'})
teams_df = teams_df[
    ['team_id', 'team_name', 'total_points', 'league_position', 'games_played', 'total_wins', 'total_loss',
     'total_draw', 'total_goals_scored', 'total_goals_against', 'goal_difference', 'home_wins',
     'away_wins', 'home_loss', 'away_loss', 'home_draw', 'away_draw', 'home_points', 'away_points',
     'home_goals_scored', 'away_goals_scored', 'home_goals_against', 'away_goals_against',
     'home_goal_difference', 'away_goal_difference', 'strength', 'strength_overall_home',
     'strength_overall_away', 'strength_attack_home', 'strength_attack_away', 'strength_defence_home',
     'strength_defence_away']]

strength_cols = ['strength_overall_home', 'strength_overall_away', 'strength_attack_home', 'strength_attack_away',
                 'strength_defence_home', 'strength_defence_away']

for col in strength_cols:
    teams_df[col] = teams_df[col] / 100

teams_df.to_csv(
    r'C:\Users\alexh\OneDrive\Documents\Training\Python\Projects\FPL Database\Data Files\Team Data @ '
    + str(date) + '.csv', header=True, index=False)

master_df.to_csv(
    r'C:\Users\alexh\OneDrive\Documents\Training\Python\Projects\FPL Database\Data Files\Gameweek Fixtures @ '
    + str(date) + '.csv', header=True, index=False)


# Connect to database
conn = psycopg2.connect("dbname='fpl.db.22.23' user='postgres' host='localhost' password=" + database_password)

# Create cursor
cur = conn.cursor()

# Zip data from dataframe ready to be uploaded to database
teams_rows = zip(teams_df.team_id, teams_df.team_name, teams_df.total_points, teams_df.league_position,
                 teams_df.games_played,
                 teams_df.total_wins, teams_df.total_loss, teams_df.total_draw, teams_df.total_goals_scored,
                 teams_df.total_goals_against, teams_df.goal_difference, teams_df.home_wins, teams_df.away_wins,
                 teams_df.home_loss, teams_df.away_loss, teams_df.home_draw, teams_df.away_draw, teams_df.home_points,
                 teams_df.away_points, teams_df.home_goals_scored, teams_df.away_goals_scored,
                 teams_df.home_goals_against,
                 teams_df.away_goals_against, teams_df.home_goal_difference, teams_df.away_goal_difference,
                 teams_df.strength,
                 teams_df.strength_overall_home, teams_df.strength_overall_away, teams_df.strength_attack_home,
                 teams_df.strength_attack_away, teams_df.strength_defence_home, teams_df.strength_defence_away)

fixture_rows = zip(master_df.gameweek_id,
                   master_df.finished,
                   master_df.kickoff_time,
                   master_df.minutes,
                   master_df.away_team_id,
                   master_df.away_team_score,
                   master_df.home_team_id,
                   master_df.home_team_score,
                   master_df.team_h_difficulty,
                   master_df.team_a_difficulty,
                   master_df.home_points,
                   master_df.away_points,
                   master_df.home_result,
                   master_df.away_result
                   )

# Use a sql command to create a temp table as a copy of the team_info table 
cur.execute("""CREATE TEMP TABLE temp AS SELECT * FROM team_info WITH NO DATA;""")

# Insert data from our teams zip file into the temp table we have created 
cur.executemany("""INSERT INTO temp(team_id, team_name, total_points, league_position, games_played, 
total_wins, total_loss, total_draw, total_goals_scored, total_goals_against, goal_difference, home_wins, 
away_wins, home_loss, away_loss, home_draw, away_draw, home_points, away_points, home_goals_scored, 
away_goals_scored, home_goals_against, away_goals_against, home_goal_difference, away_goal_difference, 
strength, strength_overall_home, strength_overall_away, strength_attack_home, strength_attack_away, 
strength_defence_home, strength_defence_away
) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", teams_rows)

# Insert data from our temp table into the team_info table. 
# On conflict team_id do update
cur.execute("""INSERT INTO team_info(
            team_id,
            team_name,
            total_points,
            league_position,
            games_played,
            total_wins,
            total_loss,
            total_draw,
            total_goals_scored,
            total_goals_against,
            goal_difference,
            home_wins,
            away_wins,
            home_loss,
            away_loss,
            home_draw,
            away_draw,
            home_points,
            away_points,
            home_goals_scored,
            away_goals_scored,
            home_goals_against,
            away_goals_against,
            home_goal_difference,
            away_goal_difference,
            strength,
            strength_overall_home,
            strength_overall_away,
            strength_attack_home,
            strength_attack_away,
            strength_defence_home,
            strength_defence_away) 
            
            SELECT
            team_id,
            team_name,
            total_points,
            league_position,
            games_played,
            total_wins,
            total_loss,
            total_draw,
            total_goals_scored,
            total_goals_against,
            goal_difference,
            home_wins,
            away_wins,
            home_loss,
            away_loss,
            home_draw,
            away_draw,
            home_points,
            away_points,
            home_goals_scored,
            away_goals_scored,
            home_goals_against,
            away_goals_against,
            home_goal_difference,
            away_goal_difference,
            strength,
            strength_overall_home,
            strength_overall_away,
            strength_attack_home,
            strength_attack_away,
            strength_defence_home,
            strength_defence_away
            FROM temp
            
            ON CONFLICT (team_id)
            DO UPDATE 
            SET
            team_id=EXCLUDED.team_id, 
            team_name=EXCLUDED.team_name, 
            total_points=EXCLUDED.total_points, 
            league_position=EXCLUDED.league_position, 
            games_played=EXCLUDED.games_played, 
            total_wins=EXCLUDED.total_wins, 
            total_loss=EXCLUDED.total_loss, 
            total_draw=EXCLUDED.total_draw, 
            total_goals_scored=EXCLUDED.total_goals_scored, 
            total_goals_against=EXCLUDED.total_goals_against, 
            goal_difference=EXCLUDED.goal_difference, 
            home_wins=EXCLUDED.home_wins, 
            away_wins=EXCLUDED.away_wins, 
            home_loss=EXCLUDED.home_loss, 
            away_loss=EXCLUDED.away_loss, 
            home_draw=EXCLUDED.home_draw, 
            away_draw=EXCLUDED.away_draw, 
            home_points=EXCLUDED.home_points, 
            away_points=EXCLUDED.away_points, 
            home_goals_scored=EXCLUDED.home_goals_scored, 
            away_goals_scored=EXCLUDED.away_goals_scored, 
            home_goals_against=EXCLUDED.home_goals_against, 
            away_goals_against=EXCLUDED.away_goals_against, 
            home_goal_difference=EXCLUDED.home_goal_difference, 
            away_goal_difference=EXCLUDED.away_goal_difference, 
            strength=EXCLUDED.strength, 
            strength_overall_home=EXCLUDED.strength_overall_home, 
            strength_overall_away=EXCLUDED.strength_overall_away, 
            strength_attack_home=EXCLUDED.strength_attack_home, 
            strength_attack_away=EXCLUDED.strength_attack_away, 
            strength_defence_home=EXCLUDED.strength_defence_home, 
            strength_defence_away=EXCLUDED.strength_defence_away
            ;""")

# Create temp table as a copy of fixture_info table
cur.execute("""CREATE TEMP TABLE fix_temp AS SELECT * FROM fixture_info WITH NO DATA;""")
cur.execute("""DELETE FROM fixture_info""")

# Insert data from our fixture zip file into the temp table
cur.executemany("""INSERT INTO fix_temp(
gameweek_id,
finished,
kickoff_time,
minutes,
away_team_id,
away_team_score,
home_team_id,
home_team_score,
team_h_difficulty,
team_a_difficulty,
home_points,
away_points,
home_result,
away_result
) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", fixture_rows)

# Insert data from out temp table into the fixture_info table
cur.execute("""INSERT INTO fixture_info(
gameweek_id,
finished,
kickoff_time,
minutes,
away_team_id,
away_team_score,
home_team_id,
home_team_score,
team_h_difficulty,
team_a_difficulty,
home_points,
away_points,
home_result,
away_result)

SELECT
gameweek_id,
finished,
kickoff_time,
minutes,
away_team_id,
away_team_score,
home_team_id,
home_team_score,
team_h_difficulty,
team_a_difficulty,
home_points,
away_points,
home_result,
away_result

FROM fix_temp """)

# Close connection
conn.commit()
cur.close()
conn.close()

