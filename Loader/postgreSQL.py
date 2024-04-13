import time
import psycopg
def importToPostgreSQL():
    with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
        with db.cursor() as cursor:
            with cursor.copy("COPY Countries (country_id, country_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/countries.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                
                cy.write(data)

            with cursor.copy("COPY Competitions (competition_ID, competition_Name, competition_Gender, competition_Youth, competition_International, country_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/competitions.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                
                cy.write(data)

            with cursor.copy("COPY Referee (referees_Id, referees_Name, referees_Country) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/referees.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                
                cy.write(data)

            with cursor.copy("COPY Stadium (stadiums_Id, stadiums_Name, stadiums_Country) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/stadium.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                
                cy.write(data)

            with cursor.copy("COPY Manager (managers_Id, managers_Name, managers_Nickname, managers_date_of_Birth, managers_Country) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/managers.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                
                cy.write(data)

            with cursor.copy("COPY Team (teams_Id, teams_Name, teams_Gender, teams_Group, teams_Country, managers_Id) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/teams.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                
                cy.write(data)

            with cursor.copy("COPY Match (matches_Id, competitions_Id, season_Id, season_Name, match_Date, kick_off_Time, home_team_Name, away_team_Name, home_Score, away_Score, match_Week, competition_Stage, stadium_Id, referee_Id) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/match.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                
                cy.write(data)

            with cursor.copy("COPY Position (position_Id, position_Name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/position.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                
                cy.write(data)

            with cursor.copy("COPY Player (player_Id, player_Name, position_ID, jersey_Number, team_Name, country_Name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/player.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                
                cy.write(data)

            with cursor.copy("COPY Tactic (match_id, event_id, formation, lineup) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/tactic.csv', 'r', encoding='utf-8') as f:
                    # Iterate over each line in the CSV file
                    data = f.read()
                
                cy.write(data)

            with cursor.copy("COPY PlayPattern (play_pattern_Id, play_pattern_Name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/play_pattern.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                
                cy.write(data)

            with cursor.copy("COPY Events (event_id, event_Index, event_Period, event_timeStamp, event_Minute, event_Second, type_Id, type_Name, possession, possession_team_Name, play_pattern_Id, location_x, location_y, event_Duration, counterpress, position_Id, team_Name, player_Name, match_id, season_Name, competition_Name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/events.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                
                cy.write(data)
            
            with cursor.copy("COPY Pass (event_id, playerName, teamName, recipient_Name, pass_Length, angle, heightId, heightName, aerialWon, endLocationX, endLocationY, assistedShotId, deflected, miscommunication, isCross, cutBack, switch, shotAssist, goalAssist, bodyPartId, bodyPartName, TypeId, TypeName, outcomeId, outcomeName, techniqueId, techniqueName, match_id, season_Name, competition_Name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/pass.csv', 'r', encoding='utf-8') as f:
                     data = f.read()
                
                cy.write(data)

            with cursor.copy("COPY Shot (event_id, playerName, teamName, statsbomXg, endLocationX, endLocationY, endLocationZ, followsDribble, firstTime, openGoal, deflected, techniqueId, techniqueName, bodyPartId, bodyPartName, typeId, typeName, outcomeId, outcomeName, match_id, season_name, competition_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/shot.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)

            with cursor.copy("COPY Dribble (event_id, playerName, teamName, overrun, nutmeg, noTouch, outcomeId, outcomeName, match_id, season_Name, competition_Name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/dribble.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                
                cy.write(data)
            
            # Importing data for Bad Behaviour table
            with cursor.copy("COPY badBehaviour (event_id, playerName, teamName, cardId, cardName, match_id, season_name, competition_Name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/badBehaviour.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)

            # Importing data for Ball Receipt table
            with cursor.copy("COPY ballReceipt (event_id, playerName, teamName, outcomeId, outcomeName, match_id, season_name, competition_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/ballReceipt.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)

            # Importing data for Ball Recovery table
            with cursor.copy("COPY ballRecovery (event_id, playerName, teamName, offensive, recoveryFailure, match_id, season_name, competition_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/ballRecovery.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)

            # Importing data for Block table
            with cursor.copy("COPY Block (event_id, playerName, teamName, deflection, offensive, saveBlock, match_id, season_name, competition_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/block.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)

            # Importing data for Carry table
            with cursor.copy("COPY Carry (event_id, playerName, teamName, endLocationX, endLocationY, match_id, season_name, competition_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/carry.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)

            # Importing data for Clearance table
            with cursor.copy("COPY Clearance (event_id, playerName, teamName, bodyPartId, bodyPartName, aerialWon, match_id, season_name, competition_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/clearance.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)

            # Importing data for Duel table
            with cursor.copy("COPY Duel (event_id, playerName, teamName, typeId, typeName, outcomeId, outcomeName, match_id, season_name, competition_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/duel.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)

            # Importing data for Foul Committed table
            with cursor.copy("COPY foulCommitted (event_id, playerName, teamName, offensive, foulTypeId, foulTypeName, advantage, penalty, cardId, cardName, match_id, season_name, competition_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/foulCommitted.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)

            # Importing data for Foul Won table
            with cursor.copy("COPY foulWon (event_id, playerName, teamName, defensive, advantage, penalty, match_id, season_name, competition_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/foulWon.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)

            # Importing data for Goalkeeper table
            with cursor.copy("COPY Goalkeeper (event_id,  playerName, teamName, positionId, techniqueId, techniqueName, bodyPartId, bodyPartName, typeId, typeName, outcomeId, outcomeName, match_id, season_name, competition_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/goalkeeper.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)

            # Importing data for Interception table
            with cursor.copy("COPY Interception (event_id, playerName, teamName, outcomeId, outcomeName, match_id, season_name, competition_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/interception.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)

            # Importing data for Substitution table
            with cursor.copy("COPY Substitution (event_id, playerName, teamName, replacementId, replacementName, outcomeId, outcomeName, match_id, season_name, competition_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/substitution.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)
            
            with cursor.copy("COPY lineup (match_id, team_name, player_name, player_nickname,country_name) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/lineup.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)
            
            with cursor.copy("COPY lineupPositions (match_id, player_name, position_id, startFrom, endTo, from_period, to_period, start_reason, end_reason) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/lineupPositions.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)
            
            with cursor.copy("COPY lineupCards (match_id, player_name, card_time, card_type, card_reason, period) FROM STDIN WITH CSV HEADER") as cy:
                with open('CSV/lineupCards.csv', 'r', encoding='utf-8') as f:
                    data = f.read()
                cy.write(data)


            
def renewTables():
    tableNames = ['Countries', 'Competitions', 'Events', 'Referee', 'Manager', 'Team', 'Stadium', 'Match', 'Position', 'Player', 'Tactic', 'PlayPattern', 'Pass', 'Shot', 'Dribble', 'badBehaviour', 'ballReceipt', 'ballRecovery', 'Block', 'Carry', 'Clearance', 'Duel', 'foulCommitted', 'foulWon', 'Goalkeeper', 'Interception', 'Substitution', 'lineup', 'lineupPositions', 'lineupCards']
    indexNames = ['idx_shot_statsbomXg', 'idx_shot_playerName', 'idx_shot_firstTime', 'idx_pass_teamName', 'idx_pass_playerName', 'idx_shot_teamName', 'idx_dribble_outcomename']
    with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
        with db.cursor() as cursor:
            for index in indexNames:
                cursor.execute(f"DROP INDEX IF EXISTS {index} CASCADE")
            for table in tableNames:
                cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")


            with open('../Database/SCHEMA.sql', 'r') as file:
                cursor.execute(file.read())
        db.commit()



if __name__ == "__main__":
    start = time.time()
    renewTables()
    importToPostgreSQL()
    end = time.time()
    print(f"Time taken: {end - start} seconds")