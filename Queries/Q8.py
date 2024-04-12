#Q8: In the La Liga season of 2020/2021, find the teams who made the most through balls. Sort
#them from highest to lowest.

import psycopg
import csv
import time

with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
    
    with db.cursor() as cursor:
        
        # join the events and eventDetails table to get the team_name and count the passes recieved by checking event_attribute = 'through_ball'
        start_time = time.time()
        cursor.execute(
            f"""
            SELECT
                teamName AS team_name,
                COUNT(*) AS num_through_balls
            FROM
                Pass
            WHERE
                pass.season_Name = '2020/2021' -- Assuming the season name format is 'YYYY/YYYY'
                AND pass.competition_name = 'La Liga' -- Assuming the competition name is 'La Liga'
                AND Pass.techniqueName = 'Through Ball' -- Filter for through ball passes
            GROUP BY
                teamName
            HAVING
                COUNT(*) >= 1 -- Considering only teams with at least one through ball made in a match
            ORDER BY
                num_through_balls DESC;

            """
        )
        end_time = time.time()
        execution = end_time - start_time
        print(f"Execution time: {execution} seconds")
        result = cursor.fetchall()
    
    with open('Q8.csv', 'w', encoding='utf-8', newline='') as file: # write the output to a csv file called Q_8.csv
        writer = csv.writer(file)
        writer.writerow(["player_name", "num_through_balls"])
        for row in result:
            player_name = row[0]
            writer.writerow([player_name, row[1]])