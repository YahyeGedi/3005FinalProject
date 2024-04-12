#Q6: In the Premier League season of 2003/2004, find the teams with the most shots made. Sort
#them from highest to lowest.

import psycopg
import csv
import time

with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
    
    with db.cursor() as cursor:
        start_time = time.time()
        cursor.execute(
            f"""
            SELECT
                teamName AS team_name,
                COUNT(*) AS num_shots
            FROM
                Shot
            WHERE
                shot.season_Name = '2003/2004'
                AND shot.competition_Name = 'Premier League'
            GROUP BY
                teamName
            HAVING
                COUNT(*) >= 1 -- Considering only teams that made at least one shot
            ORDER BY
                num_shots DESC;

            """
        )
        end_time = time.time()
        execution = end_time - start_time
        print(f"Execution time: {execution} seconds")
        result = cursor.fetchall()
    
    with open('Q6.csv', 'w', encoding='utf-8', newline='') as file: # write the output to a csv file called Q_6.csv
        writer = csv.writer(file)
        writer.writerow(["team_name", "num_shots"])
        for row in result:
            player_name = row[0]
            writer.writerow([player_name, row[1]])