#  3: In the La Liga seasons of 2020/2021, 2019/2020, and 2018/2019 combined, find the players with
# the most first-time shots. Sort them from highest to lowest.

import psycopg
import csv
import time

with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
    with db.cursor() as cursor:
        start_time = time.time()
        cursor.execute(f"""
                        SELECT
                            playerName AS player_name,
                            COUNT(*) AS num_first_time_shots
                        FROM
                            Shot
                        WHERE
                            season_Name IN ('2020/2021', '2019/2020', '2018/2019') -- Considering multiple seasons
                            AND competition_Name = 'La Liga' -- Assuming the competition name is 'La Liga'
                            AND firstTime = TRUE -- Considering only first-time shots
                        GROUP BY
                            playerName
                        HAVING
                            COUNT(*) >= 1 -- Considering only players who made at least one shot
                        ORDER BY
                            num_first_time_shots DESC;
                       """)       
        end_time = time.time()
        execution = end_time - start_time
        print(f"Execution time: {execution} seconds")
        result = cursor.fetchall()
    
    with open('Q3.csv', 'w', encoding='utf-8', newline='') as file: 
        writer = csv.writer(file)
        writer.writerow(["player_name", "num_first_time_shots"])
        for row in result:
            player_name = row[0]
            num_first_time_shots = row[1]
            writer.writerow([player_name] + [num_first_time_shots])