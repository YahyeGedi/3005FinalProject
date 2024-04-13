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
                            (
                                SELECT * FROM Shot_2020_2021
                                UNION ALL
                                SELECT * FROM Shot_2019_2020
                                UNION ALL
                                SELECT * FROM Shot_2018_2019
                            ) AS ShotCombined 
                        WHERE
                            competition_Name = 'La Liga'
                            AND firstTime = TRUE 
                        GROUP BY
                            playerName
                        HAVING
                            COUNT(*) >= 1 
                        ORDER BY
                            num_first_time_shots DESC;
                       """)       
        end_time = time.time()
        execution = end_time - start_time
        print(f"{execution}")
        result = cursor.fetchall()
    
    with open('Q3.csv', 'w', encoding='utf-8', newline='') as file: 
        writer = csv.writer(file)
        writer.writerow(["player_name", "num_first_time_shots"])
        for row in result:
            player_name = row[0]
            num_first_time_shots = row[1]
            writer.writerow([player_name] + [num_first_time_shots])