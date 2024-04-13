import psycopg
import csv
import time
with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
    with db.cursor() as cursor:
        start_time  = time.time()
        cursor.execute(f"""
                        SELECT
                            playerName AS player_name,
                            COUNT(*) AS num_shots
                        FROM
                            Shot_2020_2021
                        WHERE
                            competition_Name = 'La Liga'
                        GROUP BY
                            playerName
                        HAVING
                            COUNT(*) >= 1 
                        ORDER BY
                            num_shots DESC;
                       """)
        end_time = time.time()
        execution = end_time - start_time
        print(f"{execution}")

        result = cursor.fetchall()
    
    with open('Q2.csv', 'w', encoding='utf-8', newline='') as file: 
        writer = csv.writer(file)
        writer.writerow(["player_name", "num_shots"])
        for row in result:
            player_name = row[0]
            num_shots = row[1]
            writer.writerow([player_name] + [num_shots])