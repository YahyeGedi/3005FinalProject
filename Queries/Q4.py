import psycopg
import csv
import time
with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
    with db.cursor() as cursor:
        start_time = time.time()
        cursor.execute(f"""
                        SELECT
                            teamName AS team_name,
                            COUNT(*) AS num_passes
                        FROM
                            Pass
                        WHERE
                            pass.season_Name = '2020/2021'
                            AND pass.competition_Name = 'La Liga'
                        GROUP BY
                            teamName
                        HAVING
                            COUNT(*) >= 1 -- Considering only teams that made at least one pass
                        ORDER BY
                            num_passes DESC;
                       """)        
        end_time = time.time()
        execution = end_time - start_time
        print(f"Execution time: {execution} seconds")
        result = cursor.fetchall()
    
    with open('Q4.csv', 'w', encoding='utf-8', newline='') as file: 
        writer = csv.writer(file)
        writer.writerow(["team_name", "num_passes"])
        for row in result:
            player_name = row[0]
            num_passes = row[1]
            writer.writerow([player_name] + [num_passes])