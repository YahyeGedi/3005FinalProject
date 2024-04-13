import psycopg
import csv
import time

with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
    with db.cursor() as cursor:
        start = time.time()
        cursor.execute(
            f"""
            SELECT teamname, COUNT(*) AS num_successful_passes
            FROM Pass 
            WHERE season_name = '2020/2021'
                AND competition_name = 'La Liga'
                AND outcomename is NULL
                AND (endlocationx >= 0 AND endlocationx <= 18 AND endlocationy >= 18 AND endlocationy <= 62
                OR endlocationx >= 102 AND endlocationx <= 120 AND endlocationy >= 18 AND endlocationy <= 62)
            GROUP BY pass.teamName
            HAVING COUNT(*) >= 1
            ORDER BY num_successful_passes DESC;
            """
        )
        end = time.time()
        print(f"Execution time: {end - start} seconds")
        result = cursor.fetchall()
    
    with open('Q_Bonus_2.csv', 'w', encoding='utf-8', newline='') as f: 
        bonus2 = csv.writer(f)
        bonus2.writerow(["team_name", "num_successful_passes"])
        for row in result:
            team_name = row[0]
            num_successful_passes = row[1]
            bonus2.writerow([team_name, num_successful_passes])