import psycopg
import csv
import time

with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
        
    with db.cursor() as cursor:
        start = time.time()
        sql_query = """
            SELECT COUNT(*) AS num_shots_top , s.playername
            FROM Shot s
            WHERE s.season_name IN ('2019/2020', '2020/2021', '2018/2019')
                AND s.competition_name = 'La Liga'
                AND ((s.endlocationx = 120 AND s.endlocationy BETWEEN 36 AND 38.67 AND s.endlocationz BETWEEN 1.33 AND 2.67)
                OR (s.endlocationx = 0 AND s.endlocationy BETWEEN 36 AND 38.67 AND s.endlocationz BETWEEN 1.33 AND 2.67)
                OR (s.endlocationx = 0 AND s.endlocationy BETWEEN 41.33 AND 44 AND s.endlocationz BETWEEN 1.33 AND 2.67)
                OR (s.endlocationx = 120 AND s.endlocationy BETWEEN 41.33 AND 44 AND s.endlocationz BETWEEN 1.33 AND 2.67))
            GROUP BY s.playername
            HAVING COUNT(*) >= 1
            ORDER BY num_shots_top DESC;
        """
        cursor.execute(sql_query)
        end = time.time()
        print(f"Execution time: {end - start} seconds")
        result = cursor.fetchall()
    
    with open('Q_Bonus_1.csv', 'w', encoding='utf-8', newline='') as f: 
        bonus1 = csv.writer(f)
        bonus1.writerow(["player_name", "num_shots_top"])
        for row in result:
            player_name = row[0]
            num_shots_top = row[1]
            bonus1.writerow([num_shots_top, player_name])