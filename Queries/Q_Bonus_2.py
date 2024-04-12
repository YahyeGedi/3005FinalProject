import psycopg
import csv
import time

#BOX 1:
# x:0 y: 62
# x:0 y: 18
# x:18 y: 18
# x:18 y: 62

#BOX 2:
# x:102 y: 18
# x:120 y: 18
# x:120 y: 62
# x:102 y: 62

with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
    
    with db.cursor() as cursor:
        
        start_time = time.time()
        cursor.execute(
            f"""
            SELECT pass.teamname, COUNT(*) AS num_successful_passes
            FROM Pass 
            WHERE pass.season_name = '2020/2021'
                AND pass.competition_name = 'La Liga'
                AND pass.outcomename is NULL
                AND pass.endlocationx >= 0 AND pass.endlocationx <= 18 AND pass.endlocationy >= 18 AND pass.endlocationy <= 62
                OR pass.endlocationx >= 102 AND pass.endlocationx <= 120 AND pass.endlocationy >= 18 AND pass.endlocationy <= 62
            GROUP BY pass.teamName
            HAVING COUNT(*) >= 1
            ORDER BY num_successful_passes DESC;
            """
        )
        end_time = time.time()
        execution = end_time - start_time
        print(f"Execution time: {execution} seconds")
        result = cursor.fetchall()
    
    with open('Q_Bonus2.csv', 'w', encoding='utf-8', newline='') as file: # write the output to a csv file called Q_Bonus2.csv
        writer = csv.writer(file)
        writer.writerow(["team_name", "num_successful_passes_into_box"])
        for row in result:
            writer.writerow([row[0], row[1]])