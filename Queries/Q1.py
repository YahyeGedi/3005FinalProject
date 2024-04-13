import psycopg
import csv
import time

with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
    with db.cursor() as cursor:        
        # only select the player_name sorted by average_xG  in descending order using a subquery
        start_time = time.time()
        
        # any column where we perform a where join or an order by or a group by should be indexed

        # We are indexing only statsbomb_xg and player_name as we are using these columns in the query
        cursor.execute(f"""
                        SELECT
                            playerName AS player_name,
                            AVG(statsbomXg) AS average_xG
                        FROM
                            Shot_2020_2021
                        WHERE
                            statsbomXg > 0 
                            AND competition_Name = 'La Liga' 
                        GROUP BY
                            playerName
                        ORDER BY
                            average_xG DESC;
                       """)
        end_time = time.time()
        execution = end_time - start_time
        print(f"{execution}")
        result = cursor.fetchall()
    
    with open('Q1.csv', 'w', encoding='utf-8', newline='') as file: # write the output to a csv file called Q_1.csv
        writer = csv.writer(file)
        writer.writerow(["player_name", "avg_xG"])
        for row in result:
            player_name = row[0]
            avg_xG = row[1] 
            writer.writerow([player_name] + [avg_xG])