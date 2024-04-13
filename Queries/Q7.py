#Q7: In the La Liga season of 2020/2021, find the players who made the most through balls. Sort
#them from highest to lowest.


#This SQL query joins the events and eventDetails tables to retrieve the player names and count the passes that were through balls. 
#It filters events with the event name 'Pass' and event attribute 'through_ball'. 
#It also selects matches from the La Liga season of 2020/2021.

import psycopg
import csv
import time

with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
    
    with db.cursor() as cursor:
        
        # join the events and eventDetails table to get the player_name and count the passes recieved by checking event_attribute = 'through_ball'
        start_time = time.time()
        cursor.execute(f"""
                SELECT
                    playerName AS player_name,
                    COUNT(*) AS num_through_balls
                FROM
                    La_Liga_Pass_2020_2021
                WHERE
                    techniqueName = 'Through Ball' 
                GROUP BY
                    playerName
                HAVING
                    COUNT(*) >= 1 
                ORDER BY
                    num_through_balls DESC;
                       """)
        end_time = time.time()
        execution = end_time - start_time
        print(f"{execution}")
        result = cursor.fetchall()
    
    with open('Q7.csv', 'w', encoding='utf-8', newline='') as file: # write the output to a csv file called Q_7.csv
        writer = csv.writer(file)
        writer.writerow(["player_name", "num_through_balls"])
        for row in result:
            player_name = row[0]
            writer.writerow([player_name, row[1]])