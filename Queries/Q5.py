
# Q 5: In the Premier League season of 2003/2004, find the players who were the most intended
# recipients of passes. Sort them from highest to lowest

# To access intended recipients of passes: eventType Pass -> eventAttribute = "recipient"; meaning there is a pass made as there is a recipient
# we need to get the competition_name -> premierleague, season_name -> 2003/2004,
# get all the matches in that season, get all the events in those matches,
# we need to group by player_name from the events table and count the passes recieved

import psycopg
import csv
import time

with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
    
    with db.cursor() as cursor:
        
        # join the events and eventDetails table to get the player_name and count the passes recieved by checking event_attribute = 'recipient'
        start_time = time.time()

        #OLD QUERY IS WRONG
        # cursor.execute("SELECT player_name FROM events JOIN eventDetails ON events.event_id = eventDetails.event_id WHERE event_name = 'Pass' AND event_attribute = 'recipient' AND match_id IN (SELECT match_id FROM matches WHERE competition_id = 2 AND season_id = 44) GROUP BY player_name ORDER BY COUNT(event_value) DESC;") # this query will get the player_name and count the passes recieved
      
        cursor.execute(f"""
                        SELECT
                            recipient_Name AS player_name,
                            COUNT(*) AS num_passes_received
                        FROM
                            Pass
                        JOIN 
                            Player ON Pass.recipient_name = Player.player_name
                        WHERE
                            Pass.season_Name = '2003/2004' -- Assuming the season name format is 'YYYY/YYYY'
                            AND Pass.competition_Name = 'Premier League' -- Assuming the competition name is 'Premier League'
                        GROUP BY
                            recipient_Name
                        HAVING
                            COUNT(*) >= 1 -- Considering only players who received at least one pass
                        ORDER BY
                            num_passes_received DESC;
                       """) 
      
        end_time = time.time()
        execution = end_time - start_time
        print(f"Execution time: {execution} seconds")
        result = cursor.fetchall()
    
    with open('Q5.csv', 'w', encoding='utf-8', newline='') as file: # write the output to a csv file called Q_5.csv
        writer = csv.writer(file)
        writer.writerow(["player_name", "num_pass_recipients"])
        for row in result:
            player_name = row[0]
            num_pass_recipients = row[1]
            writer.writerow([player_name] + [num_pass_recipients])