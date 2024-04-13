#Q8: Q 9: In the La Liga seasons of 2020/2021, 2019/2020, and 2018/2019 combined, find the players that
# were the most successful in completed dribbles. Sort them from highest to lowest

# event is dribble -> event_name = 'Dribble', event_attribute = 'outcome event_value = 'complete'
# seasonId's = 90, 42, 4
import psycopg
import csv
import time
# event value looks like this {'id': 8, 'name': 'Complete'}
# so the query would look like:

# SO event_name = 'Dribble', event_attribute = 'outcome' event_value = 'complete'

with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
        
    with db.cursor() as cursor:

        sql_query = """
            SELECT
                playerName AS player_name,
                COUNT(*) AS num_successful_dribbles
            FROM
                (
                    SELECT * FROM Dribble_2020_2021
                    UNION ALL
                    SELECT * FROM Dribble_2019_2020
                    UNION ALL
                    SELECT * FROM Dribble_2018_2019
                ) AS DribbleCombined 
            WHERE
                competition_Name = 'La Liga' 
                AND outcomeName = 'Complete' 
            GROUP BY
                playerName
            HAVING
                COUNT(*) >= 1 
            ORDER BY
                num_successful_dribbles DESC;

        """

# Now you can use the sql_query string in your code as needed.
        start_time = time.time()
        cursor.execute(sql_query)
        end_time = time.time()
        execution = end_time - start_time
        print(f"{execution} ")
        result = cursor.fetchall()
    
    with open('Q9.csv', 'w', encoding='utf-8', newline='') as file: # write the output to a csv file called Q_9.csv
        writer = csv.writer(file)
        writer.writerow(["player_name", "num_successful_dribbles"])
        for row in result:
            player_name = row[0]
            num_successful_dribbles = row[1]
            writer.writerow([player_name] + [num_successful_dribbles])