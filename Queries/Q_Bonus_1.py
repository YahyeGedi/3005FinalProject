# BONMUES: Divide the goal into 6 equal-size areas (top-left, top-middle, top-right, bottom-left, bottom-middle, and
# bottom-right). In the La Liga seasons of 2020/2021, 2019/2020, and 2018/2019 combined, find the players
# who shot the most in either the top-left or top-right corners. Sort them from highest to lowest
import psycopg
import csv
import time
# GOAL IS DIVIDED INTO 6 EQUAL-SIZE AREAS (HOME GOAL):
# TOP LEFT CORNER: 
# 1. (TOP LEFT part of box) x: 120 y: 36 z: 2.67
# 2. (TOP RIGHT PART OF BOX) x: 120, y: 38.67,z:  2.67
# 3. (BOTTOM LEFT PART OF BOX) x: 120, y: 36, z: 1.33
# 4. (BOTTOM RIGHT PART OF BOX) x: 120, y: 38.67, z: 1.33

#B BOTTOM LEFT CORNER:
# 1. (TOP LEFT part of box) x: 120 y: 36 z: 1.33
# 2. (TOP RIGHT PART OF BOX) x: 120, y: 38.67,z:  1.33
# 3. (BOTTOM LEFT PART OF BOX) x: 120, y: 36, z: 0
# 4. (BOTTOM RIGHT PART OF BOX) x: 120, y: 38.67, z: 0

# MIDDLE TOP CORNER:
# 1. (TOP LEFT part of box) x: 120 y: 38.67 z: 2.67
# 2. (TOP RIGHT PART OF BOX) x: 120, y: 41.33,z:  2.67
# 3. (BOTTOM LEFT PART OF BOX) x: 120, y: 38.67, z: 1.33
# 4. (BOTTOM RIGHT PART OF BOX) x: 120, y: 41.33, z: 1.33

# MIDDLE BOTTOM CORNER:
# 1. (TOP LEFT part of box) x: 120 y: 38.67 z: 1.33
# 2. (TOP RIGHT PART OF BOX) x: 120, y: 41.33,z:  1.33
# 3. (BOTTOM LEFT PART OF BOX) x: 120, y: 38.67, z: 0
# 4. (BOTTOM RIGHT PART OF BOX) x: 120, y: 41.33, z: 0

# TOP RIGHT CORNER:
# 1. (TOP LEFT part of box) x: 120 y: 41.33 z: 2.67
# 2. (TOP RIGHT PART OF BOX) x: 120, y: 44,z:  2.67
# 3. (BOTTOM LEFT PART OF BOX) x: 120, y: 41.33, z: 1.33
# 4. (BOTTOM RIGHT PART OF BOX) x: 120, y: 44, z: 1.33

# BOTTOM RIGHT CORNER:
# 1. (TOP LEFT part of box) x: 120 y: 41.33 z: 1.33
# 2. (TOP RIGHT PART OF BOX) x: 120, y: 44,z:  1.33
# 3. (BOTTOM LEFT PART OF BOX) x: 120, y: 41.33, z: 0
# 4. (BOTTOM RIGHT PART OF BOX) x: 120, y: 44, z: 0

# AWAY GOAL DIMENSIONS:
# COORDINATES ARE THE SAME, except x is 0 instead of 120

with psycopg.connect("dbname=project_database user=postgres password=1234") as db:
        
    with db.cursor() as cursor:
        
        sql_query = """
            SELECT s.playername, COUNT(*) AS num_shots_top_right_top_left
            FROM Shot s
            WHERE s.season_name IN ('2020/2021', '2019/2020', '2018/2019')
                AND s.competition_name = 'La Liga'
                AND (s.endlocationx = 120 AND s.endlocationy BETWEEN 36 AND 38.67 AND s.endlocationz BETWEEN 1.33 AND 2.67)
                OR (s.endlocationx = 120 AND s.endlocationy BETWEEN 41.33 AND 44 AND s.endlocationz BETWEEN 1.33 AND 2.67)
                OR (s.endlocationx = 0 AND s.endlocationy BETWEEN 36 AND 38.67 AND s.endlocationz BETWEEN 1.33 AND 2.67)
                OR (s.endlocationx = 0 AND s.endlocationy BETWEEN 41.33 AND 44 AND s.endlocationz BETWEEN 1.33 AND 2.67)
            GROUP BY s.playername
            HAVING COUNT(*) >= 1
            ORDER BY num_shots_top_right_top_left DESC;
        """
# Now you can use the sql_query string in your code as needed.
        start_time = time.time()
        cursor.execute(sql_query)
        end_time = time.time()
        execution = end_time - start_time
        print(f"Execution time: {execution} seconds")
        result = cursor.fetchall()
    
    with open('Q_Bonus_1.csv', 'w', encoding='utf-8', newline='') as file: # write the output to a csv file called Q_10.csv
        writer = csv.writer(file)
        writer.writerow(["player_name", "num_shots_top_right_top_left"])
        for row in result:
            player_name = row[0]
            writer.writerow([player_name, row[1]])