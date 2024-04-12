import requests
import json
import psycopg
import csv
import os
import time

import requests
import os

github_url = 'https://raw.githubusercontent.com/statsbomb/open-data/0067cae166a56aa80b2ef18f61e16158d6a7359a/data/events/'
json_extension = '.json'

competitions = []
seasons = []
countries = []
referee = []
manager = []
player = []
team = []
stadium = []
match = []
position = []
tactics = []
playPattern = []
events = []
passing = []
shot = []
dribble = []
badBehaviour = []
ballReceipt = []
ballRecovery = []
block = []
carry = []
clearance = []
duel = []
foulCommitted = []
foulWon = []
goalkeeper = []
interception = []
substitution = []


competitions.append({
    "competition_ID": 11,
    "competition_Name": "La Liga",
    "competition_Gender": "Male",
    "competition_Youth": False,
    "competition_International": False,
    "country_Name": "Spain"
})
competitions.append({
    "competition_ID": 2,
    "competition_Name": "Premier League",
    "competition_Gender": "Male",
    "competition_Youth": False,
    "competition_International": False,
    "country_Name": "England"
})


def populatingTablesFromMatch():
    matchesDir = './Matches/'
    for filename in os.listdir(matchesDir):
        if filename.endswith('.json'):
            filepath = os.path.join(matchesDir, filename)
            with open(filepath, encoding='utf-8') as file:
                data = json.load(file)
                for matchdata in data:
                    tempStadium = None
                    stadiumExists = False
                    tempReferee = None
                    refereeExists = False
                    homeManager = None
                    awayManager = None
                    managerExists = False

                    # Populating Seasons
                    if matchdata.get('season') is not None:
                        seasons.append({
                            "season_Id": matchdata['season']['season_id'],
                            "season_Name": matchdata['season']['season_name'],
                            "match_id" : matchdata['match_id'],
                            "competition_Id": matchdata['competition']['competition_id'],
                            "competition_Name": matchdata['competition']['competition_name']
                        })
                    
                    # Populating Stadium
                    if matchdata.get('stadium') is not None:
                        tempStadium = matchdata['stadium']['id']
                        for stadiumData in stadium:
                            if stadiumData['stadiums_Id'] == tempStadium:
                                stadiumExists = True
                                break
                        
                        if not stadiumExists:
                            stadium.append({
                                "stadiums_Id": tempStadium,
                                "stadiums_Name": matchdata['stadium']['name'],
                                "stadiums_Country": matchdata['stadium']['country']['name'],
                            })

                    # Populating Referee     
                    if matchdata.get('referee') is not None:
                        tempReferee = matchdata['referee']['id']
                        for refereeData in referee:
                            if refereeData['referees_Id'] == tempReferee:
                                refereeExists = True
                        
                        if not refereeExists:
                            referee.append({
                                "referees_Id": tempReferee,
                                "referees_Name": matchdata['referee']['name'],
                                "referees_Country": matchdata['referee']['country']['name'],
                            })

                    # Populating Managers
                    if matchdata.get('home_team') and matchdata['home_team'].get('managers'):
                        homeManager = matchdata['home_team']["managers"][0]['id']
                        if homeManager not in [manager['managers_Id'] for manager in manager]:
                            manager.append({
                                "managers_Id": homeManager,
                                "managers_Name": matchdata['home_team']["managers"][0]['name'],
                                "managers_Nickname" : matchdata['home_team']["managers"][0]['nickname'],
                                "managers_date_of_Birth": matchdata['home_team']["managers"][0]['dob'],
                                "managers_Country": matchdata['home_team']["managers"][0]['country']['name']
                            })
                    
                    # Populating Away Manager
                    if matchdata.get('away_team') and matchdata['away_team'].get('managers'):
                        awayManager = matchdata['away_team']["managers"][0]['id']
                        # for managerData in manager:
                        #     if managerData['managers_Id'] == awayManager:
                        #         managerExists = True
                        #         break
                        
                        # if managerExists == False:
                        #     manager.append({
                        #         "managers_Id": awayManager,
                        #         "managers_Name": matchdata['away_team']["managers"][0]['name'],
                        #         "managers_Nickname" : matchdata['away_team']["managers"][0]['nickname'],
                        #         "managers_date_of_Birth": matchdata['away_team']["managers"][0]['dob'],
                        #         "managers_Country": matchdata['away_team']["managers"][0]['country']['name']
                        #     })
                        awayManager = matchdata['away_team']["managers"][0]['id']
                        if awayManager not in [manager['managers_Id'] for manager in manager]:
                            manager.append({
                                "managers_Id": awayManager,
                                "managers_Name": matchdata['away_team']["managers"][0]['name'],
                                "managers_Nickname" : matchdata['away_team']["managers"][0]['nickname'],
                                "managers_date_of_Birth": matchdata['away_team']["managers"][0]['dob'],
                                "managers_Country": matchdata['away_team']["managers"][0]['country']['name']
                            })
                    

                    # Populating Teams   
                    if matchdata.get('home_team') and matchdata.get('home_team').get('home_team_id'):
                        teamExists = False
                        for teamData in team:
                            if teamData['teams_Id'] == matchdata['home_team']['home_team_id']:
                                teamExists = True
                                break
                        
                        if not teamExists:
                            team.append({
                                "teams_Id": matchdata['home_team']['home_team_id'],
                                "teams_Name": matchdata['home_team']['home_team_name'],
                                "teams_Gender": matchdata['home_team']['home_team_gender'],
                                "teams_Group": matchdata['home_team']['home_team_group'],
                                "teams_Country": matchdata['home_team']['country']['name'],
                                "managers_Id": homeManager
                            })

                    # Populating Away Team
                    if matchdata.get('away_team') and matchdata.get('away_team').get('away_team_id'):
                        teamExists = False
                        for teamData in team:
                            if teamData['teams_Id'] == matchdata['away_team']['away_team_id']:
                                teamExists = True
                                break
                        
                        if not teamExists:
                            team.append({
                                "teams_Id": matchdata['away_team']['away_team_id'],
                                "teams_Name": matchdata['away_team']['away_team_name'],
                                "teams_Gender": matchdata['away_team']['away_team_gender'],
                                "teams_Group": matchdata['away_team']['away_team_group'],
                                "teams_Country": matchdata['away_team']['country']['name'],
                                "managers_Id": awayManager
                            })

                    # Populating Countries
                    if matchdata.get('home_team') and matchdata.get('home_team').get('country').get('id'):
                        countryExists = False
                        for countryData in countries:
                            if countryData['country_id'] == matchdata['home_team']['country']['id']:
                                countryExists = True
                                break
                        
                        if not countryExists:
                            countries.append({
                                "country_id": matchdata['home_team']['country']['id'],
                                "country_name": matchdata['home_team']['country']['name'],
                            })

                    if matchdata.get('away_team') and matchdata.get('away_team').get('country').get('id'):
                        countryExists = False
                        for countryData in countries:
                            if countryData['country_id'] == matchdata['away_team']['country']['id']:
                                countryExists = True
                                break
                        
                        if not countryExists:
                            countries.append({
                                "country_id": matchdata['away_team']['country']['id'],
                                "country_name": matchdata['away_team']['country']['name'],
                            })


                    if matchdata.get('home_team') and matchdata.get('home_team').get('managers'):
                        countryExists = False
                        for countryData in countries:
                            if countryData['country_id'] == matchdata['home_team']['managers'][0]['country']['id']:
                                countryExists = True
                                break
                        
                        if not countryExists:
                            countries.append({
                                "country_id": matchdata['home_team']['managers'][0]['country']['id'],
                                "country_name": matchdata['home_team']['managers'][0]['country']['name'],
                            })

                    
                        if matchdata.get('away_team') and matchdata.get('away_team').get('managers'):
                            countryExists = False
                            for countryData in countries:
                                if countryData['country_id'] == matchdata['away_team']['managers'][0]['country']['id']:
                                    countryExists = True
                                    break
                            
                            if not countryExists:
                                countries.append({
                                    "country_id": matchdata['away_team']['managers'][0]['country']['id'],
                                    "country_name": matchdata['away_team']['managers'][0]['country']['name'],
                                })
                    
                    if matchdata.get('referee') and matchdata.get('referee').get('country').get('id'):
                        countryExists = False
                        for countryData in countries:
                            if countryData['country_id'] == matchdata['referee']['country']['id']:
                                countryExists = True
                                break
                        
                        if not countryExists:
                            countries.append({
                                "country_id": matchdata['referee']['country']['id'],
                                "country_name": matchdata['referee']['country']['name'],
                            })
                    
                    if matchdata.get('stadium') and matchdata.get('stadium').get('country').get('id'):
                        countryExists = False
                        for countryData in countries:
                            if countryData['country_id'] == matchdata['stadium']['country']['id']:
                                countryExists = True
                                break
                        
                        if not countryExists:
                            countries.append({
                                "country_id": matchdata['stadium']['country']['id'],
                                "country_name": matchdata['stadium']['country']['name'],
                            })
                    
                    match.append({
                        "matches_Id": matchdata['match_id'],
                        "competitions_Id" : matchdata['competition']['competition_id'],
                        "season_Id": matchdata['season']['season_id'],
                        "season_Name": matchdata['season']['season_name'],
                        "match_Date": matchdata['match_date'],
                        "kick_off_Time": matchdata['kick_off'],
                        "home_team_Name": matchdata['home_team']['home_team_name'],
                        "away_team_Name": matchdata['away_team']['away_team_name'],
                        "home_Score": matchdata['home_score'],
                        "away_Score": matchdata['away_score'],
                        "match_Week": matchdata['match_week'],
                        "competition_Stage": matchdata['competition_stage']['name'],
                        "stadium_Id": tempStadium,
                        "referee_Id": tempReferee,
                    })


def populatingTablesFromEvents():
    eventsDir = './Events/'
    for filename in os.listdir(eventsDir):
        for seasonData in seasons:
            if seasonData['match_id'] == int(filename.split(".")[0]):
                Season_Name = seasonData['season_Name']
                Competition_Name = seasonData['competition_Name']
        if filename.endswith('.json'):
            filepath = os.path.join(eventsDir, filename)
            with open(filepath, encoding='utf-8') as file:
                data = json.load(file)
                for eventdata in data:
                    # Populating Position
                    if (eventdata['type']['name'] == 'Starting XI'):
                        for positionData in eventdata['tactics']['lineup']:
                            positionExists = False
                            for data in position:
                                if data['position_Id'] == positionData['position']['id']:
                                    positionExists = True
                                    break
                            
                            if not positionExists:
                                position.append({
                                    "position_Id": positionData['position']['id'],
                                    "position_Name": positionData['position']['name'],
                                })
                    
                    if eventdata.get('position'):
                        positionExists = False
                        for data in position:
                            if data['position_Id'] == eventdata['position']['id']:
                                positionExists = True
                                break
                            
                        if not positionExists:
                            position.append({
                                "position_Id": eventdata['position']['id'],
                                "position_Name": eventdata['position']['name'],
                            })
                    
                    if eventdata.get('goalkeeper'):
                        if eventdata['goalkeeper'].get('position'):
                            positionExists = False
                            for data in position:
                                if data['position_Id'] == eventdata['goalkeeper']['position']['id']:
                                    positionExists = True
                                    break
                            
                            if not positionExists:
                                position.append({
                                    "position_Id": eventdata['goalkeeper']['position']['id'],
                                    "position_Name": eventdata['goalkeeper']['position']['name'],
                                })
                    # Populating Tactics
                    if eventdata.get('tactics') is not None:
                        tactics.append({
                            "match_id": filename.split(".")[0],
                            "event_id": eventdata['id'],
                            "formation": eventdata['tactics']['formation'],
                            "lineup": [int(player['player']['id']) for player in eventdata['tactics']['lineup']]
                        })


                    # Populating Player
                    if (eventdata['type']['name'] == 'Starting XI'):
                        if (eventdata.get('tactics')):
                            for playerdata in eventdata['tactics']['lineup']:
                                playerExists = False
                                for data in player:
                                    if data['player_Id'] == playerdata['player']['id']:
                                        playerExists = True
                                        break
                                
                                if not playerExists:
                                    player.append({
                                        "player_Id" : playerdata['player']['id'],
                                        "player_Name": playerdata['player']['name'],
                                        "position_Id" : playerdata['position']['id'],
                                        "jersey_Number": playerdata['jersey_number'],
                                        "team_Name": eventdata['team']['name']
                                    })
                                
                    if eventdata.get('player'):
                        for data in player:
                            if data['player_Id'] == eventdata['player']['id']:
                                playerExists = True
                                break
                                
                        if not playerExists:
                            player.append({
                            "player_Id" : playerdata['player']['id'],
                            "player_Name": playerdata['player']['name'],
                            "position_Id" : playerdata['position']['id'],
                            "jersey_Number": playerdata['jersey_number'],
                            "team_Name": eventdata['team']['name']
                        })
                    
                    if eventdata.get('shot'):
                        if eventdata['shot'].get('freeze_frame'):
                            for freezeframe in eventdata['shot']['freeze_frame']:
                               for data in player:
                                   if data['player_Id'] == freezeframe['player']['id']:
                                       playerExists = True
                                       break
                                
                               if not playerExists:
                                   player.append({
                                    "player_Id" : freezeframe['player']['id'],
                                    "player_Name": freezeframe['player']['name'],
                                    "position_Id" : freezeframe['position']['id'],
                                    "jersey_Number": None,
                                    "team_Name": eventdata['team']['name']
                                })
                                   
                    if eventdata['type']['name'] == 'Starting XI':
                        if eventdata.get('tactics'):
                            for playerdata in eventdata['tactics']['lineup']:
                                # Check if player already exists in playerArr
                                if not any(p['player_Id'] == playerdata['player']['id'] for p in player):
                                    player.append({
                                        "player_Id" : playerdata['player']['id'],
                                        "player_Name": playerdata['player']['name'],
                                        "position_Id" : playerdata['position']['id'],
                                        "jersey_Number": playerdata['jersey_number'],
                                        "team_Name": eventdata['team']['name']
                                    })       

                    if eventdata.get('player'):
                        if not any(p['player_Id'] == eventdata['player']['id'] for p in player):
                            player.append({
                                "player_Id" : eventdata['player']['id'],
                                "player_Name": eventdata['player']['name'],
                                "position_Id" : None,
                                "jersey_Number": None,
                                "team_Name": eventdata['team']['name']
                            })       

                    if eventdata.get('shot'):
                        if eventdata['shot'].get('freeze_frame'):
                            for frame in eventdata['shot']['freeze_frame']:
                                if not any(p['player_Id'] == frame['player']['id'] for p in player):
                                    player.append({
                                        "player_Id" : frame['player']['id'],
                                        "player_Name": frame['player']['name'],
                                        "position_Id" : frame['position']['id'] if 'position' in frame else None,
                                        "jersey_Number": None,
                                        "team_Name": eventdata['team']['name']
                                    })



                    # Populating Play Pattern
                    if eventdata.get('play_pattern') is not None:
                        playPatternExists = False
                        for data in playPattern:
                            if data['play_pattern_Id'] == eventdata['play_pattern']['id']:
                                playPatternExists = True
                                break
                        if not playPatternExists:
                            playPattern.append({
                                "play_pattern_Id": eventdata['play_pattern']['id'],
                                "play_pattern_Name": eventdata['play_pattern']['name']
                            })
                    
                    # Populating Events
                    if (eventdata.get('location')):
                        location_x = eventdata['location'][0]
                        location_y = eventdata['location'][1]
                    else:
                        location_x = None
                        location_y = None
                    
                    if (eventdata.get('counterpress')):
                        counterpress = eventdata['counterpress']
                    else:
                        counterpress = False
                    
                    if (eventdata.get('duration')):
                        duration = eventdata['duration']
                    else:
                        duration = None
                    
                    if (eventdata.get('player')):
                        playersname = eventdata['player']['name']
                    else:
                        playersname = None

                    if (eventdata.get('team')):
                        teamname = eventdata['team']['name']
                    else:
                        teamname = None
                    
                    if (eventdata.get('position')):
                        positionid = eventdata['position']['id']
                    else:
                        positionid = None
                  
                    events.append({
                        "event_id": eventdata['id'],
                        "event_Index" : eventdata['index'],
                        "event_Period": eventdata['period'],
                        "event_timeStamp": eventdata['timestamp'],
                        "event_Minute": eventdata['minute'],
                        "event_Second": eventdata['second'],
                        "type_Id": eventdata['type']['id'],
                        "type_Name": eventdata['type']['name'],
                        "possession": eventdata['possession'],
                        "possession_team_Name": eventdata['possession_team']['name'],
                        "play_pattern_Id": eventdata['play_pattern']['id'],
                        "location_x" : location_x,
                        "location_y" : location_y,
                        "event_Duration": duration,
                        "counterpress": counterpress,
                        "position_Id": positionid,
                        "team_Name": teamname,
                        "player_Name": playersname,
                        "match_id": filename.split(".")[0],
                        "season_Name" : Season_Name,
                        "competition_Name": Competition_Name
                    })
                
                    
                    # Populating Passing
                    if (eventdata['type']['name'] == 'Pass'):
                        if (eventdata.get('player')):
                            playersname = eventdata['player']['name']
                        else:
                            playersname = None

                        if (eventdata.get('team')):
                            teamname = eventdata['team']['name']
                        else:
                            teamname = None

                        # Extract recipient ID
                        recipient_name = eventdata['pass']['recipient']['name'] if eventdata.get('pass') and eventdata['pass'].get('recipient') else None

                        # Extract pass length
                        length = eventdata['pass']['length'] if eventdata.get('pass') and eventdata['pass'].get('length') else None

                        # Extract pass angle
                        angle = eventdata['pass']['angle'] if eventdata.get('pass') and eventdata['pass'].get('angle') else None

                        # Extract pass height ID and name
                        if eventdata.get('pass') and eventdata['pass'].get('height'):
                            height_id = eventdata['pass']['height']['id']
                            height_name = eventdata['pass']['height']['name']
                        else:
                            height_id = None
                            height_name = None

                        # Check if pass was aerial won
                        aerial_won = eventdata['pass']['aerial_won'] if eventdata.get('pass') and eventdata['pass'].get('aerial_won') else False

                        # Extract end location
                        if eventdata.get('pass') and eventdata['pass'].get('end_location'):
                            end_location_x = eventdata['pass']['end_location'][0]
                            end_location_y = eventdata['pass']['end_location'][1]
                        else:
                            end_location_x = None
                            end_location_y = None

                        # Extract assisted shot ID
                        assisted_shot_id = eventdata['pass']['assisted_shot_id'] if eventdata.get('pass') and eventdata['pass'].get('assisted_shot_id') else None

                        # Check if pass was deflected
                        deflected = eventdata['pass']['deflected'] if eventdata.get('pass') and eventdata['pass'].get('deflected') else False

                        # Check if there was miscommunication
                        miscommunication = eventdata['pass']['miscommunication'] if eventdata.get('pass') and eventdata['pass'].get('miscommunication')else False

                        # Check if pass was a cross
                        is_cross = eventdata['pass']['cross'] if eventdata.get('pass') and eventdata['pass'].get('cross') else False

                        # Check if pass was a cut back
                        cut_back = eventdata['pass']['cut_back'] if eventdata.get('pass') and eventdata['pass'].get('cut_back') else False

                        # Check if pass was a switch
                        switch = eventdata['pass']['switch'] if eventdata.get('pass') and eventdata['pass'].get('switch') else False

                        # Check if pass was a shot assist
                        shot_assist = eventdata['pass']['shot_assist'] if eventdata.get('pass') and eventdata['pass'].get('shot_assist') else False

                        # Check if pass was a goal assist
                        goal_assist = eventdata['pass']['goal_assist'] if eventdata.get('pass') and eventdata['pass'].get('goal_assist') else False

                        # Extract body part ID and name
                        if eventdata.get('pass') and eventdata['pass'].get('body_part'):
                            body_part_id = eventdata['pass']['body_part']['id']
                            body_part_name = eventdata['pass']['body_part']['name']
                        else:
                            body_part_id = None
                            body_part_name = None

                        # Extract pass type ID and name
                        if eventdata.get('pass') and eventdata['pass'].get('type'):
                            pass_type_id = eventdata['pass']['type']['id']
                            pass_type_name = eventdata['pass']['type']['name']
                        else:
                            pass_type_id = None
                            pass_type_name = None

                        # Extract pass outcome ID and name
                        if eventdata.get('pass') and eventdata['pass'].get('outcome'):
                            outcome_id = eventdata['pass']['outcome']['id']
                            outcome_name = eventdata['pass']['outcome']['name']
                        else:
                            outcome_id = None
                            outcome_name = None

                        # Extract pass technique ID and name
                        if eventdata.get('pass') and eventdata['pass'].get('technique'):
                            technique_id = eventdata['pass']['technique']['id']
                            technique_name = eventdata['pass']['technique']['name']
                        else:
                            technique_id = None
                            technique_name = None
                        
                        passing.append({
                            "event_id": eventdata['id'],
                            "playerName": playersname,
                            "teamName": teamname,
                            "recipient_Name": recipient_name,
                            "pass_Length": length,
                            "angle": angle,
                            "heightId": height_id,
                            "heightName": height_name,
                            "aerialWon": aerial_won,
                            "endLocationX": end_location_x,
                            "endLocationY": end_location_y,
                            "assistedShotId": assisted_shot_id,
                            "deflected": deflected,
                            "miscommunication": miscommunication,
                            "isCross": is_cross,
                            "cutBack": cut_back,
                            "switch": switch,
                            "shotAssist": shot_assist,
                            "goalAssist": goal_assist,
                            "bodyPartId": body_part_id,
                            "bodyPartName": body_part_name,
                            "typeId": pass_type_id,
                            "typeName": pass_type_name,
                            "outcomeId": outcome_id,
                            "outcomeName": outcome_name,
                            "techniqueId": technique_id,
                            "techniqueName": technique_name,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })

                    # Populating Shots
                    if eventdata['type']['name'] == 'Shot':
                        # Extract player information
                        if 'player' in eventdata:
                            player_id = eventdata['player']['id']
                            player_name = eventdata['player']['name']
                        else:
                            player_id = None
                            player_name = None

                        # Extract team information
                        if 'team' in eventdata:
                            team_id = eventdata['team']['id']
                            team_name = eventdata['team']['name']
                        else:
                            team_id = None
                            team_name = None

                        # Extract statsbomb_xg
                        statsbomb_xg = eventdata['shot']['statsbomb_xg'] if 'shot' in eventdata and 'statsbomb_xg' in eventdata['shot'] else None

                        # Extract end location
                        if 'shot' in eventdata and eventdata['shot'].get('end_location'):
                            end_location_x = eventdata['shot']['end_location'][0]
                            end_location_y = eventdata['shot']['end_location'][1]
                            end_location_z = eventdata['shot']['end_location'][2] if len(eventdata['shot']['end_location']) > 2 else None
                        else:
                            end_location_x = None
                            end_location_y = None
                            end_location_z = None

                        # Extract boolean attributes
                        follows_dribble = eventdata['shot']['follows_dribble'] if 'shot' in eventdata and 'follows_dribble' in eventdata['shot'] else False
                        first_time = eventdata['shot']['first_time'] if 'shot' in eventdata and 'first_time' in eventdata['shot'] else False
                        open_goal = eventdata['shot']['open_goal'] if 'shot' in eventdata and 'open_goal' in eventdata['shot'] else False
                        deflected = eventdata['shot']['deflected'] if 'shot' in eventdata and 'deflected' in eventdata['shot'] else False

                        # Extract technique information
                        technique_id = eventdata['shot']['technique']['id'] if 'shot' in eventdata and 'technique' in eventdata['shot'] else None
                        technique_name = eventdata['shot']['technique']['name'] if 'shot' in eventdata and 'technique' in eventdata['shot'] else None

                        # Extract body part information
                        body_part_id = eventdata['shot']['body_part']['id'] if 'shot' in eventdata and 'body_part' in eventdata['shot'] else None
                        body_part_name = eventdata['shot']['body_part']['name'] if 'shot' in eventdata and 'body_part' in eventdata['shot'] else None

                        # Extract type information
                        type_id = eventdata['shot']['type']['id'] if 'shot' in eventdata and 'type' in eventdata['shot'] else None
                        type_name = eventdata['shot']['type']['name'] if 'shot' in eventdata and 'type' in eventdata['shot'] else None

                        # Extract outcome information
                        outcome_id = eventdata['shot']['outcome']['id'] if 'shot' in eventdata and 'outcome' in eventdata['shot'] else None
                        outcome_name = eventdata['shot']['outcome']['name'] if 'shot' in eventdata and 'outcome' in eventdata['shot'] else None

                        # Extract season and competition information
                        shot.append({
                            "event_id": eventdata['id'],
                            "playerName": player_name,
                            "teamName": team_name,
                            "statsbombXg": statsbomb_xg,
                            "endLocationX": end_location_x,
                            "endLocationY": end_location_y,
                            "endLocationZ": end_location_z,
                            "followsDribble": follows_dribble,
                            "firstTime": first_time,
                            "openGoal": open_goal,
                            "deflected": deflected,
                            "techniqueId": technique_id,
                            "techniqueName": technique_name,
                            "bodyPartId": body_part_id,
                            "bodyPartName": body_part_name,
                            "typeId": type_id,
                            "typeName": type_name,
                            "outcomeId": outcome_id,
                            "outcomeName": outcome_name,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })

                    # Populating Dribbles
                    if eventdata['type']['name'] == 'Dribble':
                        # Extract team information
                        if 'team' in eventdata:
                            team_id = eventdata['team']['id']
                            team_name = eventdata['team']['name']
                        else:
                            team_id = None
                            team_name = None
                        
                        # Extract player information
                        if 'player' in eventdata:
                            player_id = eventdata['player']['id']
                            player_name = eventdata['player']['name']
                        else:
                            player_id = None
                            player_name = None
                        
                        # Extract dribble attributes
                        if 'dribble' in eventdata:
                            overrun = eventdata['dribble'].get('overrun', False)
                            nutmeg = eventdata['dribble'].get('nutmeg', False)
                            no_touch = eventdata['dribble'].get('no_touch', False)
                            
                            # Extract outcome information
                            outcome_id = eventdata['dribble']['outcome']['id'] if 'outcome' in eventdata['dribble'] else None
                            outcome_name = eventdata['dribble']['outcome']['name'] if 'outcome' in eventdata['dribble'] else None
                        else:
                            overrun = False
                            nutmeg = False
                            no_touch = False
                            outcome_id = None
                            outcome_name = None

                         # Extract season and competition information
                        dribble.append({
                            "event_id": eventdata['id'],
                            "playerName": player_name,
                            "teamName": team_name,
                            "overrun": overrun,
                            "nutmeg": nutmeg,
                            "noTouch": no_touch,
                            "outcomeId": outcome_id,
                            "outcomeName": outcome_name,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })

                    # Populating Bad Behaviour
                    if eventdata['type']['name'] == 'Bad Behaviour':
                        # Extract card information
                        if 'bad_behaviour' in eventdata:
                            card_id = eventdata['bad_behaviour']['card']['id'] if 'card' in eventdata['bad_behaviour'] else None
                            card_name = eventdata['bad_behaviour']['card']['name'] if 'card' in eventdata['bad_behaviour'] else None
                        else:
                            card_id = None
                            card_name = None
                        
                        # Extract player information
                        if 'player' in eventdata:
                            player_id = eventdata['player']['id']
                            player_name = eventdata['player']['name']
                        else:
                            player_id = None
                            player_name = None
                        
                        # Extract team information
                        if 'team' in eventdata:
                            team_id = eventdata['team']['id']
                            team_name = eventdata['team']['name']
                        else:
                            team_id = None
                            team_name = None

                        # Extract season and competition information
                        badBehaviour.append({
                            "event_id": eventdata['id'],
                            "playerName": player_name,
                            "teamName": team_name,
                            "cardId": card_id,
                            "cardName": card_name,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })
                    
                    # Populating Ball Receipt
                    if eventdata['type']['name'] == 'Ball Receipt*':
                        # Extract player information
                        player_name = eventdata['player']['name'] if 'player' in eventdata else None

                        # Extract team information
                        team_name = eventdata['team']['name'] if 'team' in eventdata else None

                        # Extract outcome information
                        if 'ball_receipt' in eventdata:
                            outcome_id = eventdata['ball_receipt']['outcome']['id'] if 'outcome' in eventdata['ball_receipt'] else None
                            outcome_name = eventdata['ball_receipt']['outcome']['name'] if 'outcome' in eventdata['ball_receipt'] else None
                        else:
                            outcome_id = None
                            outcome_name = None
                        
                        # Extract season and competition information
                        ballReceipt.append({
                            "event_id": eventdata['id'],
                            "playerName": player_name,
                            "teamName": team_name,
                            "outcomeId": outcome_id,
                            "outcomeName": outcome_name,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })
                    # Populating Ball Recovery
                    if eventdata['type']['name'] == 'Ball Recovery':
                        # Extract player information
                        player_name = eventdata['player']['name'] if 'player' in eventdata else None

                        # Extract team information
                        team_name = eventdata['team']['name'] if 'team' in eventdata else None

                        # Extract ball recovery information
                        if 'ball_recovery' in eventdata:
                            offensive = eventdata['ball_recovery']['offensive'] if 'offensive' in eventdata['ball_recovery'] else False
                            ball_recovery = eventdata['ball_recovery']['recovery_failure'] if 'recovery_failure' in eventdata['ball_recovery'] else False
                        else:
                            ball_recovery = False
                            offensive = False
                        
                        # Extract season and competition information
                        ballRecovery.append({
                            "event_id": eventdata['id'],
                            "playerName": player_name,
                            "teamName": team_name,
                            "offensive": offensive,
                            "recoveryFailure": ball_recovery,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })

                    # Populating Block
                    if eventdata['type']['name'] == 'Block':
                        if eventdata.get('block'):
                            # Extract player information
                            player_name = eventdata['player']['name'] if 'player' in eventdata else None

                            # Extract team information
                            team_name = eventdata['team']['name'] if 'team' in eventdata else None

                            # Extract block information
                            deflection = eventdata['block']['deflection'] if 'deflection' in eventdata['block'] else False
                            offensive = eventdata['block']['offensive'] if 'offensive' in eventdata['block'] else False
                            save_block = eventdata['block']['block'] if 'block' in eventdata['block'] else False
                        else:
                            deflection = False
                            offensive = False
                            save_block = False
                        
                        # Extract season and competition information
                        block.append({
                            "event_id": eventdata['id'],
                            "playerName": player_name,
                            "teamName": team_name,
                            "deflection": deflection,
                            "offensive": offensive,
                            "saveBlock": save_block,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })

                    # Populating Carry
                    if eventdata['type']['name'] == 'Carry':
                        if eventdata.get('carry'):
                            # Extract player information
                            player_name = eventdata['player']['name'] if 'player' in eventdata else None

                            # Extract team information
                            team_name = eventdata['team']['name'] if 'team' in eventdata else None

                            # Extract carry end location
                            if eventdata.get('carry'):
                                carry_x = eventdata['carry']['end_location'][0]
                                carry_y = eventdata['carry']['end_location'][1]
                            else:
                                carry_x = None
                                carry_y = None
                        else:
                            carry_x = None
                            carry_y = None
                        
                        # Extract season and competition information
                        carry.append({
                            "event_id": eventdata['id'],
                            "playerName": player_name,
                            "teamName": team_name,
                            "endLocationX": carry_x,
                            "endLocationY": carry_y,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })

                    # Populating Clearance
                    if eventdata['type']['name'] == 'Clearance':
                        if eventdata.get('clearance'):
                            # Extract player information
                            player_name = eventdata['player']['name'] if 'player' in eventdata else None

                            # Extract team information
                            team_name = eventdata['team']['name'] if 'team' in eventdata else None

                            # Extract body part information
                            if eventdata.get('clearance') and eventdata['clearance'].get('body_part'):
                                body_part_id = eventdata['clearance']['body_part']['id']
                                body_part_name = eventdata['clearance']['body_part']['name']
                            else:
                                body_part_id = None
                                body_part_name = None

                            # Extract aerial won status
                            aerial_won = eventdata['clearance']['aerial_won'] if eventdata.get('clearance') and eventdata['clearance'].get('aerial_won') else False
                        else:
                            body_part_id = None
                            body_part_name = None
                            aerial_won = False
                        
                        # Extract season and competition information
                        clearance.append({
                            "event_id": eventdata['id'],
                            "playerName": player_name,
                            "teamName": team_name,
                            "bodyPartId": body_part_id,
                            "bodyPartName": body_part_name,
                            "aerialWon": aerial_won,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })

                    # Populating Duel
                    if eventdata['type']['name'] == 'Duel':
                        # Extract team information
                        team_name = eventdata['team']['name'] if 'team' in eventdata else None

                        # Extract player information
                        player_name = eventdata['player']['name'] if 'player' in eventdata else None

                        # Extract duel type ID and name
                        if eventdata.get('duel') and eventdata['duel'].get('type'):
                            duel_type_id = eventdata['duel']['type']['id']
                            duel_type_name = eventdata['duel']['type']['name']
                        else:
                            duel_type_id = None
                            duel_type_name = None

                        # Extract outcome ID and name
                        if eventdata.get('duel') and eventdata['duel'].get('outcome'):
                            outcome_id = eventdata['duel']['outcome']['id']
                            outcome_name = eventdata['duel']['outcome']['name']
                        else:
                            outcome_id = None
                            outcome_name = None

                        # Extract season and competition information
                        duel.append({
                            "event_id": eventdata['id'],
                            "playerName": player_name,
                            "teamName": team_name,
                            "typeId": duel_type_id,
                            "typeName": duel_type_name,
                            "outcomeId": outcome_id,
                            "outcomeName": outcome_name,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })
                    
                    # Populating Foul Committed
                    if eventdata['type']['name'] == 'Foul Committed':
                        # Extract team information
                        team_name = eventdata['team']['name'] if 'team' in eventdata else None

                        # Extract player information
                        player_name = eventdata['player']['name'] if 'player' in eventdata else None

                        if eventdata.get('foul_committed'):
                            # Extract offensive flag
                            offensive = eventdata['foul_committed']['offensive'] if eventdata['foul_committed'].get('offensive') else False

                            # Extract foul type ID and name
                            if eventdata['foul_committed'].get('type'):
                                foul_type_id = eventdata['foul_committed']['type']['id']
                                foul_type_name = eventdata['foul_committed']['type']['name']
                            else:
                                foul_type_id = None
                                foul_type_name = None

                            # Extract advantage flag
                            advantage = eventdata['foul_committed']['advantage'] if eventdata['foul_committed'].get('advantage') else False

                            # Extract penalty flag
                            penalty = eventdata['foul_committed']['penalty'] if eventdata['foul_committed'].get('penalty') else False

                            # Extract card ID and name
                            if eventdata['foul_committed'].get('card'):
                                card_id = eventdata['foul_committed']['card']['id']
                                card_name = eventdata['foul_committed']['card']['name']
                            else:
                                card_id = None
                                card_name = None
                        else:
                            offensive = False
                            foul_type_id = None
                            foul_type_name = None
                            advantage = False
                            penalty = False
                            card_id = None
                            card_name = None
                        
                        # Extract season and competition information
                        foulCommitted.append({
                            "event_id": eventdata['id'],
                            "playerName": player_name,
                            "teamName": team_name,
                            "offensive": offensive,
                            "foulTypeId": foul_type_id,
                            "foulTypeName": foul_type_name,
                            "advantage": advantage,
                            "penalty": penalty,
                            "cardId": card_id,
                            "cardName": card_name,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })

                    # Populating Foul Won
                    if eventdata['type']['name'] == 'Foul Won':
                        # Extract team information
                        team_name = eventdata['team']['name'] if 'team' in eventdata else None

                        # Extract player information
                        player_name = eventdata['player']['name'] if 'player' in eventdata else None

                        if eventdata.get('foul_won'):
                            # Extract defensive flag
                            defensive = eventdata['foul_won']['defensive'] if eventdata['foul_won'].get('defensive') else False

                            # Extract advantage flag
                            advantage = eventdata['foul_won']['advantage'] if eventdata['foul_won'].get('advantage') else False

                            # Extract penalty flag
                            penalty = eventdata['foul_won']['penalty'] if eventdata['foul_won'].get('penalty') else False
                        else:
                            defensive = False
                            advantage = False
                            penalty = False
                        
                        # Extract season and competition information
                        foulWon.append({
                            "event_id": eventdata['id'],
                            "playerName": player_name,
                            "teamName": team_name,
                            "defensive": defensive,
                            "advantage": advantage,
                            "penalty": penalty,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })

                    # Populating Goalkeeper
                    if 'type' in eventdata and eventdata['type']['name'] == "Goal Keeper":
                        # Extract team information
                        team_name = eventdata['team']['name'] if 'team' in eventdata else None

                        # Extract player information
                        player_name = eventdata['player']['name'] if 'player' in eventdata else None

                        # Extract position information
                        if eventdata.get('goalkeeper') and eventdata['goalkeeper'].get('position'):
                            position_id = eventdata['goalkeeper']['position']['id']
                            position_name = eventdata['goalkeeper']['position']['name']
                        else:
                            position_id = None
                            position_name = None

                        # Extract technique information
                        if eventdata.get('goalkeeper') and eventdata['goalkeeper'].get('technique'):
                            technique_id = eventdata['goalkeeper']['technique']['id']
                            technique_name = eventdata['goalkeeper']['technique']['name']
                        else:
                            technique_id = None
                            technique_name = None

                        # Extract body part information
                        if eventdata.get('goalkeeper') and eventdata['goalkeeper'].get('body_part'):
                            body_part_id = eventdata['goalkeeper']['body_part']['id']
                            body_part_name = eventdata['goalkeeper']['body_part']['name']
                        else:
                            body_part_id = None
                            body_part_name = None

                        # Extract type information
                        if eventdata.get('goalkeeper') and eventdata['goalkeeper'].get('type'):
                            type_id = eventdata['goalkeeper']['type']['id']
                            type_name = eventdata['goalkeeper']['type']['name']
                        else:
                            type_id = None
                            type_name = None

                        # Extract outcome information
                        if eventdata.get('goalkeeper') and eventdata['goalkeeper'].get('outcome'):
                            outcome_id = eventdata['goalkeeper']['outcome']['id']
                            outcome_name = eventdata['goalkeeper']['outcome']['name']
                        else:
                            outcome_id = None
                            outcome_name = None
                        
                        # Extract season and competition information
                        goalkeeper.append({
                            "event_id": eventdata['id'],
                            "playerName": player_name,
                            "teamName": team_name,
                            "positionId": position_id,
                            "techniqueId": technique_id,
                            "techniqueName": technique_name,
                            "bodyPartId": body_part_id,
                            "bodyPartName": body_part_name,
                            "typeId": type_id,
                            "typeName": type_name,
                            "outcomeId": outcome_id,
                            "outcomeName": outcome_name,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })
                    
                    # Populating Interception
                    if 'type' in eventdata and eventdata['type']['name'] == 'Interception':
                        # Extract team information
                        team_id = eventdata['team']['id'] if 'team' in eventdata else None
                        team_name = eventdata['team']['name'] if 'team' in eventdata else None

                        # Extract player information
                        player_id = eventdata['player']['id'] if 'player' in eventdata else None
                        player_name = eventdata['player']['name'] if 'player' in eventdata else None

                        # Extract outcome information
                        if eventdata.get('interception') and eventdata['interception'].get('outcome'):
                            outcome_id = eventdata['interception']['outcome']['id']
                            outcome_name = eventdata['interception']['outcome']['name']
                        else:
                            outcome_id = None
                            outcome_name = None

                        # Extract season and competition information
                        interception.append({
                            "event_id": eventdata['id'],
                            "playerName": player_name,
                            "teamName": team_name,
                            "outcomeId": outcome_id,
                            "outcomeName": outcome_name,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })

                    # Populating Substitution
                    if 'type' in eventdata and eventdata['type']['name'] == 'Substitution':
                        # Extract team information
                        team_id = eventdata['team']['id'] if 'team' in eventdata else None
                        team_name = eventdata['team']['name'] if 'team' in eventdata else None

                        # Extract player information
                        player_id = eventdata['player']['id'] if 'player' in eventdata else None
                        player_name = eventdata['player']['name'] if 'player' in eventdata else None

                        # Extract replacement information
                        if eventdata.get('substitution') and eventdata['substitution'].get('replacement'):
                            replacement_id = eventdata['substitution']['replacement']['id']
                            replacement_name = eventdata['substitution']['replacement']['name']
                        else:
                            replacement_id = None
                            replacement_name = None

                        # Extract outcome information
                        if eventdata.get('substitution') and eventdata['substitution'].get('outcome'):
                            outcome_id = eventdata['substitution']['outcome']['id']
                            outcome_name = eventdata['substitution']['outcome']['name']
                        else:
                            outcome_id = None
                            outcome_name = None

                        # Extract season and competition information
                        substitution.append({
                            "event_id": eventdata['id'],
                            "playerName": player_name,
                            "teamName": team_name,
                            "replacementId": replacement_id,
                            "replacementName": replacement_name,
                            "outcomeId": outcome_id,
                            "outcomeName": outcome_name,
                            "match_id": filename.split(".")[0],
                            "season_Name": Season_Name,
                            "competition_Name": Competition_Name
                        })


def toCSV():
    # Writing counries to CSV
    with open('CSV/countries.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Country ID", "Country Name"])
        for data in countries:
            writer.writerow([data['country_id'], data['country_name']])
    
    # Writing competitions to CSV
    with open('CSV/competitions.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Competition ID", "Competition Name", "Competition Gender", "Competition Youth", "Competition International", "Country Name"])
        for data in competitions:
            writer.writerow([data['competition_ID'], data['competition_Name'], data['competition_Gender'], data['competition_Youth'], data['competition_International'], data['country_Name']])

    # Writing Referees to CSV
    with open('CSV/referees.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Referee ID", "Referee Name", "Referee Country"], )
        for data in referee:
            writer.writerow([data['referees_Id'], data['referees_Name'], data['referees_Country']])
    
    # Writing Stadium to CSV
    with open('CSV/stadium.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Stadium ID", "Stadium Name", "Stadium Country"])
        for data in stadium:
            writer.writerow([data['stadiums_Id'], data['stadiums_Name'], data['stadiums_Country']])
    
    # Writing Managers to CSV
    with open('CSV/managers.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Manager ID", "Manager Name", "Manager Nickname", "Manager DOB", "Manager Country"])
        for data in manager:
            writer.writerow([data['managers_Id'], data['managers_Name'], data['managers_Nickname'], data['managers_date_of_Birth'], data['managers_Country']])
        
    # Writing teams to CSV
    with open('CSV/teams.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Team ID", "Team Name", "Team Gender", "Team Group", "Team Country", "Team Manager"])
        for data in team:
            writer.writerow([data['teams_Id'], data['teams_Name'], data['teams_Gender'], data['teams_Group'], data['teams_Country'], data['managers_Id']])
    
    # Writing Match to CSV
    with open('CSV/match.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Match ID", "Competition ID", "Season Id", "Season Name", "Match Date", "Kick Off Time", "Home Team Name", "Away Team Name", "Home Score", "Away Score", "Match Week", "Competition Stage", "Stadium ID", "Referee ID"])
        for data in match:
            writer.writerow([data['matches_Id'], data['competitions_Id'], data['season_Id'], data['season_Name'], data['match_Date'], data['kick_off_Time'], data['home_team_Name'], data['away_team_Name'], data['home_Score'], data['away_Score'], data['match_Week'], data['competition_Stage'], data['stadium_Id'], data['referee_Id']])
        
    # Writing Position to CSV
    with open('CSV/position.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Position ID", "Position Name"])
        for data in position:
            writer.writerow([data['position_Id'], data['position_Name']])

    # Writing Player to CSV
    with open('CSV/player.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Player ID", "Player Name", "Position ID", "Jersey Number", "Team Name"])
        for data in player:
            writer.writerow([data['player_Id'], data['player_Name'], data['position_Id'], data['jersey_Number'], data['team_Name']])

    # Writing tactics to CSV
    with open('CSV/tactic.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Match ID", "Event ID", "Formation", "Lineup"])
        for data in tactics:
            # Convert lineup to PostgreSQL array format
            lineup_str = "{" + ','.join(map(str, data['lineup'])) + "}"
            writer.writerow([data['match_id'], data['event_id'], data['formation'], lineup_str])


    # Writing PlayPattern to CSV
    with open('CSV/play_pattern.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Play Pattern ID", "Play Pattern Name"])
        for data in playPattern:
            writer.writerow([data['play_pattern_Id'], data['play_pattern_Name']])

    # Writing Events to CSV
    with open('CSV/events.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Event Index", "Event Period", "Event TimeStamp", "Event Minute", "Event Second", "Type ID", "Type Name", "Possession", "Possession Team Name", "Play Pattern ID", "Location X", "Location Y", "Event Duration", "Counterpress", "Position ID", "Team Name", "Player Name", "Match ID", "Season Name", "Competition Name"])
        for data in events:
            writer.writerow([data['event_id'], data['event_Index'], data['event_Period'], data['event_timeStamp'], data['event_Minute'], data['event_Second'], data['type_Id'], data['type_Name'], data['possession'], data['possession_team_Name'], data['play_pattern_Id'], data['location_x'], data['location_y'], data['event_Duration'], data['counterpress'], data['position_Id'], data['team_Name'], data['player_Name'], data['match_id'], data['season_Name'], data['competition_Name']])

    # Writing Pass to CSV
    with open('CSV/pass.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID","Player Name", "Team Name", "Recipient Name", "Pass Length", "Angle", "Height ID", "Height Name", "Aerial Won", "End Location X", "End Location Y", "Assisted Shot ID", "Deflected", "Miscommunication", "Is Cross", "Cut Back", "Switch", "Shot Assist", "Goal Assist", "Body Part ID", "Body Part Name", "Type ID", "Type Name", "Outcome ID", "Outcome Name", "Technique ID", "Technique Name", "Match ID", "Season Name", "Competition Name"])
        for data in passing:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['recipient_Name'], data['pass_Length'], data['angle'], data['heightId'], data['heightName'], data['aerialWon'], data['endLocationX'], data['endLocationY'], data['assistedShotId'], data['deflected'], data['miscommunication'], data['isCross'], data['cutBack'], data['switch'], data['shotAssist'], data['goalAssist'], data['bodyPartId'], data['bodyPartName'], data['typeId'], data['typeName'], data['outcomeId'], data['outcomeName'], data['techniqueId'], data['techniqueName'], data['match_id'], data['season_Name'], data['competition_Name']])

    # Writing Shot to CSV
    with open('CSV/shot.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Player Name", "Team Name", "Statsbomb Xg", "End Location X", "End Location Y", "End Location Z", "Follows Dribble", "First Time", "Open Goal", "Deflected", "Technique ID", "Technique Name", "Body Part ID", "Body Part Name", "Type ID", "Type Name", "Outcome ID", "Outcome Name", "Match ID", "Season Name", "Competition Name"])
        for data in shot:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['statsbombXg'], data['endLocationX'], data['endLocationY'], data['endLocationZ'], data['followsDribble'], data['firstTime'], data['openGoal'], data['deflected'], data['techniqueId'], data['techniqueName'], data['bodyPartId'], data['bodyPartName'], data['typeId'], data['typeName'], data['outcomeId'], data['outcomeName'], data['match_id'], data['season_Name'], data['competition_Name']])


    # Writing Dribble to CSV
    with open('CSV/dribble.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Player Name", "Team Name", "Overrun", "Nutmeg", "No Touch", "Outcome ID", "Outcome Name", "Match ID", "Season Name", "Competition Name"])
        for data in dribble:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['overrun'], data['nutmeg'], data['noTouch'], data['outcomeId'], data['outcomeName'], data['match_id'], data['season_Name'], data['competition_Name']])

    # Writing badBehaviour to CSV
    with open('CSV/badBehaviour.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Player Name", "Team Name", "Card ID", "Card Name", "Match ID", "Season Name", "Competition Name"])
        for data in badBehaviour:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['cardId'], data['cardName'], data['match_id'], data['season_Name'], data['competition_Name']])

    # Writing ballReciept to CSV
    with open('CSV/ballReceipt.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Player Name", "Team Name", "Outcome ID", "Outcome Name", "Match ID", "Season Name", "Competition Name"])
        for data in ballReceipt:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['outcomeId'], data['outcomeName'], data['match_id'], data['season_Name'], data['competition_Name']])


    # Writing ballRecovery to CSV
    with open('CSV/ballRecovery.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Player Name", "Team Name", "Offensive", "Recovery Failure", "Match ID", "Season Name", "Competition Name"])
        for data in ballRecovery:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['offensive'], data['recoveryFailure'], data['match_id'], data['season_Name'], data['competition_Name']])


    # Writing Block to CSV
    with open('CSV/block.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Player Name", "Team Name", "Deflection", "Offensive", "Save Block", "Match ID", "Season Name", "Competition Name"])
        for data in block:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['deflection'], data['offensive'], data['saveBlock'], data['match_id'], data['season_Name'], data['competition_Name']])


    # Writing Carry to CSV
    with open('CSV/carry.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Player Name", "Team Name", "Carry End Location X", "Carry End Location Y", "Match ID", "Season Name", "Competition Name"])
        for data in carry:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['endLocationX'], data['endLocationY'], data['match_id'], data['season_Name'], data['competition_Name']])


    # Writing Clearance to CSV
    with open('CSV/clearance.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Player Name", "Team Name",  "Body Part ID", "Body Part Name","Aerial Won", "Match ID", "Season Name", "Competition Name"])
        for data in clearance:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['bodyPartId'], data['bodyPartName'], data['aerialWon'], data['match_id'], data['season_Name'], data['competition_Name']])


    # Writing duel to CSV
    with open('CSV/duel.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Player Name", "Team Name", "Type ID", "Type Name", "Outcome ID", "Outcome Name", "Match ID", "Season Name", "Competition Name"])
        for data in duel:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['typeId'], data['typeName'], data['outcomeId'], data['outcomeName'], data['match_id'], data['season_Name'], data['competition_Name']])


   # Writing foulCommitted to CSV
    with open('CSV/foulCommitted.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Player Name", "Team Name", "Offensive", "Foul Type ID", "Foul Type Name", "Advantage", "Penalty", "Card ID", "Card Name", "Match ID", "Season Name", "Competition Name"])
        for data in foulCommitted:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['offensive'], data['foulTypeId'], data['foulTypeName'], data['advantage'], data['penalty'], data['cardId'], data['cardName'], data['match_id'], data['season_Name'], data['competition_Name']])


    # Writing foulWon to CSV
    with open('CSV/foulWon.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Player Name", "Team Name", "Defensive", "Advantage", "Penalty", "Match ID", "Season Name", "Competition Name"])
        for data in foulWon:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['defensive'], data['advantage'], data['penalty'], data['match_id'], data['season_Name'], data['competition_Name']])


    # Writing goalkeeper to CSV
    with open('CSV/goalkeeper.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Player Name", "Team Name", "Position ID", "Technique ID", "Technique Name", "Body Part ID", "Body Part Name", "Type ID", "Type Name", "Outcome ID", "Outcome Name", "Match ID", "Season Name", "Competition Name"])
        for data in goalkeeper:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['positionId'], data['techniqueId'], data['techniqueName'], data['bodyPartId'], data['bodyPartName'], data['typeId'], data['typeName'], data['outcomeId'], data['outcomeName'], data['match_id'], data['season_Name'], data['competition_Name']])


    # Writing Interception to CSV
    with open('CSV/interception.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Player Name", "Team Name", "Outcome ID", "Outcome Name", "Match ID", "Season Name", "Competition Name"])
        for data in interception:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['outcomeId'], data['outcomeName'], data['match_id'], data['season_Name'], data['competition_Name']])


    # Writing substitution to CSV
    with open('CSV/substitution.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Event ID", "Player Name", "Team Name", "Replacement ID", "Replacement Name", "Outcome ID", "Outcome Name", "Match ID", "Season Name", "Competition Name"])
        for data in substitution:
            writer.writerow([data['event_id'], data['playerName'], data['teamName'], data['replacementId'], data['replacementName'], data['outcomeId'], data['outcomeName'], data['match_id'], data['season_Name'], data['competition_Name']])


if __name__ == "__main__":
    start = time.time()
    populatingTablesFromMatch()
    populatingTablesFromEvents()
    toCSV()
    print("Populating Tables from Matches.json")
    print("Stadium Table")
    # for stadiumData in substitution:
    #     print(stadiumData)
    print(len(player))
    end = time.time()

    print("Time taken to populate Stadium Table: ", end-start)
    

