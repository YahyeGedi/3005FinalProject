

CREATE TABLE Countries (
    country_id INT,
    country_name VARCHAR(255),
    PRIMARY KEY (country_name)
);

CREATE TABLE Competitions (
	competition_ID INT UNIQUE,
	competition_Name VARCHAR(255) NOT NULL,
    competition_Gender VARCHAR(255),
    competition_Youth BOOLEAN,
    competition_International BOOLEAN,
    country_Name VARCHAR(255) NOT NULL,
	PRIMARY KEY (competition_Id),
	FOREIGN KEY (country_Name)
    	REFERENCES Countries (country_name)
);


CREATE TABLE Manager (
    managers_Id INT UNIQUE,
    managers_Name VARCHAR(255) NOT NULL,
    managers_Nickname VARCHAR(255),
    managers_date_of_Birth DATE,
    managers_Country VARCHAR(255) NOT NULL,
    PRIMARY KEY (managers_Id),
    FOREIGN KEY (managers_Country)
    	REFERENCES Countries (country_name)
);

CREATE TABLE Referee (
    referees_Id INT UNIQUE ,
    PRIMARY KEY (referees_Id),
    referees_Name VARCHAR(255) NOT NULL,
    referees_Country VARCHAR(255) NOT NULL,
    FOREIGN KEY (referees_Country)
    	REFERENCES Countries (country_name)
);

CREATE TABLE Team (
    teams_Id INT UNIQUE,
    teams_Name VARCHAR(255) NOT NULL,
    teams_Gender VARCHAR(255),
    teams_Group VARCHAR(255),
    teams_Country VARCHAR(255) NOT NULL,
    managers_Id INT,
    PRIMARY KEY (teams_Name),
    FOREIGN KEY (teams_Country)
    	REFERENCES Countries (country_name),
    FOREIGN KEY (managers_Id)
    	REFERENCES Manager (managers_Id)
);

CREATE TABLE Stadium (
    stadiums_Id INT UNIQUE,
    stadiums_Name VARCHAR(255) NOT NULL,
    stadiums_Country VARCHAR(255) NOT NULL,
    FOREIGN KEY (stadiums_Country)
    	REFERENCES Countries (country_name)
);

CREATE TABLE Match (
    matches_Id INT UNIQUE,
	competitions_Id INT,
	season_Id INT,
    season_Name VARCHAR(255),
    match_Date DATE NOT NULL,
    kick_off_Time TIME NOT NULL,
    home_team_Name VARCHAR(255),
    away_team_Name VARCHAR(255),
    home_Score INT NOT NULL,
    away_Score INT NOT NULL,
    match_Week INT,
    competition_Stage VARCHAR(255),
    stadium_Id INT,
    referee_Id INT,
	PRIMARY KEY (matches_Id),
	FOREIGN KEY (competitions_Id)
		REFERENCES Competitions (competition_Id),
    FOREIGN KEY (home_team_Name)
    	REFERENCES Team (teams_Name),
    FOREIGN KEY (away_team_Name)
    	REFERENCES Team (teams_Name),
    FOREIGN KEY (stadium_Id)
    	REFERENCES Stadium (stadiums_Id),
    FOREIGN KEY (referee_Id)
    	REFERENCES Referee (referees_Id)
);

CREATE TABLE Position (
    position_Id INT,
    PRIMARY KEY(position_Id),
    position_Name VARCHAR(255)
);

CREATE TABLE Tactic (
    match_id INT,
    event_id VARCHAR(255),
    formation VARCHAR(255),
    lineup INTEGER[],
    FOREIGN KEY (match_id)
        REFERENCES Match (matches_Id)
);

CREATE TABLE PlayPattern (
    play_pattern_Id INT,
    PRIMARY KEY(play_pattern_Id),
    play_pattern_Name VARCHAR(255)
);

CREATE TABLE Player (
    player_Id INT,
    player_Name VARCHAR(255) PRIMARY KEY,
    position_Id INT,
    jersey_Number INT,
    team_Name VARCHAR(255),
    country_Name VARCHAR(255),
    FOREIGN KEY (country_Name)
    	REFERENCES Countries (country_name),
    FOREIGN KEY (position_Id)
        REFERENCES Position (position_Id)
);

CREATE TABLE Events (
    event_id VARCHAR(255),
    event_Index INT,
    event_Period INT,
    event_timeStamp TIME,
    event_Minute INT,
    event_Second INT,
    type_Id INT,
    type_Name VARCHAR(255),
    possession INT,
    possession_team_Name VARCHAR(255),
    play_pattern_Id INT,
    location_x FLOAT,
    location_y FLOAT,
    event_Duration FLOAT,
    counterpress BOOLEAN,
    position_Id INT,
    team_name VARCHAR(255),
    player_name VARCHAR(255),
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id)
        REFERENCES Match (matches_Id),
    FOREIGN KEY (player_name)
    	REFERENCES Player (player_Name),
    FOREIGN KEY (position_Id)
    	REFERENCES Position (position_Id),    
    FOREIGN KEY (team_name)
    	REFERENCES Team (teams_Name),
    FOREIGN KEY (possession_team_Name)
        REFERENCES Team (teams_Name),
    FOREIGN KEY (play_pattern_Id)
        REFERENCES PlayPattern (play_pattern_Id)
) PARTITION BY LIST (type_Name);

CREATE TABLE Event_DribblePast PARTITION OF Events FOR VALUES IN ('Dribbled Past');
CREATE TABLE Rest_Of_Events PARTITION OF Events
    DEFAULT;


CREATE TABLE Pass (
    event_Id VARCHAR(255),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    recipient_Name VARCHAR(255),
    pass_Length FLOAT,
    angle FLOAT,
    heightId INT,
    heightName VARCHAR(255),
    aerialWon BOOLEAN,
    endLocationX FLOAT,
    endLocationY FLOAT,
    assistedShotId VARCHAR(255),
    deflected BOOLEAN,
    miscommunication BOOLEAN,
    isCross BOOLEAN,
    cutBack BOOLEAN,
    switch BOOLEAN,
    shotAssist BOOLEAN,
    goalAssist BOOLEAN,
    bodyPartId INT,
    bodyPartName VARCHAR(255),
    typeId INT,
    typeName VARCHAR(255),
    outcomeId INT,
    outcomeName VARCHAR(255),
    techniqueId INT,
    techniqueName VARCHAR(255),  
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES Match (matches_Id),
    FOREIGN KEY (recipient_Name) REFERENCES Player (player_Name),
    FOREIGN KEY (teamName) REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName)
    	REFERENCES Player (player_Name)
) PARTITION BY LIST (season_Name);

CREATE TABLE Pass_2018_2019 PARTITION OF Pass FOR VALUES IN ('2018/2019');
CREATE TABLE Pass_2019_2020 PARTITION OF Pass FOR VALUES IN ('2019/2020');
CREATE TABLE Pass_2020_2021 PARTITION OF Pass FOR VALUES IN ('2020/2021')
PARTITION BY LIST (competition_Name);

CREATE TABLE Pass_2003_2004 PARTITION OF Pass FOR VALUES IN ('2003/2004')
PARTITION BY LIST (competition_Name);

CREATE TABLE La_Liga_Pass_2020_2021 PARTITION of Pass_2020_2021 FOR VALUES IN ('La Liga');
CREATE TABLE Premier_League_Pass_2003_2004 PARTITION of Pass_2003_2004 FOR VALUES IN ('Premier League');


CREATE TABLE Shot(
    event_id VARCHAR(255),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    statsbomXg FLOAT,
    endLocationX FLOAT,
    endLocationY FLOAT,
    endLocationZ FLOAT,
    followsDribble BOOLEAN,
    firstTime BOOLEAN,
    openGoal BOOLEAN,
    deflected BOOLEAN,
    techniqueId INT,
    techniqueName VARCHAR(255),
    bodyPartId INT,
    bodyPartName VARCHAR(255),
    typeId INT,
    typeName VARCHAR(255),
    outcomeId INT,
    outcomeName VARCHAR(255),
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id)
        REFERENCES Match (matches_Id),
    FOREIGN KEY (teamName) REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName)
    	REFERENCES Player (player_Name)
) PARTITION BY LIST (season_Name);

CREATE TABLE Shot_2018_2019 PARTITION OF Shot FOR VALUES IN ('2018/2019');
CREATE TABLE Shot_2019_2020 PARTITION OF Shot FOR VALUES IN ('2019/2020');
CREATE TABLE Shot_2020_2021 PARTITION OF Shot FOR VALUES IN ('2020/2021');
CREATE TABLE Shot_2003_2004 PARTITION OF Shot FOR VALUES IN ('2003/2004')
PARTITION BY LIST (competition_Name);

CREATE TABLE Premier_League_Shot_2003_2004 PARTITION of Shot_2003_2004 FOR VALUES IN ('Premier League');

CREATE TABLE Dribble(
    event_id VARCHAR(255),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    overrun BOOLEAN,
    nutmeg BOOLEAN,
    noTouch BOOLEAN,
    outcomeId INT,
    outcomeName VARCHAR(255),
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id)
        REFERENCES Match (matches_Id),
    FOREIGN KEY (teamName)
        REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName)
    	REFERENCES Player (player_Name)
) PARTITION BY LIST (season_Name);

CREATE TABLE Dribble_2018_2019 PARTITION OF Dribble FOR VALUES IN ('2018/2019');
CREATE TABLE Dribble_2019_2020 PARTITION OF Dribble FOR VALUES IN ('2019/2020');
CREATE TABLE Dribble_2020_2021 PARTITION OF Dribble FOR VALUES IN ('2020/2021');
CREATE TABLE Dribble_2003_2004 PARTITION OF Dribble FOR VALUES IN ('2003/2004');

CREATE TABLE ballReceipt(
    event_id VARCHAR(255),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    outcomeId INT,
    outcomeName VARCHAR(255),
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id)
        REFERENCES Match (matches_Id),
    FOREIGN KEY (teamName)
        REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName)
    	REFERENCES Player (player_Name)
);

CREATE TABLE Goalkeeper (
    event_id VARCHAR(255),
    PRIMARY KEY(event_id),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    positionId INT,
    techniqueId INT,
    techniqueName VARCHAR(255),
    bodyPartId INT,
    bodyPartName VARCHAR(255),
    typeId INT,
    typeName VARCHAR(255),
    outcomeId INT,
    outcomeName VARCHAR(255),
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES Match (matches_Id),
    FOREIGN KEY (teamName) REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName) REFERENCES Player (player_Name),
    FOREIGN KEY (positionId) REFERENCES Position (position_Id)
);

CREATE TABLE Duel (
    event_id VARCHAR(255),
    PRIMARY KEY(event_id),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    typeId INT,
    typeName VARCHAR(255),
    outcomeId INT,
    outcomeName VARCHAR(255),
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES Match (matches_Id),
    FOREIGN KEY (teamName) REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName) REFERENCES Player (player_Name)
);


CREATE TABLE ballRecovery (
    event_id VARCHAR(255),
    PRIMARY KEY(event_id),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    offensive BOOLEAN,
    recoveryFailure BOOLEAN,
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES Match (matches_Id),
    FOREIGN KEY (teamName) REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName) REFERENCES Player (player_Name)
);


CREATE TABLE Carry (
    event_id VARCHAR(255),
    PRIMARY KEY(event_id),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    endLocationX FLOAT, 
    endLocationY FLOAT,
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES Match (matches_Id),
    FOREIGN KEY (teamName) REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName) REFERENCES Player (player_Name)
);

CREATE TABLE badBehaviour(
    event_id VARCHAR(255),
    PRIMARY KEY(event_id),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    cardId INT, 
    cardName VARCHAR(255),
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id)
        REFERENCES Match (matches_Id),
    FOREIGN KEY (teamName)
        REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName)
    	REFERENCES Player (player_Name)
);


CREATE TABLE Block (
    event_id VARCHAR(255),
    PRIMARY KEY(event_id),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    deflection BOOLEAN,
    offensive BOOLEAN,
    saveBlock BOOLEAN,
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES Match (matches_Id),
    FOREIGN KEY (teamName) REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName) REFERENCES Player (player_Name)
);

CREATE TABLE Interception (
    event_id VARCHAR(255),
    PRIMARY KEY(event_id),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    outcomeId INT,
    outcomeName VARCHAR(255),
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES Match (matches_Id),
    FOREIGN KEY (teamName) REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName) REFERENCES Player (player_Name)
);


CREATE TABLE Clearance (
    event_id VARCHAR(255),
    PRIMARY KEY(event_id),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    bodyPartId INT,
    bodyPartName VARCHAR(255),
    aerialWon BOOLEAN,
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES Match (matches_Id),
    FOREIGN KEY (teamName) REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName) REFERENCES Player (player_Name)
);


CREATE TABLE foulCommitted (
    event_id VARCHAR(255),
    PRIMARY KEY(event_id),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    offensive BOOLEAN,
    foulTypeId INT,
    foulTypeName VARCHAR(255),
    advantage BOOLEAN,
    penalty BOOLEAN,
    cardId INT,
    cardName VARCHAR(255),
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES Match (matches_Id),
    FOREIGN KEY (teamName) REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName) REFERENCES Player (player_Name)
);

CREATE TABLE foulWon (
    event_id VARCHAR(255),
    PRIMARY KEY(event_id),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    defensive BOOLEAN,
    advantage BOOLEAN,
    penalty BOOLEAN,
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES Match (matches_Id),
    FOREIGN KEY (teamName) REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName) REFERENCES Player (player_Name)
);




CREATE TABLE Substitution (
    event_id VARCHAR(255),
    PRIMARY KEY(event_id),
    playerName VARCHAR(255),
    teamName VARCHAR(255),
    replacementId INT,
    replacementName VARCHAR(255),
    outcomeId INT,
    outcomeName VARCHAR(255),
    match_id INT,
    season_Name VARCHAR(255),
    competition_Name VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES Match (matches_Id),
    FOREIGN KEY (teamName) REFERENCES Team (teams_Name),
    FOREIGN KEY (playerName) REFERENCES Player (player_Name)
);

CREATE TABLE lineup (
    match_id INT,
    team_name VARCHAR(255),
    player_name VARCHAR(255),
    player_nickname VARCHAR(255),
    country_name VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES Match (matches_Id),
    FOREIGN KEY (team_name) REFERENCES Team (teams_Name),
    FOREIGN KEY (player_name) REFERENCES Player (player_Name),
    FOREIGN KEY (country_name) REFERENCES Countries (country_name)
);

CREATE TABLE lineupPositions (
    match_id INT,
    player_name VARCHAR(255),
    position_id INT,
    startFrom VARCHAR(255),
    endTo VARCHAR(255),
    from_period INT,
    to_period INT,
    start_reason VARCHAR(255),
    end_reason VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES Match (matches_Id),
    FOREIGN KEY (position_id) REFERENCES Position (position_Id),
    FOREIGN KEY (player_name) REFERENCES Player (player_Name)
);

CREATE TABLE lineupCards (
    match_id INT,
    player_name VARCHAR(255),
    card_time VARCHAR(255),
    card_type VARCHAR(255),
    card_reason VARCHAR(255),
    period INT,
    FOREIGN KEY (match_id) REFERENCES Match (matches_Id),
    FOREIGN KEY (player_name) REFERENCES Player (player_Name)
);





