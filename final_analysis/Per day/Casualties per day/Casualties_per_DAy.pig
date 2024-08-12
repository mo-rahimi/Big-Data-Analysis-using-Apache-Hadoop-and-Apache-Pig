accident_data = load '/user/big_data_project/UK_CarAccident_Data.csv/' USING PigStorage (',');
accident = FILTER accident_data BY $0>1;
distinct accident = DISTINCT accident;
accident limit = LIMIT distinct accident 30000;
columns = FOREACH accident_limit GENERATE $9 as Number_of_Casualties, $11 as Day_of_Week;

group_columns = GROUP columns BY Day_of_Week;
Number_of_Casualties = FOREACH group_columns GENERATE group as Day_of_Week, SUM(columns. Number_of_Casualties) as NumberofCasualties;

STORE Number_of_Casualties INTO 'output/Number_of_Casualties_per_Day' using PigStorage(', ');
