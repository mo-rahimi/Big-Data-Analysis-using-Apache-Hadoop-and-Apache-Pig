accident_data = load '/user/big_data_project/UK_CarAccident_Data.csv/' USING PigStorage (',');
accident = FILTER accident_data BY $0>1;
distinct_accident = DISTINCT accident;
accident_limit = LIMIT distinct_accident 30000;
columns = FOREACH accident_limit GENERATE $8 as Number_of_Vehicles, $11 as Day_of_Week;

group_columns = GROUP columns BY Day_of_Week;
number_of_vehicles = FOREACH group_columns GENERATE group as Day_of_Week, SUM(columns .Number_of_Vehicles) as NumberofVehicles;

STORE number of vehicles INTO 'output/Number of Vehicles per Day' using PigStorage(',');
