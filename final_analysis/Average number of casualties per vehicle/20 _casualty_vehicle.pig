accident data = load /user/big data project/UK CarAccident Data.csv/' USING PigStorage (', ');
accident = FILTER accident data BY $0>1;
distinct_accident = DISTINCT accident;
accident_limit = LIMIT distinct_accident 30000;
columns_vehicle_casualty = FOREACH accident_limit GENERATE $8 as Number_of_Vehicles, $9 as Number_of_Casualties;
STORE columns vehicle casualty INTO 'output/Columns of Vehicle and Casualty' using PigStorage(',");


