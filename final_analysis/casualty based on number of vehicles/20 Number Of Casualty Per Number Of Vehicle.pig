accident data = load '/user/big_data_project/UK_CarAccident_Data.cv/' USING PigStorage (',');
accident = FILTER accident_data BY $0>1;
distinct accident = DISTINCT accident;
accident limit = LIMIT distinct_accident 30000;

columns = FOREACH accident_limit GENERATE $8 as Number_of_Vehicles, $9 as Number_of_Casualties, $32 as Number_Of_Chains;
vc group = GROUP columns BY Number of Vehicles;
vc_count = FOREACH vc _group GENERATE group as Number of Vehicles,
SUM(columns. Number_Of_Chains) as NumberOfAccident, SUM(columns. Number_of_Casualties) as NumberOfCasualties, 10 (float) (SUM(columns.Number_of_Casualties)/SUM(columns.Number_Of_Chains)) as Casualties_per_Vehicle;

STORE vc count INTO 'output/Average Number Of Casualty Based On Number Of Vehicle' using PigStorage(',');

