accident_data = load '/user/big_data_project/UK_CarAccident_Data.csv/' USING PigStorage (',');
accident = FILTER accident_data BY $0>1;
distinct_accident = DISTINCT accident;
accident limit = LIMIT distinct accident 30000;
columns = FOREACH accident_limit GENERATE $7 as Accident_Severity, $8 as Number_of_Vehicles, $32 as Number_Of_chains;

severity_group = GROUP columns BY Accident_Severity;
severity_vehicles_count = FOREACH severity_group GENERATE group as Accident_Severity,
SUM(columns .Number_Of_Chains) as NumberOfAccident, SUM(columns.Number_of_Vehicles) as NumberOfVehicles, 11 (float)(SUM(columns.Number_of Vehicles)/SUM(columns.Number Of Chains)) as Vehicles per Accident;

STORE severity_vehicles_count INTO 'output/Number Of Damaged Vehicle Per Accident' using PigStoragel',');
