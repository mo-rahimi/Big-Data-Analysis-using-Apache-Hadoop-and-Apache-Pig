accident data = load '/user/big data_project/UK_CarAccident_Data.cv/' USING PigStorage (',');
accident = FILTER accident_data BY $0>1;
distinct accident = DISTINCT accident;
accident limit = LIMIT distinct_accident 30000;
columns = FOREACH accident_limit GENERATE $7 as Accident_Severity, $8 as Number_of_Vehicles,
$9 as Number_of_Casualties, $32 as Number_Of_Chains;
severity_filter = filter columns BY Accident_Severity == 'Severity_1';
columns= FOREACH severity_filter GENERATE Number_of_Casualties, Number_Of_Chains, Number_of_Vehicles;
group columns= Group columns BY Number of Casualties;

final = FOREACH group_columns GENERATE group as Number_of_Casualties,
SUM (columns. Number_Of_Chains) as NumberOfAccident, SUM(columns. Number_of_Casualties) as NumberOfCasualties,
SUM (columns. Number of Vehicles) as NumberOfVehicles;

dump final;
