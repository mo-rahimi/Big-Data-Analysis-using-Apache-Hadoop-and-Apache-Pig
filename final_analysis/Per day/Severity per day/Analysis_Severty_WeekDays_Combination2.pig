accident_data = load '/user/big_data_project/UK_CarAccident_Data.csv/' USING PigStorage (',');accident = FILTER accident data BY $0>1;distinct accident = DISTINCT accident;accident_limit = LIMIT distinct_accident 30000;columns = FOREACH accident_limit GENERATE $7 as Accident_Severity, $11 as Day_of_Week, $32 as Number_Of_Chains;
L1_columns= Filter columns BY Accident_Severity =='Severity_1';L1_final_columns = FOREACH L1_columns GENERATE Day_of_Week, Number_Of_Chains;L1_group = GROUP L1_final_columns BY Day_of _Week;
L1_count = FOREACH L1_group GENERATE group as Day_of _Week, SUM(L1_final_columns.Number_Of_Chains) as NumberofAccident;

L2_columns= Filter columns BY Accident Severity =='Severity 2';
L2_final_columns = FOREACH L2_columns GENERATE Day_of_Week, Number_Of_Chains;
L2_group = GROUP L2_final_columns BY Day_of_Week;
L2_count = FOREACH L2_group GENERATE group as Day_of_Week, SUM(L2_final_columns.Number_Of_Chains) as NumberOfAccident;

L3_columns= Filter columns BY Accident_Severity =='Severity_3';
L3_final_columns = FOREACH L3_columns GENERATE Day_of_Week, Number_Of_Chains;
L3_group = GROUP L3_final_columns BY Day_of _Week;
L3_count = FOREACH L3_group GENERATE group as Day_of _Week, SUM(L3_final_columns.Number_Of_Chains) as NumberofAccident;

L4_columns= Filter columns BY Accident_Severity =='Severity_4';
L4_final_columns = FOREACH L4_columns GENERATE Day_of_Week, Number_Of_Chains;
L4_group = GROUP L4_final_columns BY Day_of_Week;
L4_count = FOREACH L4_group GENERATE group as Day_of _Week, SUM(L4_final_columns.Number_Of_Chains) as NumberofAccident;

L5_columns= Filter columns BY Accident_Severity =='Severity_5';
L5_final_columns = FOREACH L5_columns GENERATE Day_of_Week, Number_Of_Chains;
L5_group = GROUP L5_final_columns BY Day_of_Week;
L5_count = FOREACH L5_group GENERATE group as Day_of_Week, SUM(L5_final_columns.Number_Of_Chains) as NumberofAccident;

STORE L1 count INTO 'output/Severity Level1 per Day' using PigStorage
STORE L2 count INTO 'output/Severity Level2 per Day' using PigStorage
STORE L3 count INTO 'output/Severity Level3 per Day' using PigStorage
STORE L4_count INTO 'output/Severity Level4 per Day' using PigStorage
STORE L5_count INTO 'output/Severity Level5 per Day' using PigStorage
