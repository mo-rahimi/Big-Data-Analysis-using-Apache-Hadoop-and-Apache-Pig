
accident_data = load '/user/big_data_project/UK_CarAccident Data.cs/' USING PigStorage (', ');
accident = FILTER accident_data BY $0>1;
distinct accident = DISTINCT accident;
accident_limit = LIMIT distinct_accident 30000;
columns = FOREACH accident_limit GENERATE $7 as Accident_Severity, $25 as Weather_Conditions, $32 as Number_Of_Chains;

L1 columns= Filter columns BY Accident Severity =='Severity 1';
L1_final_columns = FOREACH L1_columns GENERATE Weather_Conditions, Number_Of_Chains;
L1_group = GROUP L1_final_columns BY Weather_Conditions;
L1_count = FOREACH L1_group GENERATE group as Weather_Conditions, SUM(L1_final_columns.Number_Of_Chains) as NumberOfAccident;

L2 columns= Filter columns BY Accident Severity =='Severity 2';
L2_final_columns = FOREACH L2_columns GENERATE Weather_Conditions, Number_Of_Chains;
L2_group = GROUP L2_final_columns BY Weather_Conditions;
L2_count = FOREACH L2_group GENERATE group as Weather_Conditions, SUM(L2 final columns. Number Of Chains) as NumberOfAccident;

L3_columns= Filter columns BY Accident_Severity =='Severity_3';
L3_final_columns = FOREACH L3_columns GENERATE Weather_Conditions, Number_Of_Chains;
L3_group = GROUP L3_final_columns BY Weather_Conditions;
L3_count = FOREACH L3_group GENERATE group as Weather_Conditions, SUM(L3_final_columns.Number_Of_Chains) as NumberOfAccident;

L4 columns= Filter columns BY Accident Severity =='Severity 4' :
L4_final_columns = FOREACH L4_columns GENERATE Weather_Conditions, Number_Of_Chains;
L4_group = GROUP L4_final_columns BY Weather_Conditions;
L4_count = FOREACH L4_group GENERATE group as Weather_Conditions, SUM(L4_final_columns.Number_Of_Chains) as NumberofAccident;

L5 columns= Filter columns BY Accident Severity =='Severity 5';
L5_final_columns = FOREACH L5_columns GENERATE Weather_Conditions, Number_Of_Chains;
L5_group = GROUP L5_final_columns BY Weather_Conditions;
L5_count = FOREACH L5_group GENERATE group as Weather_Conditions, SUM(L5_final_columns.Number_Of_Chains) as NumberOfAccident;

STORE L1 count INTO 'output/Severity Levell Based On Weather' using PigStorage(',');
STORE L2 count INTO output/Severity Level2 Based On Weather' using PigStorage(',');
STORE L3 count INTO 'output/Severity Level Based On Weather' using PigStorage(',');
STORE L4 count INTO output/Severity Level4 Based On Weather' using PigStorage(',');
STORE L5_count INTO 'output/Severity Level5 Based On Weather' using PigStorage(',');