accident_data = load '/user/big_data_project/UK_CarAccident_Data.cv/' USING PigStorage (',');
accident = FILTER accident_data BY $0>1;
distinct_accident = DISTINCT accident;
accident_limit = LIMIT distinct_accident 30000;
columns = FOREACH accident_limit GENERATE $7 as Accident_Severity, $17 as Road_Type, $32 as Number_Of_Chains;

L1_columns= Filter columns BY Accident_Severity =='Severity_1';
L1_final_columns = FOREACH L1_columns GENERATE Road_Type, Number_Of_Chains;
L1_group = GROUP L1_final_columns BY Road_Type;
L1_count = FOREACH L1_group GENERATE group as Road_Type, SUM(L1_final_columns.Number_Of_Chains) as NumberOfAccident;

L2 columns= Filter columns BY Accident Severity =='Severity 2' ;
L2_final_columns = FOREACH L2_columns GENERATE Road_Type, Number_Of_Chains;
L2_group = GROUP L2_final_columns BY Road_Type;
L2_count = FOREACH L2_group GENERATE group as Road_Type, SUM(L2_final_columns.Number_Of_Chains) as NumberofAccident;

L3_columns= Filter columns BY Accident_Severity =='Severity_3';
L3_final_columns = FOREACH L3_columns GENERATE Road_Type, Number_Of_Chains;
L3_group = GROUP L3_final_columns BY Road_Type;
L3_count = FOREACH L3_group GENERATE group as Road_Type, SUM(L3_final_columns.Number_Of_Chains) as NumberOfAccident;

L4 columns= Filter columns BY Accident Severity =='Severity 4';
L4_final_columns = FOREACH L4_columns GENERATE Road_Type, Number_Of_Chains;
L4_group = GROUP L4_final_columns BY Road_Type;
L4_count = FOREACH L4_group GENERATE group as Road_Type, SUM(L4_final_columns.Number_Of_Chains) as NumberofAccident;

L5_columns= Filter columns BY Accident_Severity == 'Severity_5';
L5_final_columns = FOREACH L5_columns GENERATE Road_Type, Number_Of_Chains;
L5_group = GROUP L5_final_columns BY Road_Type;
L5_count = FOREACH L5_group GENERATE group as Road_Type, SUM(L5_final_columns.Number_Of_Chains) as NumberofAccident;

STORE L1 count INTO 'output/Severity Level 1 Based On Road Type' using PigStorage(',');
STORE L2_count INTO 'output/Severity Level 2 Based On Road_Type' using PigStorage(',');
STORE L3 count INTO 'output/Severity Level 3 Based On Road Type' using PigStorage(',');
STORE L4_count INTO 'output/Severity Level 4 Based On Road_Type' using PigStorage(',');
STORE L5_count INTO 'output/Severity Level 5 Based On Road_Type' using PigStorage(',');