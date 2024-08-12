accident_data = load '/user/big_data_project/UK_CarAccident_Data.cv/' USING PigStorage (',');
accident = FILTER accident_data BY $0>1;
distinct accident = DISTINCT accident;
accident_limit = LIMIT distinct_accident 30000;
columns = FOREACH accident_limit GENERATE $7 as Accident_Severity, $24 as Light_Conditions, $32 as Number_Of_Chains;

L1_columns= Filter columns BY Accident_Severity =='Severity_1';
L1_final_columns = FOREACH L1_columns GENERATE Light_Conditions, Number_Of_Chains;
L1_group = GROUP L1_final_columns BY Light_Conditions;
L1_count = FOREACH L1_group GENERATE group as Light_Conditions, SUM(L1_final_columns.Number_Of_Chains) as NumberofAccident;

L2_columns= Filter columns BY Accident_Severity =='Severity_2';
L2_final_columns = FOREACH L2_columns GENERATE Light_Conditions, Number_Of_Chains;
L2_group = GROUP L2_final_columns BY Light_Conditions;
L2_count = FOREACH L2_group GENERATE group as Light_Conditions, SUM(L2_final_columns.Number_Of_Chains) as NumberofAccident;

L3_columns= Filter columns BY Accident_Severity =='Severity_3';
L3_final_columns = FOREACH L3_columns GENERATE Light_Conditions, Number_Of_Chains;
L3_group = GROUP L3_final_columns BY Light_Conditions;
L3_count = FOREACH L3_group GENERATE group as Light_Conditions, SUM(L3_final_columns.Number_Of_Chains) as NumberofAccident;

L4_columns= Filter columns BY Accident_Severity =='Severity_4' ;
L4_final_columns = FOREACH L4_columns GENERATE Light_Conditions, Number_Of_Chains;
L4_group = GROUP L4_final_columns BY Light_Conditions;
L4 count = FOREACH L4 group GENERATE group as Light Conditions, SUM(L4 final columns. Number Of Chains) as NumberofAccident;

L5_columns= Filter columns BY Accident Severity =='Severity 5';
L5_final_columns = FOREACH L5_columns GENERATE Light_Conditions, Number_Of_Chains;
L5_group = GROUP L5_final_columns BY Light_Conditions;
L5_count = FOREACH L5_group GENERATE group as Light_Conditions, SUM(L5_final_columns.Number_Of_Chains) as NumberofAccident;

STORE L1_count INTO output/Severity Level 1 Based On Light Condition' using PigStorage('
STORE L2_count INTO 'output/Severity Level 2 Based On Light Condition' using PieStorage
STORE L3_count INTO 'output/Severity Level 3 Based On Light Condition' using PigStorage (
STORE L4_count INTO 'output/Severity Level 4 Based On Light Condition' using PigStorage
STORE L5_count INTO 'output/Severity Level 5 Based On Light Condition' using Piestorage