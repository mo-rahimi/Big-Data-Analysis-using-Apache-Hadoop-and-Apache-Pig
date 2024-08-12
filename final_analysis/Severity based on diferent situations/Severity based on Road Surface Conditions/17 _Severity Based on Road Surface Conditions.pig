accident_data = load '/user/big_data_project/UK_CarAccident_Data.csv/' USING PigStorage (',');
accident = FILTER accident_data BY $0>1;
distinct_accident = DISTINCT accident;
accident_limit = LIMIT distinct_accident 30000;
columns = FOREACH accident_limit GENERATE $7 as Accident_Severity, $26 as Road_Surface_Conditions, $32 as Number_Of_Chains;

L1 columns= Filter columns BY Accident Severity =='Severity 1';
L1_final_columns = FOREACH L1_columns GENERATE Road_Surface_Conditions, Number_Of_Chains;
L1_group = GROUP L1_final_columns BY Road_Surface_Conditions;
L1_count = FOREACH L1_group GENERATE group as Road_Surface_Conditions, SUM(L1_final_columns.Number_Of_Chains) as NumberofAccident;

L2 columns= Filter columns BY Accident Severity =='Severity 2';
L2_final_columns = FOREACH L2_columns GENERATE Road_Surface_Conditions, Number_of_Chains;
L2 group = GROUP L2 final columns BY Road Surface Conditions;
L2_count = FOREACH L2_group GENERATE group as Road_Surface_Conditions, SUM(L2_final_columns.Number_Of_Chains) as NumberofAccident;

L3 columns= Filter columns BY Accident Severity =='Severity 3';
L3_final_columns = FOREACH L3_columns GENERATE Road_Surface_Conditions, Number_Of_Chains;
L3_group = GROUP L3_final_columns BY Road_Surface_Conditions;
L3_count = FOREACH L3_group GENERATE group as Road_Surface_Conditions, SUM(L3_final_columns.Number_Of_Chains) as NumberofAccident;

L4 columns= Filter columns BY Accident Severity =='Severity 4' :
L4_final_columns = FOREACH L4_columns GENERATE Road_Surface_Conditions, Number_Of_Chains;
L4_group = GROUP L4_final_columns BY Road_Surface_Conditions;
L4_count = FOREACH L4_group GENERATE group as Road_Surface_Conditions, SUM(L4_final_columns.Number_Of_Chains) as NumberofAccident;

L5 _columns= Filter columns BY Accident Severity =='Severity 5';
L5_final_columns = FOREACH L5_columns GENERATE Road_Surface_Conditions, Number_Of_Chains;
L5_group = GROUP L5_final_columns BY Road_Surface_Conditions;
L5_count = FOREACH L5_group GENERATE group as Road_Surface_Conditions, SUM(L5_final_columns. Number_Of_Chains) as NumberofAccident;

STORE L1_count INTO 'output/Severity Level 1 Based On Road_Surface_Conditions' using PigStorage(',');
STORE L2_count INTO 'output/Severity Level 2 Based On Road _Surface_Conditions' using PigStorage(',');
STORE L3 count INTO 'output/Severity Level 3 Based On Road Surface Conditions' using PigStorage(',');
STORE L4_count INTO 'output/Severity Level 4 Based On Road_Surface_Conditions' using PigStorage(',');
STORE L5_count INTO 'output/Severity Level 5 Based On Road Surface Conditions' using PigStorage(',');