# Big Data Analysis using Apache Hadoop and Apache Pig
- `Analysis of UK Accident Data`🚥  🚦  🚥  🚥  🚦  🚥 
<img src="images/heat map 0.png" alt="Overview of the project" width="70%">
## Description  
My interest in analyzing accident data stems from `my initial role as a data analyst in the traffic department in 2011`. During that time, I observed a consistent number of accidents and injuries, which sparked my curiosity about the factors contributing to these statistics.

Approximately `1.3 million people die annually in road accidents`, with `40 million sustaining injuries or disabilities`. The economic impact of road accidents is significant, affecting individuals, families, and nations. 
Understanding the primary causes of these incidents is crucial for developing effective prevention strategies.

 This project aims to explore and uncover insights into road safety and accident causation in UK between years 2000 to 2018.
Despite the dataset lacking specific details on accident causes—such as `driver fatigue` or `maneuver errors`, this analysis will address several critical questions:


**Query and problem Statements:**
1. How do social and economic factors affect road accidents frequency and severity on weekends versus weekdays?

2. How do weather patterns, driver behavior, and infrastructure quality impact accident severity?
 
3. How do light conditions, weather, and road type affect accident severity in different UK regions?

4. What insights can be derived from road surface conditions and accident severity for informing policy changes to improve road safety?

5. How does understanding the correlation between the number of vehicles involved and accident severity help design better traffic management systems?
 
6. What hidden patterns in the data reveal surprising insights into road safety?

7. How can analysis of accident severity, road type, and the number of vehicles involved inform infrastructure planning and traffic management strategies prioritizing safety?

8. How can examining light conditions, weather patterns, and road surface factors help develop predictive models to reduce serious accidents likelihood, especially during weekends with higher casualties?

9. What can be learned from accidents occurring in fine weather conditions, and how can behavioral insights be applied to design awareness campaigns and incentives to maintain vigilance, reducing non-serious accidents on less frequent days?

10. How can analysis of accident severity and its impact on injured people inform policy changes prioritizing post-crash care and rehabilitation services?

11. How can exploring differences in accident patterns between weekdays and weekends optimize emergency response systems and resource allocation, improving road safety measures' efficiency and effectiveness?
## Table of Contents
- [Introduction](#introduction)
- [Query and problem Statements](query_problem_statements)
- [data set](data_set)
- [Installation](#installation)
- [Dataset Overview](#dataset_overview)
- [Usage](#usage)
- [Code Examples and Visulization](#code-examples_visulization)
- [Conclusion](#conclusion)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)



### Introduction

This project analyzes UK accident data to understand the factors influencing road accidents and their severity. The analysis focuses on:

- Investigating the impact of road conditions, weather, and light conditions on accident severity.
- Exploring the relationship between the number of vehicles involved and the severity of accidents.

The data analysis is performed using the Pig tool, while Python is utilized for data visualization.

**Two important approaches:**
- First: investigating the influence of different conditions such as road type, road surface condition, weather condition and light condition on the severity of accidents.
- Second: finding out the relation between the severity with number of involved vehicles in the accident and impact of the severities on the number of causalities.


## Data Set Overview
The UK government amassed traffic data from 2000 and 2018, recording over 1.8 million accidents in the process and making this one of the most comprehensive traffic data sets out there. It's a huge picture of a country undergoing change.
- Download (https://www.kaggle.com/datasets/devansodariya/road-accident-united-kingdom-uk-dataset)
  

## Installation
 
### Software Requirements
- Windows OS
- Virtual Machine to run Hortonworks Sandbox
- Apache Hadoop
- Apache Pig
- Python 

### Hardware Requirements
- Hard Disk: at least 500 GB
- RAM: at least 16 GB
- Processor: Core i5 or above

## Usage (Hadoop, Pig)
### Loading Data to Hadoop

1. **Run Hortonworks Sandbox** in a Virtual Machine.
2. **Upload Data to HDFS**:
   - Use the Ambari environment to create a directory (`Big_Data_Project`) and upload `UK_CarAccident_Data.csv` to HDFS.

### Data Analysis Using Pig

1. **Access Pig View** in Ambari to write, run, and store Pig scripts for data analysis.
2. **Scripts**: A total of 23 scripts are created for various analyses. Example scripts include
3. **Save Results**: Use the STORE command to save the results in HDFS under the `admin/output` directory.


## Code Examples and Visulization

### Example usage command
pig -f your_pig_script.pig

### Sample Pig Script for Accident Severity

```pig
accident = LOAD '/user/big_data_project/UK_CarAccident.csv' USING PigStorage(',') AS (
    No:int, Accident_Index:chararray, Location_Easting_OSGR:int, 
    Location_Northing_OSGR:int, Longitude:double, Latitude:double, 
    Police_Force:int, Accident_Severity:chararray, 
    Number_of_Vehicles:int, Number_of_Casualties:int, 
    Date:chararray, Day_of_Week:chararray, Time:chararray
);

-- Limit to first 30,000 entries
accident_limit = LIMIT accident 30000;

-- Group by severity
severity_count = GROUP accident_limit BY Accident_Severity;

-- Count accidents by severity
count_severity = FOREACH severity_count GENERATE 
    group AS Severity, COUNT(accident_limit) AS Count;

DUMP count_severity;
```
### Sample Pig Script for Severity_Based_on_LightCondition

```pig
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

STORE L1_count INTO output/Severity Level 1 Based On Light Condition' using PigStorage(',');
STORE L2_count INTO 'output/Severity Level 2 Based On Light Condition' using PieStorage(',');
STORE L3_count INTO 'output/Severity Level 3 Based On Light Condition' using PigStorage(','); 
STORE L4_count INTO 'output/Severity Level 4 Based On Light Condition' using PigStorage(',');
STORE L5_count INTO 'output/Severity Level 5 Based On Light Condition' using Piestorage(',');
```
### Visualizations

- **Accident Severity Distribution**: 

- **Accidents by Day of the Week**: 

## Conclusion

This project demonstrates the use of Hadoop and Pig for big data analysis and the application of Python for data visualization. The insights gained can help identify the factors contributing to road accidents and inform strategies to reduce their severity and frequency.

## Contributing


We welcome and appreciate contributions from the community! If you would like to contribute to this project, please follow these guidelines:

1. **Fork the repository**: 

2. **Clone the repository**: C

```bash
git clone https://github.com/your-username/project-name.git
```

4. **Create a branch**:

```bash
git checkout -b your-branch-name
```

4. **Make your changes**: Make your changes and commit them to your branch.
5. **Push your changes**: Push your changes to your forked repository on GitHub.
```
bash
git push origin your-branch-name
```

6. **Create a pull request**: Go to your forked repository on GitHub and create a new pull request. 

Thank you for your interest in contributing to this project! We look forward to reviewing your changes and working together to improve the project.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact
Email:m.rahimi.hk@gmail.com

























 















