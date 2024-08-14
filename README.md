# Analysis of UK Accident Data using Hadoop and Pig environment
## Description  
• Based on the statistics data around 1.3 million people are being killed yearly in road accidents and around 40 million people are being injured or experienced disability due to an accident.
• Regarding the considerable economic losses that might happen to individuals, their families, and to nations in road accidents, figuring out the major reasoning of road crashes and finding out the solutions to decrease these losses is a vital task to do.

## Table of Contents
- [Introduction](#introduction)
- [Installation](#installation)
- [Dataset Overview](#dataset_overview)
- [Usage](#usage)
- [Code Examples](#code-examples)
- [Conclusion](#conclusion)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)



### Introduction

This project analyzes UK accident data to understand the factors influencing road accidents and their severity. The analysis focuses on:

- Investigating the impact of road conditions, weather, and light conditions on accident severity.
- Exploring the relationship between the number of vehicles involved and the severity of accidents.

The data analysis is performed using the Pig tool, while Python is utilized for data visualization.

Two important approaches:
• First: investigating the influence of different conditions such as road type, road surface condition, weather condition and light condition on the severity of accidents.
• Second: finding out the relation between the severity with number of involved vehicles in the accident and impact of the severities on the number of causalities.
In the current project, all the analysis has been done by Pig tool and Python is just used for the visualization.



## Installation
Architecture
### Software Requirements
- Windows OS
- Virtual Machine to run Hortonworks Sandbox
- Hadoop
- Pig
- Python for data visualization

### Hardware Requirements
- Hard Disk: at least 500 GB
- RAM: at least 12 GB
- Processor: Core i5 or above

## Dataset Overview
- The dataset includes over 1.8 million accidents from 2000 to 2018, with 30,032 rows analyzed for this project.
- Key features include:
  - **Accident Severity**: Classified into levels 1 to 5; most accidents (87%) have moderate severity (Level 3).
  - **Day of Week**: Analyzed to determine the distribution of accidents by day.

- The UK accident data is sourced from [Kaggle](www.Kaggle.com).

## Usage (Hadoop, Pig)
### Loading Data to Hadoop

1. **Run Hortonworks Sandbox** in a Virtual Machine.
2. **Upload Data to HDFS**:
   - Use the Ambari environment to create a directory (`Big_Data_Project`) and upload `UK_CarAccident_Data.csv` to HDFS.

### Data Analysis Using Pig

1. **Access Pig View** in Ambari to write, run, and store Pig scripts for data analysis.
2. **Scripts**: A total of 23 scripts are created for various analyses. Example scripts include:
   - **Severity Analysis**: Analyzing accident severity based on different conditions.
   - **Casualties Analysis**: Counting casualties per accident severity.

3. **Save Results**: Use the STORE command to save the results in HDFS under the `admin/output` directory.


### Code Examples

#### Sample Pig Script for Accident Severity

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

### Visualizations

- **Accident Severity Distribution**: A pie chart showing the distribution of accident severity levels.
  
Accident Severity Pie Chart

- **Accidents by Day of the Week**: A bar chart illustrating the number of accidents occurring on each day.

Accidents by Day of the Week

### Conclusion

This project demonstrates the use of Hadoop and Pig for big data analysis and the application of Python for data visualization. The insights gained can help identify the factors contributing to road accidents and inform strategies to reduce their severity and frequency.

### License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

Feel free to modify any sections or add additional code snippets and visualizations as necessary to fit your project's needs.

Citations:
[1] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/24830350/f24c2348-f433-4fae-97d6-cf10e90c4f8d/Big-Data-Analysis-of-UK-Accident-by-Hadoop-in-Pig-environment.pdf




Stage 1: Data Source : www.Kaggle.com Stage 2: Load data to Hadoop
1. Run Hortonworks Sandbox in VM.






2. Upload data to HDFS.
• Ambari environment Files View: make a directory to upload the CSV files to HDFS.
• Directory: Big_Data_Project.
• UK_CArAccident_Data.csv is uploaded to HDFS.
  Figure 2
Figure 3
3.Using Pig to analysis the data
First in the Views Menu Pig View Pig Interface and using Pig Script to write, run and store the codes and analysis the data.
23 script are made for doing different tasks in this project
Figure 4


4. Save the result in HDFS
• By STORE command, save the result in HDFS under the “admin/output” directory.
• 31 saved files in the output directory

Stage 3: Comprehending datasets
• The UK traffic data from 2000 and 2018
• Over 1.8 million accidents
• 30,000 rows are being considered and analyzed for the current project.
• This dataset consists of 32 various features.




Description of used features:
1. Accident_Severity (chararray)
  10
 1. Accident_Severity (chararray)
• The feature is classified into 5 levels of severity from 1 to 5.
• In our dataset, no accident happened with severity level more than 3.
• The pie chart illustrates that around 87 % of accidents happened in moderate severity (Level 3).
 Figure 7
11

 2. Day_of_Week (chararray)
  12

 2. Day_of_Week (chararray)
• Thefeatureexplainstheaccidentshave happened in which days of the week.
• Number 1,2,3,4,5,6,7 are representing Monday, Tuesday, Wednesday, Thursday, Friday, Saturday and Sunday respectively.
• Thebarchartrevealsthefactthat minimum and maximum of accidents have happened on Monday and Saturday respectively.
• In other days, the distribution is almost the same.
 Figure 8
13

 3. Road_Type (Chararray)
  14

 3. Road_Type (Chararray)
 • This feature is categorized in various road types including Slip Road, Roundabout, One way street, Dual carriageway, Single carriageway.
Figure 9
15

 4. Light_Conditions (chararray)
  16

 4. Light_Conditions (chararray)
 • Thefeatureisdescribedbyfour categories: Daylight, Darkness with no street lighting, Darkness with street lighting and lit, Darkness with street lighting but unlit.
• The pie chart reveals the fact that two third of accidents were happened during the day and one third during the night.
Figure 10
17

 5. Weather_Conditions (chararray)
  18

 5. Weather_Conditions (chararray)
• This feature is described by: Fog or mist, Fine with high wind, Fine without high winds, Rain with high winds, Rain without high winds, Snow with high winds and Snow without high winds.
• Thebarchartrevealsthefactthat more than 80 % of accidents happened at a normal weather condition.
 Figure 11
19

 6. Road_Surface_Conditions (chararray)
  20

 • •
This feature is classified as Dry, Snow, Wet/Damp, Frost/Ice, Flood (Over 3 cm of water).
The bar chart reveals the fact that 80 % of accidents happened at Dry road surface.
6. Road_Surface_Conditions (chararray)
 Figure 12
21

 7. Urban_or_Rural_Area (chararray)
  22

 7. Urban_or_Rural_Area (chararray)
 • This feature is defining the area of the accident. Urban area is represented with number ‘1’ and Rural area by number ‘2’.
• Thepiechartshowsthatthevast majority of accidents had occurred at urban area.
Figure 13
23

 8. Number_of_Vehicles (int)
  24

 8. Number_of_Vehicles (int)
• This feature is describing the number of vehicles which are involved in an accident.
• Number of damaged vehicles are varied from 1 to 8.
• In around 60% of accidents, two cars were involved and interestingly in 25% of occasions, the accident happened just by one car.
 Figure 14
25

 9. Number_of_Causalities (int)
  26

 9. Number_of_Causalities (int)
 • This feature is describing the number of casualties in an accident.
• The number of casualties is varied from 1 to 23 in different accidents.
• More than 80% of accidents have one person who is injured or killed.
Figure 15
27

  Stage 4: Pre-processing
• In the Road_Type feature, there are 56 accidents happened in the type of the road which is named as ‘unknown’. These data had been added to Single carriageway road type, as 23778 accidents happened in this road type and adding 65 to this number, will not affect the result of our analysis.
• In the Weather_Condition feature, there are two categories defined as “unknown” and “other” with the number of 128 and 272 respectively. They had been added to weather conditions as ‘Fine without high winds” as 25873 accidents in this weather condition would not make change in our analysis.
• In the Road_Surface_Condition feature, one of the conditions is named as “Normal” with 14 accidents. I have added them to the Dry condition with 23802 accidents due to the same reason ad mentioned above.
28

 Stage 4: Query and problem Statements
1. Are the number of accidents, damaged vehicles, and casualties more in weekends than weekdays?
2. In which days of the week, the number of non-serious accidents is less than serious accidents?
3. What is the impact of light conditions on severity of accidents? Are there more accidents
happening during nights than days?
4. What is the impact of weather conditions on severity of accidents?
5. Based on Figure 20 most of the accidents had happened Fine weather. Does it mean drivers in
rainy or snowy conditions are more cautious?
6. What is the impact of road surface conditions on severity of accidents?
7. Does the wet road surface cause more accidents than dry surface?
8. What is the relation between the road type and the severity of accidents?
9. What is the relation between the severity and the number of cars involved in an accident?
10. How different level of severity affect the number of injured people?
29

 Stages 5 and 6: Using Pig to analysis the data, answering the problems and visualize the result
• To answer the questions and accomplish this section we act as below:
• Loading the data in Pig environment by LOAD command.
• Removing the possible replication by DISTINCT command.
• Decreasing the number of rows to 30,000 by LIMIT command.
• Continuing the script in a proper way based on each question or problem type and store the result in HDFS
by STORE command.
• Doing the visualization by downloading and using files from HDFS in Python
30

 Q1. Are the number of accidents, damaged vehicles and casualties more in weekends than weekdays? Q2. In which days of the week, the number of non-serious accidents is less than serious accidents?
        31

 Q1. Are the number of accidents, damaged vehicles and casualties more in weekends than weekdays? Q2. In which days of the week, the number of non-serious accidents is less than serious accidents?
 • The number of damaged cars and casualties in Saturday and Monday are highest and lowest respectively, but their number is almost the same in weekdays and Sundays.
• Regarding to the severity level in each days, only at Mondays, number of accident with severity level 3 is less than level 1 and 2. So Monday could be considered as a calm day due to more non-severe accidents compare to other days.
Figure 16. The figure shows the accidents with severity level 1, level 2 and level 3 in different days of the week. Also, the number of vehicles involved in accidents and number of casualties are presented.
32

 Q3. What is the impact of light conditions on severity of accidents? Are there more accidents happening during nights than days?
   33

 Q3. What is the impact of light conditions on severity of accidents? Are there more accidents happening during nights than days?
 • Even though, the number of accidents in daylight is strongly higher than the night, the light condition does not affect the level of severity.
• Even the severity level 3 in darkness without any lighting, is less than other situations.
Figure 17. The bar chart represents the number of accidents with different severity levels in different light conditions.
 Figure 10
34

 Q4. What is the impact of weather conditions on severity of accidents?
Q5. Based on Figure 20 most of the accidents had happened Fine weather. Does it mean drivers in rainy or snowy conditions are more cautious?
   35

 Q4. What is the impact of weather conditions on severity of accidents?
Q5. Based on Figure 20 most of the accidents had happened Fine weather. Does it mean drivers in rainy or snowy conditions are more cautious?
 • Although almost 90 per cent of accidents had happened in fine weather, it doesn't mean that drivers in rainy days are more careful.
• The Figure reveals the most sever accident happened in fog or mist, which means the drivers were not cautious enough in that condition.
• Furthermore, there are minor differences between the number of severity level 3 accidents in various situations..
Figure 18. The bar chart represents the number of accidents with different severity levels in different weather conditions.
 Figure 11
36

 Q6. What is the impact of road surface conditions on severity of accidents? Q7. Does the wet road surface cause more accidents than dry surface?
    37

 Q6. What is the impact of road surface conditions on severity of accidents? Q7. Does the wet road surface cause more accidents than dry surface?
 • The severity level in each road surface condition is almost the same, however in flood situation, severity level 3 is higher than others. It may represent that people do not speed up in not proper road surfaces.
• The number of accidents is higher in dry road than wet road mainly because most of the days of the year are sunny.
Figure 19. The bar chart represents the number of accidents with different severity levels in different road surface conditions
 Figure 12
38

 Q8. What is the relation between the road type and the severity of accidents?
    39

 Q8. What is the relation between the road type and the severity of accidents?
 • More severe accidents happened in roundabouts and slip roads.
• Dual carriageways are caused the less severe accidents compare to single carriageway and one way street.
Figure 20. The bar chart represents the number of accidents with different severity levels in different road types
 Figure 9
40

 Q9. What is the relation between the severity and the number of cars involved in an accident?
  41

 • • •
There is a meaningful relation between the number of cars and severity of accidents.
In severity level 3, 1.8 cars involved per accident, more than level 2 (1.7 involved cars per accidents).
In addition, in level 2 more cars are involved than level 1 (1.6 involved cars per accidents).
Q9. What is the relation between the severity and the number of cars involved in an accident?
 Figure 21. Number of damaged vehicles in one accident with different severity levels.
42

 Q10. How different level of severity affect the number of injured people?
  43

 Q10. How different level of severity affect the number of injured people?
 • Severity has a reverse relation with number of casualties.
• The main reason is that the total number of people in cars has a big impact on the number of casualties per accident.
• Unfortunately, this feature is not provided in our dataset.
Figure 22. Number of casualties in one accident with different severities
44








```bash
# Example installation commands
git clone https://github.com/yourusername/yourproject.git
cd yourproject
# Additional setup commands
```





## Usage
Provide examples of how to use your project. Include command-line examples or code snippets.
bash

## Example usage command
pig -f your_pig_script.pig

## Code Examples
Include relevant code snippets from your project. Use code blocks for clarity.
text
-- Load accident data
accident_data = LOAD '/user/big_data_project/UK_CarAccident_Data.csv' USING PigStorage(',');






### Example with Section Headings



Here is a snippet of a PIG script:

```pig
-- Load accident data
accident_data = LOAD '/user/big_data_project/UK_CarAccident_Data.csv' USING PigStorage(',');
```

*Line number 2: This line does not need to be a part of the code*


## Contributing
Provide guidelines for contributing to your project. Mention how others can contribute, report issues, or suggest features.
## License
Include information about the license under which your project is distributed. For example:
text
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact
Provide your contact information for questions or feedback. You can include your email or links to your social media profiles.
text
Email: your.email@example.com
GitHub: [yourusername](https://github.com/yourusername)




