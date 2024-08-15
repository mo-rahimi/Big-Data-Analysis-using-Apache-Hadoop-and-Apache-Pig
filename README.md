# Big Data Analysis using Apache Hadoop and Apache Pig
- `Analysis of UK Accident Data`🚥 🚔 🚑 🚦🚧 🚨 🚥 🛑

[View the interactive map](https://github.com/mo-rahimi/Hadoop-Pig-BigData-2022/heatmap_with_filter.html)
    


## Description  
My interest in analyzing accident data stems from `my initial role as a data analyst in the traffic department in 2011`. During that time, I observed a consistent number of accidents and injuries, which sparked my curiosity about the factors contributing to these statistics.

Approximately `1.3 million people die annually in road accidents`, with `40 million sustaining injuries or disabilities`. The economic impact of road accidents is significant, affecting individuals, families, and nations. 
Understanding the primary causes of these incidents is crucial for developing effective prevention strategies.

 This project aims to explore and uncover insights into road safety and accident causation in UK between years 2000 to 2018.
Despite the dataset lacking specific details on accident causes—such as `driver fatigue` or `maneuver errors`, this analysis will address several critical questions:

**Query and problem Statements:**
- How do social and economic factors influence the frequency and severity of road accidents on weekends compared to weekdays?
- How do varying weather patterns interact with driver behavior and infrastructure quality to affect accident severity?
- How do light conditions, weather, and road type collectively influence the severity of accidents across different regions in the UK?
- What insights can we derive from the relationship between road surface conditions and accident severity to inform policy changes aimed at improving road safety?
- How can understanding the correlation between the number of vehicles involved and accident severity help in designing better traffic management systems?
- What hidden patterns can we uncover in the data that might reveal surprising insights into road safety?
- How can we leverage big data analytics to transform our understanding of traffic incidents and enhance public safety measures?


1. How can we leverage the insights gained from analyzing the relationship between accident severity, road type, and the number of vehicles involved to inform infrastructure planning and traffic management strategies that prioritize safety for all road users?
2. By examining the interplay between light conditions, weather patterns, and road surface factors, how can we develop predictive models to identify high-risk scenarios and implement targeted interventions to reduce the likelihood of serious accidents, especially during weekends when the number of casualties tends to be higher?
3. What can we learn from the observation that most accidents occur in fine weather conditions? Can we apply behavioral insights to design awareness campaigns and incentive structures that encourage drivers to maintain a high level of vigilance regardless of environmental factors, ultimately leading to a reduction in non-serious accidents on days when they are less frequent than serious ones?
4. How can we leverage the analysis of accident severity and its impact on the number of injured people to advocate for policy changes that prioritize post-crash care and rehabilitation services, ensuring that victims of road accidents receive timely and comprehensive support to aid their recovery and reintegration into society?
5. By exploring the differences in accident patterns between weekdays and weekends, can we identify opportunities to optimize emergency response systems and resource allocation to better meet the varying demands, ultimately improving the overall efficiency and effectiveness of road safety measures?

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

Two important approaches:
• First: investigating the influence of different conditions such as road type, road surface condition, weather condition and light condition on the severity of accidents.
• Second: finding out the relation between the severity with number of involved vehicles in the accident and impact of the severities on the number of causalities.
In the current project, all the analysis has been done by Pig tool and Python is just used for the visualization.

## Data Set
The UK government amassed traffic data from 2000 and 2018, recording over 1.8 million accidents in the process and making this one of the most comprehensive traffic data sets out there. It's a huge picture of a country undergoing change.
- Download (https://www.kaggle.com/datasets/devansodariya/road-accident-united-kingdom-uk-dataset)
  

## Installation
Architecture
- data set 
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


## Code Examples and Visulization

## Example usage command
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

### Visualizations

- **Accident Severity Distribution**: A pie chart showing the distribution of accident severity levels.
  
Accident Severity Pie Chart

- **Accidents by Day of the Week**: A bar chart illustrating the number of accidents occurring on each day.

Accidents by Day of the Week

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

























 















