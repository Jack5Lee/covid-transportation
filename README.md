# Analyzing Transportation Usage Throughout Covid
Final_Project_Processing_Big_Data.ipynb: Code for visualizations

/etl_code: For my cleaning code for the MTA Ridership data, I essentially dropped every column except the first 5. This is because only the first 5 columns has data on subway and bus ridership, which are the important metrics used for my analysis. This is important, however, as the output of this code is a new dataset that only includes the columns mentioned above.
For my cleaning code for the Fare Card history data, I similarly only kept the first 6 columns of the dataset, as these were the important metrics for my analysis. There were no null values in both of these datasets, so there was no need for me to delete any entries.
The code can be run using spark-shell --deploy-mode client -i Clean.scala (for MTA Ridership) and spark-shell --deploy-mode client -i fare_analysis_clean.scala (for fare card history).
The results of the run can be found under Final Code Drop/screenshots/etl_code run screenshots.pdf.

/profiling_code: The profiling code for the MTA Ridership dataset was done to look at the general structure of the dataset. I wrote code to look at the number of entries of the dataset. I also looked at the number of instances of each value in column 3 (Subways: % of Comparable Pre-Pandemic Day) and column 5 (Buses: % of Comparable Pre-Pandemic Day) to see the range of the numbers and how much each instances appears. Finally, I also looked at the number of unique values for each column.
The code can be run using spark-shell --deploy-mode client -i CountRecs.scala.
The results of the run can be found under Final Code Drop/screenshots/profiling_code run screenshots.pdf.

/data_ingest: The code is simply used to import each dataset into Spark Scala to use for analysis. There is not much of a need to run this code as I included importing the dataset in every one of my code files. The code also includes a commented line on how I shared my directory with the accounts requested (as17321, lj2330, adm209).

/ana_code: For my analysis code of the MTA Ridership data, I used the output of the Clean.scala file (in the etl_code directory). This code was mainly used to obtain statistical information about the MTA Ridership dataset. It outputs the mean, mode, and median of every column except the first (which is just the data). From this, we can see the general statistics for ridership through the pandemic. I also created two new columns (Subway_Ridership Affected) and (Bus_Ridership Affected) based on column 3 (Subways: % of Comparable Pre-Pandemic Day) and column 5 (Buses: % of Comparable Pre-Pandemic Day). The two columns are binary columns where if the value in columns 3 and 5 are under 0.9, the value of the new columns would be 1, and 0 otherwise. These two columns essentially help us determine if the ridership for that specific day is significantly lower than an equivalent day prior to the pandemic.
For my analysis code of the Fare Card History data, I looked at the number of swipes of full fare and senior citizen/disabled metrocards for each week since 2010. I wanted to look at the number of swipes on a monthly and yearly basis, so I had to do some aggregations to make two different tables. Since each row in the data was a specific week, I had to aggregate the rows so we add up the number of swipes to obtain the number of swipes monthly and yearly, respectively. For the number of monthly swipes table, I reordered the table to show the number of swipes in descending and ascending order for both full fare and senior citizen/disabled metrocards, for a total of 4 tables. I did not find a need to reorder the yearly swipes table, however.
The code files can be run using spark-shell --deploy-mode client -i mta_code_analysis.scala and spark-shell --deploy-mode client -i fare_analysis.scala.
The result of the run can be found under Final Code Drop/screenshots/ana_code run screenshots.pdf.

The input data (as well as all of the code used) is in the mtaRidership directory that should already be shared to the same accounts from previous assignments.
The two datasets used are MTA_Ridership.csv and Fare_Card_History.csv
MTA_Ridership.csv provides provides daily ridership for MTA subways, buses, Long Island Rail Road, Metro-North Railroad, Access-A-Ride, and Bridges and Tunnels, beginning on 3/1/20 until now.
Fare_Card_History.csv provides the number and type of MetroCard (full fare or senior citizen/disabled metrocards) swipes each week from 2010 to late 2021.
