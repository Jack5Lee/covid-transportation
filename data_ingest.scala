// Sharing Directories
// hdfs dfs -setfacl -R -m user:as17321:rwx mtaRidership
// hdfs dfs -setfacl -R -m user:lj2330:rwx mtaRidership
// hdfs dfs -setfacl -R -m user:adm209:rwx mtaRidership

val mtaData = sc.textFile("mtaRidership/MTA_Ridership_Dropped_Columns")
val fareData = sc.textFile("mtaRidership/Fare_Card_History.csv")
