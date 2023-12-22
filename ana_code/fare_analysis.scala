// Import Fare Card History Data
val fareData = sc.textFile("mtaRidership/Fare_Card_History.csv")

val Head = fareData.first()

// Remove Header from data to be able to convert it into a dataframe
val noHeader = fareData.filter(line => !line.equals(Head))

// Drop columns we do not need
val dropped_columns = fareData.map(line => {
	val fields = line.split(",")
    (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5))
})

// Turn data into a dataframe with the following columns
val fareDf = dropped_columns.toDF("From Date", "To Date", "Station ID", "Station", "Full Fare", "Senior Citizen / Disabled")

// Showing total number of full fare and senior citizen/disabled fare rides for each week
val fareByWeek = fareDf.groupBy("From Date").agg(sum("Full Fare").alias("Total Full Fare"), sum("Senior Citizen / Disabled").alias("Total Senior Citizen / Disabled")).withColumn("From Date", to_date(col("From Date"), "MM/dd/yyyy")).orderBy(col("From Date").desc)

fareByWeek.show()

// Sorted by Total Fare in descending order
val byWeekFullFare = fareByWeek.orderBy(col("Total Full Fare").desc)

// Sorted by Total Senior Citizen / Disabled in descending order
val byWeekSeniorFare = fareByWeek.orderBy(col("Total Senior Citizen / Disabled").desc)

byWeekFullFare.show()
byWeekSeniorFare.show()

// Sorted by Total Fare in ascending order
val byWeekFullFareAsc = fareByWeek.orderBy(col("Total Full Fare"))

// Sorted by Total Senior Citizen / Disabled in ascending order
val byWeekSeniorFareAsc = fareByWeek.orderBy(col("Total Senior Citizen / Disabled"))

byWeekFullFareAsc.show()
byWeekSeniorFareAsc.show()

// Showing total number of full fare and senior citizen/disabled fare rides for each year
val fareByYear = fareDf.withColumn("Year", year(to_date(col("From Date"), "MM/dd/yyyy"))).groupBy("Year").agg(sum("Full Fare").alias("Total Full Fare"),sum("Senior Citizen / Disabled").alias("Total Senior Citizen / Disabled"))

val fareByYearOrder = fareByYear.orderBy(col("Year").desc)

fareByYearOrder.show()

