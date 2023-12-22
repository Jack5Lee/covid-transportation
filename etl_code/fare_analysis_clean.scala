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

fareDf.show()
