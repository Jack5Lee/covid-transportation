// Load data from mtaRidership directory in HDFS
// MTA_Ridership_Dropped_Columns is the output of Clean.scala from homework 7
val mtaData = sc.textFile("mtaRidership/MTA_Ridership_Dropped_Columns")

// Columns of mtaData:
// Date, Subways: Total Estimated Ridership, Subways: % of Comparable Pre-Pandemic Day, Buses: Total Estimated Ridership, Buses: % of Comparable Pre-Pandemic Day
// Parsing the mtaData RDD to extract the relevant columns as numerical values
val extract = mtaData.map { line =>
  val parts = line.stripPrefix("(").stripSuffix(")").split(",")
  (parts(0).toString, parts(1).toInt, parts(2).toDouble, parts(3).toInt, parts(4).toDouble)
}

// Changing to dataframe
val mtaDf = extract.toDF("Date", "Subways: Total Estimated Ridership", "Subways: % of Comparable Pre-Pandemic Day", "Buses: Total Estimated Ridership", "Buses: % of Comparable Pre-Pandemic Day")

// Mean, Median, Mode of Columns 2-5 (Column 1 is not numerical)
val subridershipmean = mtaDf.agg(mean("Subways: Total Estimated Ridership")).first().getDouble(0)

println(s"Mean of Subways: Total Estimated Ridership: $subridershipmean")

val subridershipmedian = mtaDf.stat.approxQuantile("Subways: Total Estimated Ridership", Array(0.5), 0.0)

println(s"Median of Subways: Total Estimated Ridership: ${subridershipmedian(0)}")

val subridershipmode = mtaDf.groupBy("Subways: Total Estimated Ridership").count().orderBy(desc("count")).first().getAs[Int]("Subways: Total Estimated Ridership")

println(s"Mode of Subways: Total Estimated Ridership: $subridershipmode")


val subpercentmean = mtaDf.agg(mean("Subways: % of Comparable Pre-Pandemic Day")).first().getDouble(0)

println(s"Mean of Subways: % of Comparable Pre-Pandemic Day: $subpercentmean")

val subpercentmedian = mtaDf.stat.approxQuantile("Subways: % of Comparable Pre-Pandemic Day", Array(0.5), 0.0)

println(s"Median of Subways: % of Comparable Pre-Pandemic Day: ${subpercentmedian(0)}")

val subpercentmode = mtaDf.groupBy("Subways: % of Comparable Pre-Pandemic Day").count().orderBy(desc("count")).first().getAs[Double]("Subways: % of Comparable Pre-Pandemic Day")

println(s"Mode of Subways: % of Comparable Pre-Pandemic Day: $subpercentmode")


val busridershipmean = mtaDf.agg(mean("Buses: Total Estimated Ridership")).first().getDouble(0)

println(s"Mean of Buses: Total Estimated Ridership: $busridershipmean")

val busridershipmedian = mtaDf.stat.approxQuantile("Buses: Total Estimated Ridership", Array(0.5), 0.0)

println(s"Median of Buses: Total Estimated Ridership: ${busridershipmedian(0)}")

val busridershipmode = mtaDf.groupBy("Buses: Total Estimated Ridership").count().orderBy(desc("count")).first().getAs[Int]("Buses: Total Estimated Ridership")

println(s"Mode of Buses: Total Estimated Ridership: $busridershipmode")


val buspercentmean = mtaDf.agg(mean("Buses: % of Comparable Pre-Pandemic Day")).first().getDouble(0)

println(s"Mean of Buses: % of Comparable Pre-Pandemic Day: $buspercentmean")

val buspercentmedian = mtaDf.stat.approxQuantile("Buses: % of Comparable Pre-Pandemic Day", Array(0.5), 0.0)

println(s"Median of Buses: % of Comparable Pre-Pandemic Day: ${buspercentmedian(0)}")

val buspercentmode = mtaDf.groupBy("Buses: % of Comparable Pre-Pandemic Day").count().orderBy(desc("count")).first().getAs[Double]("Buses: % of Comparable Pre-Pandemic Day")

println(s"Mode of Buses: % of Comparable Pre-Pandemic Day: $buspercentmode")

// Standard Deviation of Column 2 - Subways: Total Estimated Ridership

val std_dev = mtaDf.agg(stddev("Subways: Total Estimated Ridership")).first().getDouble(0)

println(s"Standard Deviation of Subways: Total Estimated Ridership: $std_dev")


// Creating new column based on "Subways: % of Comparable Pre-Pandemic Day" column
// This new column shows if the subway ridership for each date is lower than 90% of a comparable pre-pandemic day
val mtaDf_subwayridershipaffected = mtaDf.withColumn(
  "Subway_Ridership Affected",
  when(col("Subways: % of Comparable Pre-Pandemic Day") < 0.9, 1).otherwise(0)
)

mtaDf_subwayridershipaffected.show()

// I cannot really do any sort of data or text formatting with my dataset so I created a second binary column
// This column shows if the bus ridership for each date is lower than 90% of a comparable pre-pandemic day

val mtaDf_ridershipaffected = mtaDf_subwayridershipaffected.withColumn(
  "Bus_Ridership Affected",
  when(col("Buses: % of Comparable Pre-Pandemic Day") < 0.9, 1).otherwise(0)
)

mtaDf_ridershipaffected.show()